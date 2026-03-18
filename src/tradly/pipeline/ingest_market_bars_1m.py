from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from tradly.pipeline.ingest_market_bars import (
    BACKFILL_DATA_STATUS,
    BACKFILL_MODE_CUTOVER,
    BACKFILL_MODE_VALIDATE,
    PROVIDER_SOURCE,
    _get_backfill_mode,
    _get_market_data_api_key,
    _load_dotenv,
    _load_market_data_symbols,
    _load_scoped_instrument_symbols,
    _upsert_market_bars,
    _write_validation_artifact,
)
from tradly.paths import get_repo_root
from tradly.services.db_time import from_db_utc, to_db_utc
from tradly.services.time_context import get_time_context


LOOKBACK_DAYS_1M = 30
VALID_PAYLOAD_STATUS = {"OK", "DELAYED"}
WATERMARK_SOURCE_NAME = "market_bars_1m"


def _ensure_pipeline_watermarks_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watermarks (
          source_name TEXT NOT NULL,
          scope_key TEXT NOT NULL,
          watermark_ts_utc TIMESTAMP,
          watermark_meta_json TEXT,
          updated_at_utc TIMESTAMP NOT NULL,
          PRIMARY KEY (source_name, scope_key)
        )
        """
    )


def _build_minute_agg_url(symbol: str, api_key: str, start_date: str, end_date: str) -> str:
    from urllib.parse import urlencode

    base = f"https://api.massive.com/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": "50000",
        "apiKey": api_key,
    }
    return f"{base}?{urlencode(params)}"


def _fetch_minute_bars(symbol: str, api_key: str, start_date: str, end_date: str) -> tuple[str, list[dict]]:
    url = _build_minute_agg_url(symbol, api_key, start_date, end_date)
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    status = payload.get("status")
    if status not in VALID_PAYLOAD_STATUS:
        raise RuntimeError(f"{symbol}: unexpected status={status}")

    rows = payload.get("results")
    if not isinstance(rows, list):
        raise RuntimeError(f"{symbol}: results missing")
    return str(status), rows


def _normalize_minute_bar_row(*, symbol: str, bar: dict, ingested_at: datetime) -> tuple:
    ts_ms = bar.get("t")
    close = bar.get("c")
    volume = bar.get("v")
    open_ = bar.get("o")
    high = bar.get("h")
    low = bar.get("l")
    vwap = bar.get("vw")

    if ts_ms is None or close is None or volume is None:
        raise RuntimeError(f"{symbol}:missing_required_bar_field")
    if close <= 0 or volume < 0:
        raise RuntimeError(f"{symbol}:non_positive_close_or_negative_volume")
    if open_ is not None and high is not None and open_ > high:
        raise RuntimeError(f"{symbol}:malformed_ohlc")
    if open_ is not None and low is not None and open_ < low:
        raise RuntimeError(f"{symbol}:malformed_ohlc")
    if high is not None and low is not None and high < low:
        raise RuntimeError(f"{symbol}:malformed_ohlc")
    if close is not None and high is not None and close > high:
        raise RuntimeError(f"{symbol}:malformed_ohlc")
    if close is not None and low is not None and close < low:
        raise RuntimeError(f"{symbol}:malformed_ohlc")

    ts_utc = to_db_utc(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
    return (
        symbol,
        "1m",
        ts_utc,
        ts_utc,
        float(open_) if open_ is not None else None,
        float(high) if high is not None else None,
        float(low) if low is not None else None,
        float(close),
        float(volume),
        float(vwap) if vwap is not None else None,
        BACKFILL_DATA_STATUS,
        PROVIDER_SOURCE,
        0,
        ingested_at,
        ingested_at,
    )


def _load_1m_watermarks(conn, symbols: Iterable[str]) -> dict[str, datetime]:
    symbol_list = list(symbols)
    if not symbol_list:
        return {}
    rows = conn.execute(
        """
        SELECT scope_key, watermark_ts_utc
        FROM pipeline_watermarks
        WHERE source_name = ?
          AND scope_key IN ({placeholders})
        """.format(placeholders=",".join("?" for _ in symbol_list)),
        [WATERMARK_SOURCE_NAME, *symbol_list],
    ).fetchall()
    return {str(scope_key): watermark_ts_utc for scope_key, watermark_ts_utc in rows if watermark_ts_utc is not None}


def _request_window_from_watermark(
    *,
    default_start_date: str,
    default_end_date: str,
    watermark_ts_utc: datetime | None,
) -> tuple[str, str]:
    if watermark_ts_utc is None:
        return default_start_date, default_end_date
    watermark_day = from_db_utc(watermark_ts_utc).astimezone(timezone.utc).date()
    start_date = (watermark_day - timedelta(days=1)).isoformat()
    return start_date, default_end_date


def _filter_rows_newer_than_watermark(rows: list[tuple], watermark_ts_utc: datetime | None) -> list[tuple]:
    if watermark_ts_utc is None:
        return rows
    return [row for row in rows if row[2] > watermark_ts_utc]


def _upsert_1m_watermarks(db_path: Path, per_symbol_max_ts: dict[str, datetime], updated_at: datetime) -> None:
    if not per_symbol_max_ts:
        return
    import duckdb

    conn = duckdb.connect(str(db_path))
    try:
        conn.executemany(
            """
            INSERT INTO pipeline_watermarks (
              source_name,
              scope_key,
              watermark_ts_utc,
              watermark_meta_json,
              updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (source_name, scope_key) DO UPDATE SET
              watermark_ts_utc = excluded.watermark_ts_utc,
              watermark_meta_json = excluded.watermark_meta_json,
              updated_at_utc = excluded.updated_at_utc
            """,
            [
                (
                    WATERMARK_SOURCE_NAME,
                    symbol,
                    max_ts,
                    json.dumps({"timeframe": "1m"}, ensure_ascii=True),
                    updated_at,
                )
                for symbol, max_ts in per_symbol_max_ts.items()
            ],
        )
    finally:
        conn.close()


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")

    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        print("run: python scripts/setup/init_db.py")
        return 1

    api_key = _get_market_data_api_key()
    if not api_key:
        print("MASSIVE_API_KEY missing")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    time_ctx = get_time_context()
    default_end_date = time_ctx.now_local.date().isoformat()
    default_start_date = (time_ctx.now_local.date() - timedelta(days=LOOKBACK_DAYS_1M)).isoformat()
    try:
        backfill_mode = _get_backfill_mode()
    except RuntimeError as exc:
        print("ingest_market_bars_1m_v0_failed")
        print(f"error={exc}")
        return 11
    env_start_date = os.getenv("TRADLY_MARKET_1M_FROM_DATE", default_start_date).strip()
    env_end_date = os.getenv("TRADLY_MARKET_1M_TO_DATE", default_end_date).strip()
    if not env_start_date or not env_end_date:
        print("invalid_market_1m_window")
        return 8
    if env_start_date > env_end_date:
        print(f"invalid_market_1m_window:start_date={env_start_date}:end_date={env_end_date}")
        return 9
    ingested_at = to_db_utc(time_ctx.now_utc)

    try:
        scoped_symbols = _load_market_data_symbols(repo_root)
    except RuntimeError as exc:
        print("ingest_market_bars_1m_v0_failed")
        print(f"error={exc}")
        return 10

    conn = duckdb.connect(str(db_path))
    try:
        _ensure_pipeline_watermarks_table(conn)
        symbols, missing_context = _load_scoped_instrument_symbols(conn, scoped_symbols)
        watermarks = _load_1m_watermarks(conn, symbols)
    finally:
        conn.close()

    if not symbols:
        print("no scoped instruments found. run: python scripts/setup/load_universe.py")
        return 4
    if missing_context:
        print("ingest_market_bars_1m_v0_failed")
        for symbol in missing_context:
            print(f"error=market_data_scope_symbol_missing:{symbol}")
        return 5

    rows_to_upsert: list[tuple] = []
    errors: list[str] = []
    summary: list[tuple[str, int, str]] = []
    watermark_summary: list[dict[str, object]] = []
    per_symbol_max_ts: dict[str, datetime] = {}
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")

    for symbol in symbols:
        previous_watermark = watermarks.get(symbol)
        start_date, end_date = _request_window_from_watermark(
            default_start_date=env_start_date,
            default_end_date=env_end_date,
            watermark_ts_utc=previous_watermark,
        )
        try:
            _status, bars = _fetch_minute_bars(symbol, api_key, start_date, end_date)
        except HTTPError as exc:
            errors.append(f"{symbol}:http_error:{exc.code}")
            continue
        except URLError as exc:
            errors.append(f"{symbol}:url_error:{exc.reason}")
            continue
        except Exception as exc:  # pragma: no cover
            errors.append(f"{symbol}:unexpected:{exc}")
            continue

        normalized_symbol_rows: list[tuple] = []
        for bar in bars:
            try:
                normalized_symbol_rows.append(_normalize_minute_bar_row(symbol=symbol, bar=bar, ingested_at=ingested_at))
            except RuntimeError as exc:
                errors.append(str(exc))
                normalized_symbol_rows = []
                break
        new_symbol_rows = _filter_rows_newer_than_watermark(normalized_symbol_rows, previous_watermark)
        rows_to_upsert.extend(new_symbol_rows)
        if new_symbol_rows:
            per_symbol_max_ts[symbol] = max(row[2] for row in new_symbol_rows)
        summary.append((symbol, len(new_symbol_rows), BACKFILL_DATA_STATUS))
        watermark_summary.append(
            {
                "symbol": symbol,
                "previous_watermark_utc": previous_watermark.isoformat() if hasattr(previous_watermark, "isoformat") else None,
                "request_window": {"from_date": start_date, "to_date": end_date},
                "fetched_row_count": len(normalized_symbol_rows),
                "new_row_count": len(new_symbol_rows),
                "advanced_watermark_utc": per_symbol_max_ts[symbol].isoformat() if symbol in per_symbol_max_ts else None,
                "advancement_status": "advanced" if symbol in per_symbol_max_ts else ("already_current" if previous_watermark is not None else "bootstrap_no_new_rows"),
            }
        )

    if errors:
        print("ingest_market_bars_1m_v0_failed")
        for err in errors:
            print(f"error={err}")
        return 6

    artifact_path = _write_validation_artifact(
        repo_root=repo_root,
        run_date=run_date,
        mode=backfill_mode,
        start_date=env_start_date,
        end_date=env_end_date,
        validation_mode="incremental",
        expected_market_dates=[],
        scoped_symbols=scoped_symbols,
        summary=summary,
        rows_to_upsert=rows_to_upsert,
        errors=errors,
        now_utc=time_ctx.now_utc,
        now_local=time_ctx.now_local,
        local_timezone=time_ctx.local_timezone,
    )
    # Rewrite timeframe marker in-place to distinguish artifacts.
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["timeframe"] = "1m"
    payload["artifact_type"] = "market_bars_backfill_1m"
    payload["window"] = {"from_date": start_date, "to_date": end_date}
    payload["watermark_mode"] = True
    payload["watermark_summary"] = watermark_summary
    artifact_path_1m = artifact_path.with_name(artifact_path.name.replace("_1d_", "_1m_"))
    artifact_path_1m.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if artifact_path_1m != artifact_path:
        artifact_path.unlink(missing_ok=True)

    if backfill_mode == BACKFILL_MODE_CUTOVER:
        if rows_to_upsert:
            _upsert_market_bars(db_path, rows_to_upsert)
        _upsert_1m_watermarks(db_path, per_symbol_max_ts, ingested_at)
        print("backfill_mode=cutover")
        print(f"rows_upserted={len(rows_to_upsert)}")
    else:
        print("backfill_mode=validate")
        print("write_skipped=true")
        print(f"rows_prepared={len(rows_to_upsert)}")

    print(f"artifact={artifact_path_1m}")
    print(f"symbols_loaded={len(summary)}")
    for symbol, count, status in summary:
        print(f"symbol={symbol} rows={count} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
