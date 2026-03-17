from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from tradly.paths import get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.market_calendar import is_trading_day
from tradly.services.time_context import get_time_context


LOOKBACK_DAYS = 180
MIN_BARS_PER_SYMBOL = 61
VALID_PAYLOAD_STATUS = {"OK", "DELAYED"}
SCOPE_MANIFEST_PATH = Path("data/manual/universe_runtime_scopes.json")
PROVIDER_SOURCE = "massive"
BACKFILL_DATA_STATUS = "DELAYED"
BACKFILL_MODE_VALIDATE = "validate"
BACKFILL_MODE_CUTOVER = "cutover"
VALIDATION_MODE_AUTO = "auto"
VALIDATION_MODE_BOOTSTRAP = "bootstrap"
VALIDATION_MODE_INCREMENTAL = "incremental"
INCREMENTAL_WINDOW_MAX_DAYS = 14


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_market_data_api_key() -> str | None:
    return os.getenv("MASSIVE_API_KEY")


def _get_backfill_mode() -> str:
    raw = str(os.getenv("TRADLY_MARKET_BACKFILL_MODE", BACKFILL_MODE_VALIDATE)).strip().lower()
    if raw not in {BACKFILL_MODE_VALIDATE, BACKFILL_MODE_CUTOVER}:
        raise RuntimeError(f"invalid_backfill_mode:{raw}")
    return raw


def _get_validation_mode(*, start_date: str, end_date: str) -> str:
    raw = str(os.getenv("TRADLY_MARKET_VALIDATION_MODE", VALIDATION_MODE_AUTO)).strip().lower()
    if raw not in {VALIDATION_MODE_AUTO, VALIDATION_MODE_BOOTSTRAP, VALIDATION_MODE_INCREMENTAL}:
        raise RuntimeError(f"invalid_validation_mode:{raw}")
    if raw != VALIDATION_MODE_AUTO:
        return raw
    window_days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    if window_days <= INCREMENTAL_WINDOW_MAX_DAYS:
        return VALIDATION_MODE_INCREMENTAL
    return VALIDATION_MODE_BOOTSTRAP


def _expected_market_dates(start_date: str, end_date: str) -> list[str]:
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out: list[str] = []
    while current <= end:
        if is_trading_day(current):
            out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def _build_daily_agg_url(symbol: str, api_key: str, start_date: str, end_date: str) -> str:
    base = f"https://api.massive.com/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": "50000",
        "apiKey": api_key,
    }
    return f"{base}?{urlencode(params)}"


def _fetch_daily_bars(symbol: str, api_key: str, start_date: str, end_date: str) -> tuple[str, list[dict]]:
    url = _build_daily_agg_url(symbol, api_key, start_date, end_date)
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    status = payload.get("status")
    if status not in VALID_PAYLOAD_STATUS:
        raise RuntimeError(f"{symbol}: unexpected status={status}")

    rows = payload.get("results")
    if not isinstance(rows, list):
        raise RuntimeError(f"{symbol}: results missing")
    return str(status), rows


def _normalize_daily_bar_row(*, symbol: str, bar: dict, ingested_at: datetime) -> tuple:
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
        "1d",
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


def _load_market_data_symbols(repo_root: Path) -> list[str]:
    manifest_path = repo_root / SCOPE_MANIFEST_PATH
    if not manifest_path.exists():
        raise RuntimeError(f"market_data_scope_manifest_missing:{manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("market_data_scope_manifest_invalid:root_not_object")
    scopes = payload.get("scopes")
    if not isinstance(scopes, dict):
        raise RuntimeError("market_data_scope_manifest_invalid:scopes_not_object")
    symbols = scopes.get("market_data_symbols")
    if not isinstance(symbols, list):
        raise RuntimeError("market_data_scope_manifest_invalid:market_data_symbols_not_list")
    cleaned = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not cleaned:
        raise RuntimeError("market_data_scope_manifest_invalid:market_data_symbols_empty")
    return cleaned


def _load_scoped_instrument_symbols(conn, scoped_symbols: Iterable[str]) -> tuple[list[str], list[str]]:
    symbols_list = list(scoped_symbols)
    placeholders = ", ".join("?" for _ in symbols_list)
    symbol_rows = conn.execute(
        f"""
        SELECT symbol
        FROM instruments
        WHERE symbol IN ({placeholders})
        ORDER BY symbol
        """,
        symbols_list,
    ).fetchall()
    loaded_symbols = [row[0] for row in symbol_rows]
    missing_symbols = sorted(symbol for symbol in symbols_list if symbol not in set(loaded_symbols))
    return loaded_symbols, missing_symbols


def _upsert_market_bars(db_path: Path, rows_to_upsert: list[tuple]) -> None:
    import duckdb

    conn = duckdb.connect(str(db_path))
    try:
        conn.begin()
        conn.execute(
            """
            CREATE TEMP TABLE tmp_market_bars (
              symbol TEXT,
              timeframe TEXT,
              ts_utc TIMESTAMP,
              as_of_utc TIMESTAMP,
              open DOUBLE,
              high DOUBLE,
              low DOUBLE,
              close DOUBLE,
              volume DOUBLE,
              vwap DOUBLE,
              data_status TEXT,
              source TEXT,
              correction_seq INTEGER,
              ingested_at_utc TIMESTAMP,
              updated_at_utc TIMESTAMP
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO tmp_market_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_upsert,
        )
        conn.execute(
            """
            INSERT INTO market_bars (
              symbol,
              timeframe,
              ts_utc,
              as_of_utc,
              open,
              high,
              low,
              close,
              volume,
              vwap,
              data_status,
              source,
              correction_seq,
              ingested_at_utc,
              updated_at_utc
            )
            SELECT
              symbol,
              timeframe,
              ts_utc,
              as_of_utc,
              open,
              high,
              low,
              close,
              volume,
              vwap,
              data_status,
              source,
              correction_seq,
              ingested_at_utc,
              updated_at_utc
            FROM tmp_market_bars
            ON CONFLICT(symbol, timeframe, ts_utc, correction_seq) DO UPDATE SET
              as_of_utc=excluded.as_of_utc,
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close,
              volume=excluded.volume,
              vwap=excluded.vwap,
              data_status=excluded.data_status,
              source=excluded.source,
              ingested_at_utc=excluded.ingested_at_utc,
              updated_at_utc=excluded.updated_at_utc
            """
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _artifact_output_path(repo_root: Path, *, run_date: str, mode: str) -> Path:
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"market_bars_backfill_1d_{mode}.json"


def _serialize_sample_row(row: tuple) -> dict[str, object]:
    return {
        "symbol": row[0],
        "timeframe": row[1],
        "ts_utc": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
        "as_of_utc": row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3]),
        "open": row[4],
        "high": row[5],
        "low": row[6],
        "close": row[7],
        "volume": row[8],
        "vwap": row[9],
        "data_status": row[10],
        "source": row[11],
        "correction_seq": row[12],
    }


def _write_validation_artifact(
    *,
    repo_root: Path,
    run_date: str,
    mode: str,
    start_date: str,
    end_date: str,
    validation_mode: str,
    expected_market_dates: list[str],
    scoped_symbols: list[str],
    summary: list[tuple[str, int, str]],
    rows_to_upsert: list[tuple],
    errors: list[str],
    now_utc: datetime,
    now_local: datetime,
    local_timezone: str,
) -> Path:
    out_path = _artifact_output_path(repo_root, run_date=run_date, mode=mode)
    payload = {
        "run_timestamp_utc": now_utc.isoformat(),
        "run_timestamp_local": now_local.isoformat(),
        "local_timezone": local_timezone,
        "provider": PROVIDER_SOURCE,
        "timeframe": "1d",
        "mode": mode,
        "scope_size": len(scoped_symbols),
        "window": {
            "from_date": start_date,
            "to_date": end_date,
        },
        "validation_mode": validation_mode,
        "expected_market_dates": expected_market_dates,
        "prepared_row_count": len(rows_to_upsert),
        "error_count": len(errors),
        "errors": errors,
        "symbol_summaries": [
            {"symbol": symbol, "row_count": row_count, "data_status": status}
            for symbol, row_count, status in summary
        ],
        "sample_rows": [_serialize_sample_row(row) for row in rows_to_upsert[:10]],
        "write_applied": mode == BACKFILL_MODE_CUTOVER,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


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
    default_start_date = (time_ctx.now_utc - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    default_end_date = time_ctx.now_utc.date().isoformat()
    try:
        backfill_mode = _get_backfill_mode()
    except RuntimeError as exc:
        print("ingest_market_bars_v0_failed")
        print(f"error={exc}")
        return 11
    start_date = os.getenv("TRADLY_MARKET_FROM_DATE", default_start_date).strip()
    end_date = os.getenv("TRADLY_MARKET_TO_DATE", default_end_date).strip()
    if not start_date or not end_date:
        print("invalid_market_window")
        return 8
    if start_date > end_date:
        print(f"invalid_market_window:start_date={start_date}:end_date={end_date}")
        return 9
    try:
        validation_mode = _get_validation_mode(start_date=start_date, end_date=end_date)
    except RuntimeError as exc:
        print("ingest_market_bars_v0_failed")
        print(f"error={exc}")
        return 12
    expected_market_dates = _expected_market_dates(start_date, end_date)
    ingested_at = to_db_utc(time_ctx.now_utc)

    try:
        scoped_symbols = _load_market_data_symbols(repo_root)
    except RuntimeError as exc:
        print("ingest_market_bars_v0_failed")
        print(f"error={exc}")
        return 10

    conn = duckdb.connect(str(db_path))
    try:
        symbols, missing_context = _load_scoped_instrument_symbols(conn, scoped_symbols)
    finally:
        conn.close()

    if not symbols:
        print("no scoped instruments found. run: python scripts/setup/load_universe.py")
        return 4
    if missing_context:
        print("ingest_market_bars_v0_failed")
        for symbol in missing_context:
            print(f"error=market_data_scope_symbol_missing:{symbol}")
        return 5

    rows_to_upsert: list[tuple] = []
    errors: list[str] = []
    summary: list[tuple[str, int, str]] = []
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")

    for symbol in symbols:
        try:
            _status, bars = _fetch_daily_bars(symbol, api_key, start_date, end_date)
        except HTTPError as exc:
            errors.append(f"{symbol}:http_error:{exc.code}")
            continue
        except URLError as exc:
            errors.append(f"{symbol}:url_error:{exc.reason}")
            continue
        except Exception as exc:  # pragma: no cover
            errors.append(f"{symbol}:unexpected:{exc}")
            continue

        if validation_mode == VALIDATION_MODE_BOOTSTRAP and len(bars) < MIN_BARS_PER_SYMBOL:
            errors.append(f"{symbol}:insufficient_bars:{len(bars)}")
            continue

        valid_symbol_rows = 0
        symbol_rows: list[tuple] = []
        for bar in bars:
            try:
                symbol_rows.append(_normalize_daily_bar_row(symbol=symbol, bar=bar, ingested_at=ingested_at))
            except RuntimeError as exc:
                errors.append(str(exc))
                valid_symbol_rows = 0
                symbol_rows = []
                break
            valid_symbol_rows += 1
        if validation_mode == VALIDATION_MODE_INCREMENTAL and symbol_rows:
            observed_market_dates = sorted({row[2].date().isoformat() for row in symbol_rows})
            missing_market_dates = [day for day in expected_market_dates if day not in observed_market_dates]
            if missing_market_dates:
                errors.append(
                    f"{symbol}:missing_expected_market_dates:{','.join(missing_market_dates)}"
                )
                continue
        elif validation_mode == VALIDATION_MODE_INCREMENTAL and expected_market_dates:
            errors.append(f"{symbol}:provider_returned_no_rows_for_expected_range")
            continue
        rows_to_upsert.extend(symbol_rows)
        summary.append((symbol, valid_symbol_rows, BACKFILL_DATA_STATUS))

    if errors:
        print("ingest_market_bars_v0_failed")
        for err in errors:
            print(f"error={err}")
        return 6

    if not rows_to_upsert:
        print("no bars prepared for upsert")
        return 7

    artifact_path = _write_validation_artifact(
        repo_root=repo_root,
        run_date=run_date,
        mode=backfill_mode,
        start_date=start_date,
        end_date=end_date,
        validation_mode=validation_mode,
        expected_market_dates=expected_market_dates,
        scoped_symbols=scoped_symbols,
        summary=summary,
        rows_to_upsert=rows_to_upsert,
        errors=errors,
        now_utc=time_ctx.now_utc,
        now_local=time_ctx.now_local,
        local_timezone=time_ctx.local_timezone,
    )

    if backfill_mode == BACKFILL_MODE_CUTOVER:
        _upsert_market_bars(db_path, rows_to_upsert)
        print("backfill_mode=cutover")
        print(f"rows_upserted={len(rows_to_upsert)}")
    else:
        print("backfill_mode=validate")
        print("write_skipped=true")
        print(f"rows_prepared={len(rows_to_upsert)}")

    print(f"artifact={artifact_path}")
    print(f"symbols_loaded={len(summary)}")
    for symbol, count, status in summary:
        print(f"symbol={symbol} rows={count} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
