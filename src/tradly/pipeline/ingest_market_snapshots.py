from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from tradly.pipeline.ingest_market_bars import (
    BACKFILL_MODE_CUTOVER,
    BACKFILL_MODE_VALIDATE,
    PROVIDER_SOURCE,
    _get_backfill_mode,
    _get_market_data_api_key,
    _load_dotenv,
    _load_market_data_symbols,
    _load_scoped_instrument_symbols,
)
from tradly.paths import get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.time_context import get_time_context


SNAPSHOT_DATA_STATUS = "REALTIME"
VALID_PAYLOAD_STATUS = {"OK"}


def _build_snapshot_url(symbol: str, api_key: str) -> str:
    return f"https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}?apiKey={api_key}"


def _fetch_snapshot(symbol: str, api_key: str) -> dict:
    url = _build_snapshot_url(symbol, api_key)
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    status = payload.get("status")
    if status not in VALID_PAYLOAD_STATUS:
        raise RuntimeError(f"{symbol}: unexpected status={status}")
    ticker_payload = payload.get("ticker")
    if not isinstance(ticker_payload, dict):
        raise RuntimeError(f"{symbol}: snapshot_ticker_missing")
    return ticker_payload


def _ns_to_db_utc(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return to_db_utc(datetime.fromtimestamp(float(value) / 1_000_000_000, tz=timezone.utc))


def _ms_to_db_utc(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return to_db_utc(datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc))


def _normalize_snapshot_row(*, symbol: str, payload: dict, ingested_at: datetime) -> tuple:
    updated_ns = payload.get("updated")
    as_of_utc = _ns_to_db_utc(updated_ns)
    if as_of_utc is None:
        raise RuntimeError(f"{symbol}:missing_snapshot_timestamp")

    day = payload.get("day") or {}
    last_quote = payload.get("lastQuote") or {}
    last_trade = payload.get("lastTrade") or {}
    prev_day = payload.get("prevDay") or {}

    bid_price = last_quote.get("p")
    ask_price = last_quote.get("P")
    if bid_price is not None and ask_price is not None and float(bid_price) > float(ask_price):
        raise RuntimeError(f"{symbol}:invalid_bid_ask")

    return (
        symbol,
        as_of_utc,
        float(last_trade.get("p")) if last_trade.get("p") is not None else None,
        float(last_trade.get("s")) if last_trade.get("s") is not None else None,
        _ns_to_db_utc(last_trade.get("t")),
        float(bid_price) if bid_price is not None else None,
        float(last_quote.get("s")) if last_quote.get("s") is not None else None,
        float(ask_price) if ask_price is not None else None,
        float(last_quote.get("S")) if last_quote.get("S") is not None else None,
        _ns_to_db_utc(last_quote.get("t")),
        float(day.get("o")) if day.get("o") is not None else None,
        float(day.get("h")) if day.get("h") is not None else None,
        float(day.get("l")) if day.get("l") is not None else None,
        float(day.get("c")) if day.get("c") is not None else None,
        float(day.get("v")) if day.get("v") is not None else None,
        float(prev_day.get("c")) if prev_day.get("c") is not None else None,
        float(payload.get("todaysChange")) if payload.get("todaysChange") is not None else None,
        float(payload.get("todaysChangePerc")) if payload.get("todaysChangePerc") is not None else None,
        float(day.get("vw")) if day.get("vw") is not None else None,
        "snapshot",
        SNAPSHOT_DATA_STATUS,
        PROVIDER_SOURCE,
        ingested_at,
        ingested_at,
    )


def _artifact_output_path(repo_root: Path, *, run_date: str, mode: str) -> Path:
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"market_snapshots_backfill_{mode}.json"


def _write_artifact(
    *,
    repo_root: Path,
    run_date: str,
    mode: str,
    scoped_symbols: list[str],
    summary: list[tuple[str, str]],
    rows: list[tuple],
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
        "artifact_type": "market_snapshots",
        "mode": mode,
        "scope_size": len(scoped_symbols),
        "prepared_row_count": len(rows),
        "error_count": len(errors),
        "errors": errors,
        "symbol_summaries": [{"symbol": symbol, "data_status": status} for symbol, status in summary],
        "sample_rows": [
            {
                "symbol": row[0],
                "as_of_utc": row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1]),
                "last_trade_price": row[2],
                "bid_price": row[5],
                "ask_price": row[7],
                "session_close": row[13],
                "prev_close": row[15],
                "change_pct": row[17],
                "market_status": row[19],
                "data_status": row[20],
                "source": row[21],
            }
            for row in rows[:10]
        ],
        "write_applied": mode == BACKFILL_MODE_CUTOVER,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def _upsert_market_snapshots(db_path: Path, rows_to_upsert: list[tuple]) -> None:
    import duckdb

    conn = duckdb.connect(str(db_path))
    try:
        conn.begin()
        conn.execute(
            """
            CREATE TEMP TABLE tmp_market_snapshots (
              symbol TEXT,
              as_of_utc TIMESTAMP,
              last_trade_price DOUBLE,
              last_trade_size DOUBLE,
              last_trade_ts_utc TIMESTAMP,
              bid_price DOUBLE,
              bid_size DOUBLE,
              ask_price DOUBLE,
              ask_size DOUBLE,
              last_quote_ts_utc TIMESTAMP,
              session_open DOUBLE,
              session_high DOUBLE,
              session_low DOUBLE,
              session_close DOUBLE,
              session_volume DOUBLE,
              prev_close DOUBLE,
              change DOUBLE,
              change_pct DOUBLE,
              day_vwap DOUBLE,
              market_status TEXT,
              data_status TEXT,
              source TEXT,
              ingested_at_utc TIMESTAMP,
              updated_at_utc TIMESTAMP
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO tmp_market_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_upsert,
        )
        conn.execute(
            """
            INSERT INTO market_snapshots (
              symbol,
              as_of_utc,
              last_trade_price,
              last_trade_size,
              last_trade_ts_utc,
              bid_price,
              bid_size,
              ask_price,
              ask_size,
              last_quote_ts_utc,
              session_open,
              session_high,
              session_low,
              session_close,
              session_volume,
              prev_close,
              change,
              change_pct,
              day_vwap,
              market_status,
              data_status,
              source,
              ingested_at_utc,
              updated_at_utc
            )
            SELECT
              symbol,
              as_of_utc,
              last_trade_price,
              last_trade_size,
              last_trade_ts_utc,
              bid_price,
              bid_size,
              ask_price,
              ask_size,
              last_quote_ts_utc,
              session_open,
              session_high,
              session_low,
              session_close,
              session_volume,
              prev_close,
              change,
              change_pct,
              day_vwap,
              market_status,
              data_status,
              source,
              ingested_at_utc,
              updated_at_utc
            FROM tmp_market_snapshots
            ON CONFLICT(symbol, as_of_utc) DO UPDATE SET
              last_trade_price=excluded.last_trade_price,
              last_trade_size=excluded.last_trade_size,
              last_trade_ts_utc=excluded.last_trade_ts_utc,
              bid_price=excluded.bid_price,
              bid_size=excluded.bid_size,
              ask_price=excluded.ask_price,
              ask_size=excluded.ask_size,
              last_quote_ts_utc=excluded.last_quote_ts_utc,
              session_open=excluded.session_open,
              session_high=excluded.session_high,
              session_low=excluded.session_low,
              session_close=excluded.session_close,
              session_volume=excluded.session_volume,
              prev_close=excluded.prev_close,
              change=excluded.change,
              change_pct=excluded.change_pct,
              day_vwap=excluded.day_vwap,
              market_status=excluded.market_status,
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

    try:
        backfill_mode = _get_backfill_mode()
    except RuntimeError as exc:
        print("ingest_market_snapshots_v0_failed")
        print(f"error={exc}")
        return 11

    time_ctx = get_time_context()
    ingested_at = to_db_utc(time_ctx.now_utc)
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")

    try:
        scoped_symbols = _load_market_data_symbols(repo_root)
    except RuntimeError as exc:
        print("ingest_market_snapshots_v0_failed")
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
        print("ingest_market_snapshots_v0_failed")
        for symbol in missing_context:
            print(f"error=market_data_scope_symbol_missing:{symbol}")
        return 5

    rows_to_upsert: list[tuple] = []
    errors: list[str] = []
    summary: list[tuple[str, str]] = []

    for symbol in symbols:
        try:
            payload = _fetch_snapshot(symbol, api_key)
            rows_to_upsert.append(_normalize_snapshot_row(symbol=symbol, payload=payload, ingested_at=ingested_at))
            summary.append((symbol, SNAPSHOT_DATA_STATUS))
        except HTTPError as exc:
            errors.append(f"{symbol}:http_error:{exc.code}")
        except URLError as exc:
            errors.append(f"{symbol}:url_error:{exc.reason}")
        except Exception as exc:  # pragma: no cover
            errors.append(f"{symbol}:unexpected:{exc}")

    if not rows_to_upsert:
        print("no snapshots prepared for upsert")
        for err in errors:
            print(f"error={err}")
        return 6

    artifact_path = _write_artifact(
        repo_root=repo_root,
        run_date=run_date,
        mode=backfill_mode,
        scoped_symbols=scoped_symbols,
        summary=summary,
        rows=rows_to_upsert,
        errors=errors,
        now_utc=time_ctx.now_utc,
        now_local=time_ctx.now_local,
        local_timezone=time_ctx.local_timezone,
    )

    if backfill_mode == BACKFILL_MODE_CUTOVER:
        _upsert_market_snapshots(db_path, rows_to_upsert)
        print("backfill_mode=cutover")
        print(f"rows_upserted={len(rows_to_upsert)}")
    else:
        print("backfill_mode=validate")
        print("write_skipped=true")
        print(f"rows_prepared={len(rows_to_upsert)}")

    print(f"artifact={artifact_path}")
    print(f"symbols_loaded={len(summary)}")
    if errors:
        print(f"errors={len(errors)}")
        for err in errors:
            print(f"error={err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
