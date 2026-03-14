from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from tradly.paths import get_repo_root
from tradly.services.db_time import to_db_utc
from tradly.services.time_context import get_time_context


LOOKBACK_DAYS = 180
MIN_BARS_PER_SYMBOL = 61
VALID_PAYLOAD_STATUS = {"OK", "DELAYED"}
CONTEXT_SYMBOLS = ("SPY", "QQQ", "VIXY", "TLT", "IEF", "SHY")


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


def _fetch_daily_bars(symbol: str, api_key: str, start_date: str, end_date: str) -> tuple[str, list[dict]]:
    base = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": "50000",
        "apiKey": api_key,
    }
    url = f"{base}?{urlencode(params)}"
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    status = payload.get("status")
    if status not in VALID_PAYLOAD_STATUS:
        raise RuntimeError(f"{symbol}: unexpected status={status}")

    rows = payload.get("results")
    if not isinstance(rows, list):
        raise RuntimeError(f"{symbol}: results missing")
    return status, rows


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")

    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        print("run: python scripts/setup/init_db.py")
        return 1

    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        print("POLYGON_API_KEY missing")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    time_ctx = get_time_context()
    default_start_date = (time_ctx.now_utc - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    default_end_date = time_ctx.now_utc.date().isoformat()
    start_date = os.getenv("TRADLY_MARKET_FROM_DATE", default_start_date).strip()
    end_date = os.getenv("TRADLY_MARKET_TO_DATE", default_end_date).strip()
    if not start_date or not end_date:
        print("invalid_market_window")
        return 8
    if start_date > end_date:
        print(f"invalid_market_window:start_date={start_date}:end_date={end_date}")
        return 9
    ingested_at = to_db_utc(time_ctx.now_utc)

    conn = duckdb.connect(str(db_path))
    try:
        symbol_rows = conn.execute(
            """
            SELECT symbol, active
            FROM instruments
            WHERE active = TRUE OR symbol IN (?, ?, ?, ?, ?, ?)
            ORDER BY symbol
            """,
            CONTEXT_SYMBOLS,
        ).fetchall()
    finally:
        conn.close()

    if not symbol_rows:
        print("no active instruments found. run: python scripts/setup/load_universe.py")
        return 4

    symbols = [row[0] for row in symbol_rows]
    missing_context = sorted(symbol for symbol in CONTEXT_SYMBOLS if symbol not in set(symbols))
    if missing_context:
        print("ingest_market_bars_v0_failed")
        for symbol in missing_context:
            print(f"error=context_symbol_missing:{symbol}")
        return 5

    rows_to_upsert: list[tuple] = []
    errors: list[str] = []
    summary: list[tuple[str, int, str]] = []

    for symbol in symbols:
        try:
            status, bars = _fetch_daily_bars(symbol, api_key, start_date, end_date)
        except HTTPError as exc:
            errors.append(f"{symbol}:http_error:{exc.code}")
            continue
        except URLError as exc:
            errors.append(f"{symbol}:url_error:{exc.reason}")
            continue
        except Exception as exc:  # pragma: no cover
            errors.append(f"{symbol}:unexpected:{exc}")
            continue

        if len(bars) < MIN_BARS_PER_SYMBOL:
            errors.append(f"{symbol}:insufficient_bars:{len(bars)}")
            continue

        data_status = "DELAYED" if status == "DELAYED" else "REALTIME"
        valid_symbol_rows = 0
        for bar in bars:
            ts_ms = bar.get("t")
            close = bar.get("c")
            volume = bar.get("v")
            if ts_ms is None or close is None or volume is None:
                errors.append(f"{symbol}:missing_required_bar_field")
                valid_symbol_rows = 0
                break
            if close <= 0 or volume <= 0:
                errors.append(f"{symbol}:non_positive_close_or_volume")
                valid_symbol_rows = 0
                break

            ts_utc = to_db_utc(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))
            rows_to_upsert.append(
                (
                    symbol,
                    "1d",
                    ts_utc,
                    ts_utc,
                    float(bar.get("o")) if bar.get("o") is not None else None,
                    float(bar.get("h")) if bar.get("h") is not None else None,
                    float(bar.get("l")) if bar.get("l") is not None else None,
                    float(close),
                    float(volume),
                    float(bar.get("vw")) if bar.get("vw") is not None else None,
                    data_status,
                    "polygon",
                    0,
                    ingested_at,
                    ingested_at,
                )
            )
            valid_symbol_rows += 1
        summary.append((symbol, valid_symbol_rows, data_status))

    if errors:
        print("ingest_market_bars_v0_failed")
        for err in errors:
            print(f"error={err}")
        return 6

    if not rows_to_upsert:
        print("no bars prepared for upsert")
        return 7

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
            INSERT INTO market_bars
            SELECT * FROM tmp_market_bars
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

    print(f"symbols_loaded={len(summary)}")
    print(f"rows_upserted={len(rows_to_upsert)}")
    for symbol, count, status in summary:
        print(f"symbol={symbol} rows={count} status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
