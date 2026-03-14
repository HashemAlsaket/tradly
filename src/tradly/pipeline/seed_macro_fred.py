from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

from tradly.paths import get_repo_root
from tradly.services.db_time import date_to_db_utc, to_db_utc
from tradly.services.time_context import get_time_context


FRED_SERIES = ("DGS2", "DGS10", "DFF", "VIXCLS")
DEFAULT_LOOKBACK_DAYS = 730


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


def _fetch_fred_series(api_key: str, series_id: str, observation_start: str, observation_end: str) -> list[dict]:
    params = urlencode(
        {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": observation_start,
            "observation_end": observation_end,
            "sort_order": "asc",
        }
    )
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise RuntimeError(f"fred_invalid_payload:series={series_id}")
    return observations


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")

    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        print("run: python scripts/setup/init_db.py")
        return 1

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FRED_API_KEY missing")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    lookback_days_raw = os.getenv("MACRO_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))
    try:
        lookback_days = int(lookback_days_raw)
    except ValueError:
        print(f"invalid MACRO_LOOKBACK_DAYS={lookback_days_raw}")
        return 4
    if lookback_days <= 0:
        print(f"invalid MACRO_LOOKBACK_DAYS={lookback_days}")
        return 5

    time_ctx = get_time_context()
    now_db_utc = to_db_utc(time_ctx.now_utc)
    default_observation_end = time_ctx.now_utc.date().isoformat()
    default_observation_start = (time_ctx.now_utc - timedelta(days=lookback_days)).date().isoformat()
    observation_start = os.getenv("TRADLY_MACRO_FROM_DATE", default_observation_start).strip()
    observation_end = os.getenv("TRADLY_MACRO_TO_DATE", default_observation_end).strip()
    if not observation_start or not observation_end:
        print("invalid_macro_window")
        return 6
    if observation_start > observation_end:
        print(f"invalid_macro_window:observation_start={observation_start}:observation_end={observation_end}")
        return 7

    rows_to_upsert: list[tuple] = []
    series_counts: dict[str, int] = {}

    for series_id in FRED_SERIES:
        observations = _fetch_fred_series(
            api_key=api_key,
            series_id=series_id,
            observation_start=observation_start,
            observation_end=observation_end,
        )
        accepted = 0
        for obs in observations:
            date_text = str(obs.get("date", "")).strip()
            value_text = str(obs.get("value", "")).strip()
            if not date_text or value_text in {"", "."}:
                continue
            try:
                value = float(value_text)
            except ValueError:
                continue
            ts_utc = date_to_db_utc(date.fromisoformat(date_text))
            rows_to_upsert.append(
                (
                    series_id,
                    ts_utc,
                    now_db_utc,
                    value,
                    "fred",
                    now_db_utc,
                )
            )
            accepted += 1
        series_counts[series_id] = accepted

    if not rows_to_upsert:
        print("seed_macro_failed=no_rows")
        return 8

    conn = duckdb.connect(str(db_path))
    try:
        conn.begin()
        conn.execute(
            """
            CREATE TEMP TABLE tmp_macro_points (
              series_id TEXT,
              ts_utc TIMESTAMP,
              as_of_utc TIMESTAMP,
              value DOUBLE,
              source TEXT,
              ingested_at_utc TIMESTAMP
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO tmp_macro_points VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows_to_upsert,
        )
        conn.execute(
            """
            INSERT INTO macro_points
            SELECT * FROM tmp_macro_points
            ON CONFLICT(series_id, ts_utc) DO UPDATE SET
              as_of_utc=excluded.as_of_utc,
              value=excluded.value,
              source=excluded.source,
              ingested_at_utc=excluded.ingested_at_utc
            """
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"macro_series={len(FRED_SERIES)}")
    print(f"rows_upserted={len(rows_to_upsert)}")
    print(f"series_counts={series_counts}")
    print(f"observation_start={observation_start}")
    print(f"observation_end={observation_end}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
