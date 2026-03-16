from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tradly.paths import get_repo_root
from tradly.services.market_calendar import build_trading_calendar_row


CALENDAR_COLUMNS = [
    ("calendar_date", "DATE"),
    ("day_of_week", "INTEGER"),
    ("day_name", "TEXT"),
    ("is_weekend", "BOOLEAN"),
    ("is_market_holiday", "BOOLEAN"),
    ("is_trading_day", "BOOLEAN"),
    ("market_calendar_state", "TEXT"),
    ("last_cash_session_date", "DATE"),
]


def _ensure_table_columns(conn, table_name: str) -> None:
    for column_name, column_type in CALENDAR_COLUMNS:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _collect_date_bounds(conn) -> tuple[datetime | None, datetime | None]:
    timestamp_queries = [
        "SELECT MIN(ts_utc), MAX(ts_utc) FROM market_bars",
        "SELECT MIN(as_of_utc), MAX(as_of_utc) FROM market_snapshots",
        "SELECT MIN(published_at_utc), MAX(published_at_utc) FROM news_events",
        "SELECT MIN(interpreted_at_utc), MAX(interpreted_at_utc) FROM news_interpretations",
        "SELECT MIN(ts_utc), MAX(ts_utc) FROM macro_points",
    ]
    mins: list[datetime] = []
    maxs: list[datetime] = []
    for query in timestamp_queries:
        min_ts, max_ts = conn.execute(query).fetchone()
        if min_ts is not None:
            mins.append(_as_utc_datetime(min_ts))
        if max_ts is not None:
            maxs.append(_as_utc_datetime(max_ts))

    date_query_pairs = [
        "SELECT MIN(request_date_utc), MAX(request_date_utc) FROM news_pull_usage",
    ]
    for query in date_query_pairs:
        min_day, max_day = conn.execute(query).fetchone()
        if min_day is not None:
            mins.append(datetime.combine(min_day, datetime.min.time(), tzinfo=timezone.utc))
        if max_day is not None:
            maxs.append(datetime.combine(max_day, datetime.min.time(), tzinfo=timezone.utc))

    min_dt = min(mins) if mins else None
    max_dt = max(maxs) if maxs else None
    return min_dt, max_dt


def _upsert_calendar_dimension(conn, *, now_utc: datetime) -> int:
    min_dt, max_dt = _collect_date_bounds(conn)
    start_day = (min_dt.date() - timedelta(days=7)) if min_dt is not None else (now_utc.date() - timedelta(days=370))
    end_day = (max_dt.date() + timedelta(days=14)) if max_dt is not None else (now_utc.date() + timedelta(days=30))
    rows = []
    current = start_day
    while current <= end_day:
        calendar_row = build_trading_calendar_row(current)
        rows.append(
            (
                calendar_row.calendar_date,
                calendar_row.day_of_week,
                calendar_row.day_name,
                calendar_row.is_weekend,
                calendar_row.is_market_holiday,
                calendar_row.is_trading_day,
                calendar_row.market_calendar_state,
                calendar_row.last_cash_session_date,
                calendar_row.next_cash_session_date,
                now_utc,
                now_utc,
            )
        )
        current += timedelta(days=1)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_calendar (
          calendar_date DATE PRIMARY KEY,
          day_of_week INTEGER NOT NULL,
          day_name TEXT NOT NULL,
          is_weekend BOOLEAN NOT NULL,
          is_market_holiday BOOLEAN NOT NULL,
          is_trading_day BOOLEAN NOT NULL,
          market_calendar_state TEXT NOT NULL,
          last_cash_session_date DATE NOT NULL,
          next_cash_session_date DATE NOT NULL,
          as_of_utc TIMESTAMP NOT NULL,
          ingested_at_utc TIMESTAMP NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_calendar (
          calendar_date,
          day_of_week,
          day_name,
          is_weekend,
          is_market_holiday,
          is_trading_day,
          market_calendar_state,
          last_cash_session_date,
          next_cash_session_date,
          as_of_utc,
          ingested_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _stamp_market_bars(conn) -> None:
    _ensure_table_columns(conn, "market_bars")
    conn.execute(
        """
        UPDATE market_bars AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = DATE(((t.ts_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'))
        """
    )


def _stamp_market_snapshots(conn) -> None:
    _ensure_table_columns(conn, "market_snapshots")
    conn.execute(
        """
        UPDATE market_snapshots AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = DATE(((t.as_of_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'))
        """
    )


def _stamp_news_events(conn) -> None:
    _ensure_table_columns(conn, "news_events")
    conn.execute(
        """
        UPDATE news_events AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = DATE(((t.published_at_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'))
        """
    )


def _stamp_news_interpretations(conn) -> None:
    _ensure_table_columns(conn, "news_interpretations")
    conn.execute(
        """
        UPDATE news_interpretations AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = DATE(((t.interpreted_at_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'))
        """
    )


def _stamp_news_pull_usage(conn) -> None:
    _ensure_table_columns(conn, "news_pull_usage")
    conn.execute(
        """
        UPDATE news_pull_usage AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = t.request_date_utc
        """
    )


def _stamp_macro_points(conn) -> None:
    _ensure_table_columns(conn, "macro_points")
    conn.execute(
        """
        UPDATE macro_points AS t
        SET
          calendar_date = mc.calendar_date,
          day_of_week = mc.day_of_week,
          day_name = mc.day_name,
          is_weekend = mc.is_weekend,
          is_market_holiday = mc.is_market_holiday,
          is_trading_day = mc.is_trading_day,
          market_calendar_state = mc.market_calendar_state,
          last_cash_session_date = mc.last_cash_session_date
        FROM market_calendar AS mc
        WHERE mc.calendar_date = DATE(((t.ts_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'))
        """
    )


def refresh_market_calendar_context(*, repo_root=None) -> dict:
    resolved_repo_root = repo_root or get_repo_root()
    db_path = resolved_repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        return {"status": "FAIL", "reason": "db_missing", "path": str(db_path)}

    try:
        import duckdb
    except ImportError:
        return {"status": "FAIL", "reason": "duckdb_missing"}

    now_utc = datetime.now(timezone.utc)
    conn = duckdb.connect(str(db_path))
    try:
        calendar_rows = _upsert_calendar_dimension(conn, now_utc=now_utc)
        _stamp_market_bars(conn)
        _stamp_market_snapshots(conn)
        _stamp_news_events(conn)
        _stamp_news_interpretations(conn)
        _stamp_news_pull_usage(conn)
        _stamp_macro_points(conn)
    finally:
        conn.close()

    return {
        "status": "PASS",
        "calendar_rows_upserted": calendar_rows,
        "as_of_utc": now_utc.isoformat(),
        "tables_stamped": [
            "market_bars",
            "market_snapshots",
            "news_events",
            "news_interpretations",
            "news_pull_usage",
            "macro_points",
        ],
    }


def main() -> int:
    import json

    payload = refresh_market_calendar_context()
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
