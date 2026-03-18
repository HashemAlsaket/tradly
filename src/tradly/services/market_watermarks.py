from __future__ import annotations

from datetime import datetime


WATERMARK_SOURCE_NAME_1M = "market_bars_1m"


def load_1m_watermark_coverage(conn, scoped_symbols: list[str]) -> tuple[datetime | None, bool, int]:
    if not scoped_symbols:
        return None, True, 0
    table_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'pipeline_watermarks'
        """
    ).fetchone()[0]
    if not table_exists:
        return None, False, 0
    rows = conn.execute(
        """
        SELECT scope_key, watermark_ts_utc
        FROM pipeline_watermarks
        WHERE source_name = ?
        """,
        (WATERMARK_SOURCE_NAME_1M,),
    ).fetchall()
    scoped = {str(scope_key): watermark_ts_utc for scope_key, watermark_ts_utc in rows if str(scope_key) in scoped_symbols}
    coverage_count = len(scoped)
    coverage_complete = coverage_count == len(scoped_symbols)
    floor = min(scoped.values()) if scoped else None
    return floor, coverage_complete, coverage_count


def load_1m_watermark_min_for_scoped_symbols(conn, scoped_symbols: list[str]) -> datetime | None:
    floor, _coverage_complete, _coverage_count = load_1m_watermark_coverage(conn, scoped_symbols)
    return floor
