from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone

from tradly.paths import get_repo_root


FOCUS_SEMIS = ("MU", "SNDK", "NVDA", "NVTS")
REGIME_SYMBOLS = ("SPY", "QQQ", "VIXY", "TLT", "IEF", "SHY")

MIN_ACTIVE_INSTRUMENTS = 20
MIN_DAILY_BARS_PER_ACTIVE_SYMBOL = 120
MAX_DAILY_BAR_STALENESS_DAYS = 7
MIN_NEWS_EVENTS_30D = 150
MIN_NEWS_SYMBOL_LINKS_30D = 100
MIN_INTERPRETATION_COVERAGE_30D = 0.60
MIN_MACRO_SERIES = 3
MIN_MACRO_POINTS_90D = 60


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: str


def _days_between(now_utc: datetime, ts_utc: datetime | None) -> int | None:
    if ts_utc is None:
        return None
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    return int((now_utc - ts_utc).total_seconds() // 86400)


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(f"seed_audit_failed=db_missing path={db_path}")
        return 2

    try:
        import duckdb
    except ImportError:
        print("seed_audit_failed=duckdb_missing_install pip install duckdb")
        return 3

    now_utc = datetime.now(timezone.utc)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        active_instruments = int(conn.execute("SELECT COUNT(*) FROM instruments WHERE active = TRUE").fetchone()[0])
        focus_rows = conn.execute(
            """
            SELECT symbol
            FROM instruments
            WHERE active = TRUE AND symbol IN (?, ?, ?, ?)
            ORDER BY symbol
            """,
            FOCUS_SEMIS,
        ).fetchall()
        focus_present = {row[0] for row in focus_rows}
        missing_focus = [s for s in FOCUS_SEMIS if s not in focus_present]

        missing_regime_rows = conn.execute(
            """
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            UNION ALL
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            UNION ALL
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            UNION ALL
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            UNION ALL
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            UNION ALL
            SELECT ? AS symbol
            WHERE NOT EXISTS (
              SELECT 1 FROM market_bars WHERE symbol = ? AND timeframe = '1d'
            )
            """,
            (
                REGIME_SYMBOLS[0],
                REGIME_SYMBOLS[0],
                REGIME_SYMBOLS[1],
                REGIME_SYMBOLS[1],
                REGIME_SYMBOLS[2],
                REGIME_SYMBOLS[2],
                REGIME_SYMBOLS[3],
                REGIME_SYMBOLS[3],
                REGIME_SYMBOLS[4],
                REGIME_SYMBOLS[4],
                REGIME_SYMBOLS[5],
                REGIME_SYMBOLS[5],
            ),
        ).fetchall()
        missing_regime = [row[0] for row in missing_regime_rows]

        bar_coverage = conn.execute(
            """
            WITH active AS (
              SELECT symbol FROM instruments WHERE active = TRUE
            ),
            bars AS (
              SELECT symbol, COUNT(*) AS n
              FROM market_bars
              WHERE timeframe = '1d'
              GROUP BY symbol
            )
            SELECT
              COUNT(*) AS active_count,
              SUM(CASE WHEN COALESCE(b.n, 0) >= ? THEN 1 ELSE 0 END) AS symbols_meeting_min
            FROM active a
            LEFT JOIN bars b ON b.symbol = a.symbol
            """,
            (MIN_DAILY_BARS_PER_ACTIVE_SYMBOL,),
        ).fetchone()
        active_count = int(bar_coverage[0] or 0)
        symbols_meeting_min = int(bar_coverage[1] or 0)

        latest_daily_bar = conn.execute(
            "SELECT MAX(ts_utc) FROM market_bars WHERE timeframe = '1d'"
        ).fetchone()[0]
        bar_staleness_days = _days_between(now_utc, latest_daily_bar)

        cutoff_30d = now_utc - timedelta(days=30)
        cutoff_90d = now_utc - timedelta(days=90)

        news_30d = conn.execute(
            """
            SELECT COUNT(*)
            FROM news_events
            WHERE published_at_utc >= ?
            """,
            (cutoff_30d,),
        ).fetchone()[0]
        links_30d = conn.execute(
            """
            SELECT COUNT(*)
            FROM news_symbols ns
            JOIN news_events ne
              ON ne.provider = ns.provider
             AND ne.provider_news_id = ns.provider_news_id
            WHERE ne.published_at_utc >= ?
            """,
            (cutoff_30d,),
        ).fetchone()[0]
        interp_30d = conn.execute(
            """
            SELECT COUNT(*)
            FROM news_interpretations ni
            JOIN news_events ne
              ON ne.provider = ni.provider
             AND ne.provider_news_id = ni.provider_news_id
            WHERE ne.published_at_utc >= ?
            """,
            (cutoff_30d,),
        ).fetchone()[0]

        series_count = int(conn.execute("SELECT COUNT(DISTINCT series_id) FROM macro_points").fetchone()[0] or 0)
        macro_points_90d = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM macro_points
                WHERE ts_utc >= ?
                """,
                (cutoff_90d,),
            ).fetchone()[0]
            or 0
        )
    finally:
        conn.close()

    checks: list[CheckResult] = []

    checks.append(
        CheckResult(
            name="instruments_active_count",
            status="PASS" if active_instruments >= MIN_ACTIVE_INSTRUMENTS else "FAIL",
            detail=f"active={active_instruments} required>={MIN_ACTIVE_INSTRUMENTS}",
        )
    )
    checks.append(
        CheckResult(
            name="focus_semis_present",
            status="PASS" if not missing_focus else "FAIL",
            detail=f"missing={missing_focus}" if missing_focus else "all focus semis present",
        )
    )
    checks.append(
        CheckResult(
            name="regime_symbols_present",
            status="PASS" if not missing_regime else "FAIL",
            detail=f"missing={missing_regime}" if missing_regime else "all regime symbols present",
        )
    )
    checks.append(
        CheckResult(
            name="daily_bar_coverage",
            status="PASS" if active_count > 0 and symbols_meeting_min == active_count else "FAIL",
            detail=(
                f"symbols_meeting_min={symbols_meeting_min}/{active_count} "
                f"(min_bars={MIN_DAILY_BARS_PER_ACTIVE_SYMBOL})"
            ),
        )
    )
    checks.append(
        CheckResult(
            name="daily_bar_recency",
            status=(
                "PASS"
                if bar_staleness_days is not None and bar_staleness_days <= MAX_DAILY_BAR_STALENESS_DAYS
                else "FAIL"
            ),
            detail=f"staleness_days={bar_staleness_days} max={MAX_DAILY_BAR_STALENESS_DAYS}",
        )
    )
    checks.append(
        CheckResult(
            name="news_events_30d",
            status="PASS" if int(news_30d) >= MIN_NEWS_EVENTS_30D else "FAIL",
            detail=f"count={int(news_30d)} required>={MIN_NEWS_EVENTS_30D}",
        )
    )
    checks.append(
        CheckResult(
            name="news_symbol_links_30d",
            status="PASS" if int(links_30d) >= MIN_NEWS_SYMBOL_LINKS_30D else "FAIL",
            detail=f"count={int(links_30d)} required>={MIN_NEWS_SYMBOL_LINKS_30D}",
        )
    )
    coverage = 0.0 if int(news_30d) == 0 else float(int(interp_30d) / int(news_30d))
    checks.append(
        CheckResult(
            name="news_interpretation_coverage_30d",
            status="PASS" if coverage >= MIN_INTERPRETATION_COVERAGE_30D else "FAIL",
            detail=f"coverage={coverage:.3f} required>={MIN_INTERPRETATION_COVERAGE_30D:.3f}",
        )
    )
    checks.append(
        CheckResult(
            name="macro_series_count",
            status="PASS" if series_count >= MIN_MACRO_SERIES else "FAIL",
            detail=f"series={series_count} required>={MIN_MACRO_SERIES}",
        )
    )
    checks.append(
        CheckResult(
            name="macro_points_90d",
            status="PASS" if macro_points_90d >= MIN_MACRO_POINTS_90D else "FAIL",
            detail=f"points={macro_points_90d} required>={MIN_MACRO_POINTS_90D}",
        )
    )

    failed = [c for c in checks if c.status != "PASS"]
    payload = {
        "audit_name": "seed_audit_v1",
        "as_of_utc": now_utc.isoformat(),
        "overall_status": "PASS" if not failed else "FAIL",
        "fail_count": len(failed),
        "checks": [asdict(c) for c in checks],
    }
    print(json.dumps(payload, indent=2))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
