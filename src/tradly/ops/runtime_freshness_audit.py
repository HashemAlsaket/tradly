from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from tradly.paths import get_repo_root
from tradly.services.db_time import from_db_utc
from tradly.services.market_calendar import previous_trading_day
from tradly.services.time_context import get_time_context


MARKET_OPEN_CT = time(8, 30)
MARKET_CLOSE_CT = time(15, 0)
MARKET_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class FreshnessCheck:
    name: str
    status: str
    detail: str


def _load_dotenv(path) -> None:
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


def _is_market_hours(now_local: datetime) -> bool:
    if now_local.weekday() >= 5:
        return False
    t = now_local.time()
    return MARKET_OPEN_CT <= t <= MARKET_CLOSE_CT


def _age_seconds_from_db_ts(ts: datetime | None, now_utc: datetime) -> int | None:
    if ts is None:
        return None
    return int((now_utc - from_db_utc(ts)).total_seconds())


def _market_date_from_db_ts(ts: datetime) -> date:
    return from_db_utc(ts).astimezone(MARKET_TZ).date()


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")
    db_path = repo_root / "data" / "tradly.duckdb"
    if not db_path.exists():
        print(json.dumps({"status": "FAIL", "reason": "db_missing", "path": str(db_path)}))
        return 2

    try:
        import duckdb
    except ImportError:
        print(json.dumps({"status": "FAIL", "reason": "duckdb_missing"}))
        return 3

    time_ctx = get_time_context()
    now_utc = time_ctx.now_utc
    now_local = time_ctx.now_local
    market_hours = _is_market_hours(now_local)

    news_max_age_min_market = int(os.getenv("TRADLY_NEWS_MAX_AGE_MINUTES_MARKET", "45"))
    news_max_age_min_offhours = int(os.getenv("TRADLY_NEWS_MAX_AGE_MINUTES_OFFHOURS", "240"))
    news_min_success_pulls_market = int(os.getenv("TRADLY_NEWS_MIN_SUCCESS_PULLS_MARKET", "1"))
    news_min_success_pulls_offhours = int(os.getenv("TRADLY_NEWS_MIN_SUCCESS_PULLS_OFFHOURS", "1"))
    interp_max_age_min_market = int(os.getenv("TRADLY_INTERP_MAX_AGE_MINUTES_MARKET", "60"))
    interp_max_age_min_offhours = int(os.getenv("TRADLY_INTERP_MAX_AGE_MINUTES_OFFHOURS", "240"))

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_daily_bar_utc = conn.execute(
            "SELECT MAX(ts_utc) FROM market_bars WHERE timeframe='1d'"
        ).fetchone()[0]
        latest_news_pull_utc = conn.execute(
            """
            SELECT MAX(created_at_utc)
            FROM news_pull_usage
            WHERE request_date_utc = ?
              AND response_status = 'success'
            """,
            (now_local.date(),),
        ).fetchone()[0]
        success_news_pulls_today = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM news_pull_usage
                WHERE request_date_utc = ?
                  AND response_status = 'success'
                """,
                (now_local.date(),),
            ).fetchone()[0]
            or 0
        )
        total_news_pulls_today = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM news_pull_usage
                WHERE request_date_utc = ?
                """,
                (now_local.date(),),
            ).fetchone()[0]
            or 0
        )
        latest_interp_utc = conn.execute("SELECT MAX(interpreted_at_utc) FROM news_interpretations").fetchone()[0]
        pending_uninterpreted_24h = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM news_events ne
                LEFT JOIN news_interpretations ni
                  ON ni.provider = ne.provider
                 AND ni.provider_news_id = ne.provider_news_id
                WHERE ne.published_at_utc >= ?
                  AND ni.provider_news_id IS NULL
                """,
                (now_utc - timedelta(hours=24),),
            ).fetchone()[0]
            or 0
        )
    finally:
        conn.close()

    checks: list[FreshnessCheck] = []

    if latest_daily_bar_utc is None:
        checks.append(FreshnessCheck("market_daily_bar_present", "FAIL", "no 1d bars found"))
    else:
        latest_daily_bar_market_date = _market_date_from_db_ts(latest_daily_bar_utc)
        expected_min_market_date = previous_trading_day(now_utc.astimezone(MARKET_TZ).date())
        if latest_daily_bar_market_date < expected_min_market_date:
            checks.append(
                FreshnessCheck(
                    "market_daily_bar_recency",
                    "FAIL",
                    (
                        f"latest_bar_market_date={latest_daily_bar_market_date} "
                        f"expected_min_market_date={expected_min_market_date}"
                    ),
                )
            )
        else:
            checks.append(
                FreshnessCheck(
                    "market_daily_bar_recency",
                    "PASS",
                    (
                        f"latest_bar_market_date={latest_daily_bar_market_date} "
                        f"expected_min_market_date={expected_min_market_date}"
                    ),
                )
            )

    # Pull recency is the correct heartbeat signal even when no new events are returned.
    if latest_news_pull_utc is None:
        checks.append(FreshnessCheck("news_pull_recency", "FAIL", "no news pull usage rows for local date"))
    else:
        age_sec = _age_seconds_from_db_ts(latest_news_pull_utc, now_utc)
        if age_sec is None:
            checks.append(FreshnessCheck("news_pull_recency", "FAIL", "timestamp_parse_failed"))
            age_sec = 10**9
        max_age_sec = (news_max_age_min_market if market_hours else news_max_age_min_offhours) * 60
        min_success = news_min_success_pulls_market if market_hours else news_min_success_pulls_offhours
        min_success_rate = float(os.getenv("TRADLY_NEWS_MIN_SUCCESS_RATE", "0.40"))
        success_rate = success_news_pulls_today / total_news_pulls_today if total_news_pulls_today else 0.0
        status = (
            "PASS"
            if age_sec <= max_age_sec
            and success_news_pulls_today >= min_success
            and success_rate >= min_success_rate
            else "FAIL"
        )
        checks.append(
            FreshnessCheck(
                "news_pull_recency",
                status,
                (
                    f"age_sec={age_sec} max_age_sec={max_age_sec} market_hours={market_hours} "
                    f"success_pulls_today={success_news_pulls_today} total_pulls_today={total_news_pulls_today} "
                    f"min_success={min_success} success_rate={success_rate:.3f} min_success_rate={min_success_rate:.3f}"
                ),
            )
        )

    # If no recent pending articles exist, interpretation staleness should not fail the run.
    if latest_interp_utc is None and pending_uninterpreted_24h > 0:
        checks.append(FreshnessCheck("news_interpretation_recency", "FAIL", "no news interpretations"))
    else:
        if latest_interp_utc is None:
            age_sec = 0
        else:
            parsed_age = _age_seconds_from_db_ts(latest_interp_utc, now_utc)
            age_sec = 10**9 if parsed_age is None else parsed_age
        max_age_sec = (interp_max_age_min_market if market_hours else interp_max_age_min_offhours) * 60
        status = "PASS"
        if pending_uninterpreted_24h > 0 and age_sec > max_age_sec:
            status = "FAIL"
        checks.append(
            FreshnessCheck(
                "news_interpretation_recency",
                status,
                (
                    f"age_sec={age_sec} max_age_sec={max_age_sec} market_hours={market_hours} "
                    f"pending_uninterpreted_24h={pending_uninterpreted_24h}"
                ),
            )
        )

    failed = [c for c in checks if c.status != "PASS"]
    payload = {
        "audit_name": "runtime_freshness_audit_v1",
        "as_of_utc": now_utc.isoformat(),
        "as_of_local": now_local.isoformat(),
        "market_hours": market_hours,
        "overall_status": "PASS" if not failed else "FAIL",
        "fail_count": len(failed),
        "metrics": {
            "latest_daily_bar_utc": from_db_utc(latest_daily_bar_utc).isoformat() if latest_daily_bar_utc else None,
            "latest_news_pull_utc": from_db_utc(latest_news_pull_utc).isoformat() if latest_news_pull_utc else None,
            "latest_interp_utc": from_db_utc(latest_interp_utc).isoformat() if latest_interp_utc else None,
            "success_news_pulls_today": success_news_pulls_today,
            "total_news_pulls_today": total_news_pulls_today,
        },
        "checks": [asdict(c) for c in checks],
    }
    print(json.dumps(payload, indent=2))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
