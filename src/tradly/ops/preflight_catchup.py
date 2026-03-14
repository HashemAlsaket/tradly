from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from tradly.paths import get_repo_root
from tradly.services.db_time import from_db_utc
from tradly.services.market_calendar import previous_trading_day
from tradly.services.time_context import get_time_context


@dataclass(frozen=True)
class SourceLag:
    source: str
    status: str
    detail: str
    backfill_from: str | None = None
    backfill_to: str | None = None


MARKET_TZ = ZoneInfo("America/New_York")


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


def _run_module(module: str, repo_root, env: dict[str, str]) -> tuple[int, str, str]:
    cmd = [sys.executable, "-m", module]
    res = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True)
    return res.returncode, res.stdout, res.stderr


def _age_seconds(ts: datetime | None, now_utc: datetime) -> int | None:
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
    expected_min_market_date = previous_trading_day(now_utc.astimezone(MARKET_TZ).date())
    news_pull_max_age_sec = int(os.getenv("TRADLY_PREFLIGHT_NEWS_PULL_MAX_AGE_SEC", "3600"))
    macro_max_age_days = int(os.getenv("TRADLY_PREFLIGHT_MACRO_MAX_AGE_DAYS", "2"))
    interp_lookback_days = int(os.getenv("TRADLY_PREFLIGHT_INTERPRET_LOOKBACK_DAYS", "7"))

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        latest_market_bar = conn.execute(
            "SELECT MAX(ts_utc) FROM market_bars WHERE timeframe='1d'"
        ).fetchone()[0]
        latest_news_pull = conn.execute(
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
        latest_news_event = conn.execute("SELECT MAX(published_at_utc) FROM news_events").fetchone()[0]
        latest_interp = conn.execute("SELECT MAX(interpreted_at_utc) FROM news_interpretations").fetchone()[0]
        latest_macro = conn.execute("SELECT MAX(ts_utc) FROM macro_points").fetchone()[0]
    finally:
        conn.close()

    lags: list[SourceLag] = []
    actions: list[str] = []

    market_stale = True
    if latest_market_bar is not None:
        latest_market_date = _market_date_from_db_ts(latest_market_bar)
        market_stale = latest_market_date < expected_min_market_date
        backfill_from = (
            (latest_market_date - timedelta(days=2)).isoformat() if market_stale else None
        )
        backfill_to = expected_min_market_date.isoformat() if market_stale else None
        lags.append(
            SourceLag(
                source="market_bars_1d",
                status="stale" if market_stale else "fresh",
                detail=f"latest_market_date={latest_market_date} expected_min_market_date={expected_min_market_date}",
                backfill_from=backfill_from,
                backfill_to=backfill_to,
            )
        )
    else:
        lags.append(
            SourceLag(
                source="market_bars_1d",
                status="stale",
                detail="no rows",
                backfill_from=(expected_min_market_date - timedelta(days=180)).isoformat(),
                backfill_to=expected_min_market_date.isoformat(),
            )
        )

    news_pull_age = _age_seconds(latest_news_pull, now_utc)
    news_pull_stale = (
        news_pull_age is None or news_pull_age > news_pull_max_age_sec or success_news_pulls_today < 1
    )
    lags.append(
        SourceLag(
            source="news_pull_usage",
            status="stale" if news_pull_stale else "fresh",
            detail=(
                f"age_sec={news_pull_age} max_age_sec={news_pull_max_age_sec} "
                f"success_pulls_today={success_news_pulls_today}"
            ),
            backfill_from=(now_local - timedelta(hours=6)).isoformat() if news_pull_stale else None,
            backfill_to=now_local.isoformat() if news_pull_stale else None,
        )
    )

    interp_age = _age_seconds(latest_interp, now_utc)
    lags.append(
        SourceLag(
            source="news_interpretations",
            status="unknown" if interp_age is None else "fresh",
            detail=f"age_sec={interp_age}",
        )
    )

    macro_stale = True
    if latest_macro is not None:
        latest_macro = from_db_utc(latest_macro)
        macro_age_days = int((now_utc.date() - latest_macro.date()).days)
        macro_stale = macro_age_days > macro_max_age_days
        macro_backfill_from = (
            (latest_macro.date() - timedelta(days=3)).isoformat() if macro_stale else None
        )
        macro_backfill_to = now_utc.date().isoformat() if macro_stale else None
        lags.append(
            SourceLag(
                source="macro_points",
                status="stale" if macro_stale else "fresh",
                detail=f"latest_date={latest_macro.date()} age_days={macro_age_days} max_age_days={macro_max_age_days}",
                backfill_from=macro_backfill_from,
                backfill_to=macro_backfill_to,
            )
        )
    else:
        lags.append(
            SourceLag(
                source="macro_points",
                status="stale",
                detail="no rows",
                backfill_from=(now_utc.date() - timedelta(days=730)).isoformat(),
                backfill_to=now_utc.date().isoformat(),
            )
        )

    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src:{existing_pythonpath}"
    runs: list[dict] = []

    if market_stale:
        actions.append("ingest_market_bars")
        market_lag = next((lag for lag in lags if lag.source == "market_bars_1d"), None)
        env_market = dict(env)
        if market_lag and market_lag.backfill_from and market_lag.backfill_to:
            env_market["TRADLY_MARKET_FROM_DATE"] = market_lag.backfill_from
            env_market["TRADLY_MARKET_TO_DATE"] = market_lag.backfill_to
        rc, out, err = _run_module("tradly.pipeline.ingest_market_bars", repo_root, env_market)
        runs.append({"step": "ingest_market_bars", "rc": rc, "stdout_tail": out[-1200:], "stderr_tail": err[-1200:]})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "FAIL",
                        "phase": "preflight_catchup",
                        "reason": "ingest_market_bars_failed",
                        "lags": [asdict(x) for x in lags],
                        "actions": actions,
                        "runs": runs,
                    },
                    indent=2,
                )
            )
            return 1

    if news_pull_stale:
        actions.append("ingest_news_budgeted")
        env_news = dict(env)
        env_news["TRADLY_NEWS_PUBLISHED_AFTER_UTC"] = (now_utc - timedelta(hours=6)).isoformat()
        rc, out, err = _run_module("tradly.pipeline.ingest_news_budgeted", repo_root, env_news)
        runs.append(
            {"step": "ingest_news_budgeted", "rc": rc, "stdout_tail": out[-1200:], "stderr_tail": err[-1200:]}
        )
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "FAIL",
                        "phase": "preflight_catchup",
                        "reason": "ingest_news_budgeted_failed",
                        "lags": [asdict(x) for x in lags],
                        "actions": actions,
                        "runs": runs,
                    },
                    indent=2,
                )
            )
            return 1

        actions.append("interpret_news_llm")
        env_with_lookback = dict(env)
        env_with_lookback["NEWS_INTERPRET_LOOKBACK_DAYS"] = str(interp_lookback_days)
        rc, out, err = _run_module("tradly.pipeline.interpret_news_llm", repo_root, env_with_lookback)
        runs.append({"step": "interpret_news_llm", "rc": rc, "stdout_tail": out[-1200:], "stderr_tail": err[-1200:]})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "FAIL",
                        "phase": "preflight_catchup",
                        "reason": "interpret_news_llm_failed",
                        "lags": [asdict(x) for x in lags],
                        "actions": actions,
                        "runs": runs,
                    },
                    indent=2,
                )
            )
            return 1

    if macro_stale:
        actions.append("seed_macro_fred")
        macro_lag = next((lag for lag in lags if lag.source == "macro_points"), None)
        env_macro = dict(env)
        if macro_lag and macro_lag.backfill_from and macro_lag.backfill_to:
            env_macro["TRADLY_MACRO_FROM_DATE"] = macro_lag.backfill_from
            env_macro["TRADLY_MACRO_TO_DATE"] = macro_lag.backfill_to
        rc, out, err = _run_module("tradly.pipeline.seed_macro_fred", repo_root, env_macro)
        runs.append({"step": "seed_macro_fred", "rc": rc, "stdout_tail": out[-1200:], "stderr_tail": err[-1200:]})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "FAIL",
                        "phase": "preflight_catchup",
                        "reason": "seed_macro_fred_failed",
                        "lags": [asdict(x) for x in lags],
                        "actions": actions,
                        "runs": runs,
                    },
                    indent=2,
                )
            )
            return 1

    # Post-catchup verification: preflight must not pass while required sources are still stale.
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        final_latest_market_bar = conn.execute(
            "SELECT MAX(ts_utc) FROM market_bars WHERE timeframe='1d'"
        ).fetchone()[0]
        final_latest_news_pull = conn.execute(
            """
            SELECT MAX(created_at_utc)
            FROM news_pull_usage
            WHERE request_date_utc = ?
              AND response_status = 'success'
            """,
            (now_local.date(),),
        ).fetchone()[0]
        final_success_news_pulls_today = int(
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
        final_latest_macro = conn.execute("SELECT MAX(ts_utc) FROM macro_points").fetchone()[0]
    finally:
        conn.close()

    final_lags: list[SourceLag] = []

    if final_latest_market_bar is None:
        final_lags.append(SourceLag("market_bars_1d", "stale", "no rows"))
    else:
        final_market_date = _market_date_from_db_ts(final_latest_market_bar)
        final_market_stale = final_market_date < expected_min_market_date
        final_lags.append(
            SourceLag(
                source="market_bars_1d",
                status="stale" if final_market_stale else "fresh",
                detail=(
                    f"latest_market_date={final_market_date} expected_min_market_date={expected_min_market_date}"
                ),
            )
        )

    final_news_age = _age_seconds(final_latest_news_pull, now_utc)
    final_news_stale = (
        final_news_age is None or final_news_age > news_pull_max_age_sec or final_success_news_pulls_today < 1
    )
    final_lags.append(
        SourceLag(
            source="news_pull_usage",
            status="stale" if final_news_stale else "fresh",
            detail=(
                f"age_sec={final_news_age} max_age_sec={news_pull_max_age_sec} "
                f"success_pulls_today={final_success_news_pulls_today}"
            ),
        )
    )

    if final_latest_macro is None:
        final_lags.append(SourceLag("macro_points", "stale", "no rows"))
    else:
        final_latest_macro = from_db_utc(final_latest_macro)
        final_macro_age_days = int((now_utc.date() - final_latest_macro.date()).days)
        final_macro_stale = final_macro_age_days > macro_max_age_days
        final_lags.append(
            SourceLag(
                source="macro_points",
                status="stale" if final_macro_stale else "fresh",
                detail=(
                    f"latest_date={final_latest_macro.date()} age_days={final_macro_age_days} "
                    f"max_age_days={macro_max_age_days}"
                ),
            )
        )

    unresolved = [x for x in final_lags if x.status == "stale"]
    if unresolved:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "phase": "preflight_catchup",
                    "reason": "post_catchup_stale_sources",
                    "lags": [asdict(x) for x in lags],
                    "actions": actions,
                    "runs": runs,
                    "final_lags": [asdict(x) for x in final_lags],
                },
                indent=2,
            )
        )
        return 1

    print(
        json.dumps(
            {
                "status": "PASS",
                "phase": "preflight_catchup",
                "as_of_utc": now_utc.isoformat(),
                "as_of_local": now_local.isoformat(),
                "lags": [asdict(x) for x in lags],
                "actions": actions,
                "runs": runs,
                "final_lags": [asdict(x) for x in final_lags],
                "latest_news_event_utc": from_db_utc(latest_news_event).isoformat() if latest_news_event else None,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
