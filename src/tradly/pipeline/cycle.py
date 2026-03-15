from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone

from tradly.ops.freshness_snapshot import run_and_write_runtime_freshness_snapshot
from tradly.paths import get_repo_root


STEPS = [
    ("ingest_market_bars", "tradly.pipeline.ingest_market_bars"),
    ("ingest_news_budgeted", "tradly.pipeline.ingest_news_budgeted"),
    ("interpret_news_llm", "tradly.pipeline.interpret_news_llm"),
    ("refresh_market_calendar_context", "tradly.ops.refresh_market_calendar_context"),
    ("run_market_regime", "tradly.pipeline.run_market_regime"),
    ("run_sector_movement", "tradly.pipeline.run_sector_movement"),
    ("run_symbol_movement", "tradly.pipeline.run_symbol_movement"),
    ("run_symbol_news", "tradly.pipeline.run_symbol_news"),
    ("run_sector_news", "tradly.pipeline.run_sector_news"),
    ("run_range_expectation", "tradly.pipeline.run_range_expectation"),
    ("run_ensemble", "tradly.pipeline.run_ensemble"),
    ("run_recommendation", "tradly.pipeline.run_recommendation"),
    ("run_recommendation_scorecard", "tradly.pipeline.run_recommendation_scorecard"),
    ("run_recommendation_scorecard_history", "tradly.pipeline.run_recommendation_scorecard_history"),
    ("run_recommendation_review", "tradly.pipeline.run_recommendation_review"),
]


def main() -> int:
    repo_root = get_repo_root()
    cycle_started_at_utc = datetime.now(timezone.utc)
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src:{existing_pythonpath}"

    for step_name, module_name in STEPS:
        cmd = [sys.executable, "-m", module_name]
        print(f"step_start={step_name} cmd={' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(repo_root), env=env)
        if result.returncode != 0:
            print(f"step_failed={step_name} exit_code={result.returncode}")
            return result.returncode
        print(f"step_ok={step_name}")

    cycle_ended_at_utc = datetime.now(timezone.utc)
    freshness_rc, freshness_out, freshness_err, _ = run_and_write_runtime_freshness_snapshot(
        env,
        repo_root=repo_root,
        cycle_started_at_utc=cycle_started_at_utc,
        cycle_ended_at_utc=cycle_ended_at_utc,
        cycle_status="PASS",
    )
    if freshness_out:
        print(freshness_out)
    if freshness_err:
        print(freshness_err, file=sys.stderr)
    if freshness_rc != 0:
        print(f"step_failed=runtime_freshness_snapshot exit_code={freshness_rc}")
        return freshness_rc

    print("step_ok=runtime_freshness_snapshot")
    print("cycle_status=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
