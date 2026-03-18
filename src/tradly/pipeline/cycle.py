from __future__ import annotations

import os
import json
import subprocess
import sys
from datetime import datetime, timezone

from tradly.ops.freshness_snapshot import run_and_write_runtime_freshness_snapshot
from tradly.paths import get_repo_root


PREFLIGHT_MODULE = "tradly.ops.preflight_catchup"
SKIP_PREFLIGHT_ENV = "TRADLY_SKIP_PREFLIGHT_CATCHUP"

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
    ("run_portfolio_policy", "tradly.pipeline.run_portfolio_policy"),
]


def _run_step(step_name: str, module_name: str, repo_root, env: dict[str, str]) -> int:
    cmd = [sys.executable, "-m", module_name]
    print(f"step_start={step_name} cmd={' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(repo_root), env=env)
    if result.returncode != 0:
        print(f"step_failed={step_name} exit_code={result.returncode}")
        return result.returncode
    print(f"step_ok={step_name}")
    return 0


def _extract_json_payload(text: str) -> dict | None:
    blob = text.strip()
    if not blob:
        return None
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def main() -> int:
    repo_root = get_repo_root()
    cycle_started_at_utc = datetime.now(timezone.utc)
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src:{existing_pythonpath}"

    if env.get(SKIP_PREFLIGHT_ENV) == "1":
        print("step_skipped=preflight_catchup reason=env_skip")
        preflight_payload = None
    else:
        cmd = [sys.executable, "-m", PREFLIGHT_MODULE]
        print(f"step_start=preflight_catchup cmd={' '.join(cmd)}")
        res = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print(res.stderr, file=sys.stderr)
        preflight_payload = _extract_json_payload(res.stdout)
        preflight_rc = res.returncode
        if preflight_rc != 0:
            print(f"step_failed=preflight_catchup exit_code={preflight_rc}")
            return preflight_rc
        print("step_ok=preflight_catchup")

    for step_name, module_name in STEPS:
        step_rc = _run_step(step_name, module_name, repo_root, env)
        if step_rc != 0:
            return step_rc

    cycle_ended_at_utc = datetime.now(timezone.utc)
    freshness_rc, freshness_out, freshness_err, _ = run_and_write_runtime_freshness_snapshot(
        env,
        repo_root=repo_root,
        cycle_started_at_utc=cycle_started_at_utc,
        cycle_ended_at_utc=cycle_ended_at_utc,
        cycle_status="PASS",
        preflight_actions=preflight_payload.get("actions") if isinstance(preflight_payload, dict) else None,
        preflight_lags=preflight_payload.get("final_lags") if isinstance(preflight_payload, dict) else None,
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
