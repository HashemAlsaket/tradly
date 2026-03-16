from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from tradly.paths import get_repo_root


def extract_json_payload(text: str) -> dict | None:
    blob = text.strip()
    if not blob:
        return None
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def write_runtime_freshness_snapshot(
    snapshot_path: Path,
    freshness_payload: dict,
    *,
    cycle_started_at_utc: datetime | None = None,
    cycle_ended_at_utc: datetime | None = None,
    cycle_status: str = "PASS",
    postflight_status: str | None = None,
    preflight_actions: list[dict] | None = None,
    preflight_lags: dict | None = None,
) -> None:
    written_at = cycle_ended_at_utc or datetime.now(timezone.utc)
    payload = {
        "written_at_utc": written_at.isoformat(),
        "cycle_started_at_utc": cycle_started_at_utc.isoformat() if cycle_started_at_utc else None,
        "cycle_ended_at_utc": cycle_ended_at_utc.isoformat() if cycle_ended_at_utc else written_at.isoformat(),
        "cycle_status": cycle_status,
        "postflight_status": postflight_status or freshness_payload.get("overall_status", "UNKNOWN"),
        "overall_status": freshness_payload.get("overall_status", "UNKNOWN"),
        "preflight_actions": preflight_actions,
        "preflight_lags": preflight_lags,
        "freshness": freshness_payload,
    }
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def run_and_write_runtime_freshness_snapshot(
    env: dict[str, str],
    *,
    repo_root: Path | None = None,
    cycle_started_at_utc: datetime | None = None,
    cycle_ended_at_utc: datetime | None = None,
    cycle_status: str = "PASS",
    preflight_actions: list[dict] | None = None,
    preflight_lags: dict | None = None,
) -> tuple[int, str, str, dict | None]:
    resolved_repo_root = repo_root or get_repo_root()
    snapshot_path = resolved_repo_root / "data" / "journal" / "freshness_snapshot.json"
    cmd = [sys.executable, "-m", "tradly.ops.runtime_freshness_audit"]
    res = subprocess.run(cmd, cwd=str(resolved_repo_root), env=env, capture_output=True, text=True)
    freshness_payload = extract_json_payload(res.stdout)
    if freshness_payload is not None:
        write_runtime_freshness_snapshot(
            snapshot_path,
            freshness_payload,
            cycle_started_at_utc=cycle_started_at_utc,
            cycle_ended_at_utc=cycle_ended_at_utc,
            cycle_status=cycle_status,
            postflight_status="PASS" if res.returncode == 0 else "FAIL",
            preflight_actions=preflight_actions,
            preflight_lags=preflight_lags,
        )
    return res.returncode, res.stdout, res.stderr, freshness_payload
