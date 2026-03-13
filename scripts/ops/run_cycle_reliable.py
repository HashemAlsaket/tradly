from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


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


def _run_step(cmd: list[str], cwd: Path, env: dict[str, str]) -> tuple[int, str, str]:
    res = subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True, text=True)
    return res.returncode, res.stdout, res.stderr


def _append_log(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _extract_json_payload(text: str) -> dict | None:
    blob = text.strip()
    if not blob:
        return None
    try:
        parsed = json.loads(blob)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    _load_dotenv(repo_root / ".env")

    max_attempts = int(os.getenv("TRADLY_CYCLE_MAX_ATTEMPTS", "2"))
    retry_sleep_sec = int(os.getenv("TRADLY_CYCLE_RETRY_SLEEP_SEC", "45"))

    lock_path = repo_root / "data" / "journal" / "cycle.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = repo_root / "data" / "journal" / "cycle_runs.jsonl"
    snapshot_path = repo_root / "data" / "journal" / "freshness_snapshot.json"

    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src:{existing_pythonpath}"

    started_at = datetime.now(timezone.utc)
    lock_fh = lock_path.open("w", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            payload = {
                "started_at_utc": started_at.isoformat(),
                "status": "SKIPPED_LOCK_HELD",
            }
            _append_log(log_path, payload)
            print("cycle_skipped=lock_held")
            return 0

        preflight_cmd = [sys.executable, "-m", "tradly.ops.preflight_catchup"]
        preflight_rc, preflight_out, preflight_err = _run_step(preflight_cmd, repo_root, env)
        preflight_payload = _extract_json_payload(preflight_out)

        if preflight_out:
            print(preflight_out)
        if preflight_err:
            print(preflight_err, file=sys.stderr)

        if preflight_rc != 0:
            ended_at = datetime.now(timezone.utc)
            payload = {
                "started_at_utc": started_at.isoformat(),
                "ended_at_utc": ended_at.isoformat(),
                "duration_sec": int((ended_at - started_at).total_seconds()),
                "preflight_rc": preflight_rc,
                "cycle_rc": None,
                "freshness_rc": None,
                "status": "FAIL",
                "preflight_lags": preflight_payload.get("lags") if preflight_payload else None,
                "preflight_actions": preflight_payload.get("actions") if preflight_payload else None,
                "preflight_stdout_tail": preflight_out[-3000:],
                "preflight_stderr_tail": preflight_err[-3000:],
                "reason": "preflight_catchup_failed",
            }
            _append_log(log_path, payload)
            return 1

        cycle_rc = 1
        cycle_stdout = ""
        cycle_stderr = ""
        for attempt in range(1, max_attempts + 1):
            cycle_cmd = [sys.executable, "-m", "tradly.pipeline.cycle"]
            rc, out, err = _run_step(cycle_cmd, repo_root, env)
            cycle_rc = rc
            cycle_stdout = out
            cycle_stderr = err
            print(f"cycle_attempt={attempt} rc={rc}")
            if rc == 0:
                break
            if attempt < max_attempts:
                time.sleep(retry_sleep_sec)

        freshness_cmd = [sys.executable, "-m", "tradly.ops.runtime_freshness_audit"]
        freshness_rc, freshness_out, freshness_err = _run_step(freshness_cmd, repo_root, env)
        freshness_payload = _extract_json_payload(freshness_out)

        ended_at = datetime.now(timezone.utc)
        payload = {
            "started_at_utc": started_at.isoformat(),
            "ended_at_utc": ended_at.isoformat(),
            "duration_sec": int((ended_at - started_at).total_seconds()),
            "preflight_rc": preflight_rc,
            "cycle_rc": cycle_rc,
            "freshness_rc": freshness_rc,
            "cycle_status": "PASS" if cycle_rc == 0 else "FAIL",
            "postflight_status": "PASS" if freshness_rc == 0 else "FAIL",
            "status": "PASS" if cycle_rc == 0 and freshness_rc == 0 else "FAIL",
            "preflight_lags": preflight_payload.get("lags") if preflight_payload else None,
            "preflight_actions": preflight_payload.get("actions") if preflight_payload else None,
            "preflight_stdout_tail": preflight_out[-3000:],
            "preflight_stderr_tail": preflight_err[-3000:],
            "cycle_stdout_tail": cycle_stdout[-3000:],
            "cycle_stderr_tail": cycle_stderr[-3000:],
            "freshness_stdout_tail": freshness_out[-3000:],
            "freshness_stderr_tail": freshness_err[-3000:],
        }
        _append_log(log_path, payload)
        if freshness_payload is not None:
            _write_json(
                snapshot_path,
                {
                    "written_at_utc": ended_at.isoformat(),
                    "cycle_started_at_utc": started_at.isoformat(),
                    "cycle_ended_at_utc": ended_at.isoformat(),
                    "cycle_status": payload["cycle_status"],
                    "postflight_status": payload["postflight_status"],
                    "overall_status": payload["status"],
                    "preflight_actions": payload["preflight_actions"],
                    "preflight_lags": payload["preflight_lags"],
                    "freshness": freshness_payload,
                },
            )

        if cycle_stdout:
            print(cycle_stdout)
        if cycle_stderr:
            print(cycle_stderr, file=sys.stderr)
        if freshness_out:
            print(freshness_out)
        if freshness_err:
            print(freshness_err, file=sys.stderr)

        return 0 if cycle_rc == 0 and freshness_rc == 0 else 1
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()


if __name__ == "__main__":
    raise SystemExit(main())
