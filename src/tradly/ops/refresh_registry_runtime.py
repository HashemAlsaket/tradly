from __future__ import annotations

import os
import subprocess
import sys

from tradly.paths import get_repo_root


STEPS = [
    ("sync_universe_registry", [sys.executable, "scripts/setup/sync_universe_registry.py"]),
    ("load_universe", [sys.executable, "-m", "tradly.pipeline.load_universe"]),
    ("run_cycle_reliable", [sys.executable, "scripts/ops/run_cycle_reliable.py"]),
]


def main() -> int:
    repo_root = get_repo_root()
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = "src" if not existing_pythonpath else f"src:{existing_pythonpath}"

    print("tradly_registry_refresh=starting")
    print("flow=sync_registry->load_universe->run_cycle_reliable")

    for step_name, cmd in STEPS:
        print(f"step_start={step_name} cmd={' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(repo_root), env=env)
        if result.returncode != 0:
            print(f"step_failed={step_name} exit_code={result.returncode}")
            print("tradly_registry_refresh=failed")
            return result.returncode
        print(f"step_ok={step_name}")

    print("tradly_registry_refresh=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
