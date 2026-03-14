from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    cmd = [sys.executable, str(repo_root / "scripts" / "ops" / "run_cycle_reliable.py")]
    print("tradly_refresh=starting")
    print("entrypoint=scripts/ops/refresh_all.py")
    print("runner=scripts/ops/run_cycle_reliable.py")
    print("flow=preflight->cycle->postflight")
    res = subprocess.run(cmd, cwd=str(repo_root))
    print(f"tradly_refresh=finished rc={res.returncode}")
    return res.returncode


if __name__ == "__main__":
    raise SystemExit(main())
