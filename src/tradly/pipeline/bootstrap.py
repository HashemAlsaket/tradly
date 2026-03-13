from __future__ import annotations

import os
import subprocess
import sys

from tradly.paths import get_repo_root


STEPS = [
    ("setup_db", "tradly.pipeline.setup_db"),
    ("load_universe", "tradly.pipeline.load_universe"),
    ("seed_macro_fred", "tradly.pipeline.seed_macro_fred"),
    ("seed_news_marketaux", "tradly.pipeline.seed_news_marketaux"),
    ("interpret_news_llm", "tradly.pipeline.interpret_news_llm"),
]


def main() -> int:
    repo_root = get_repo_root()
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

    print("bootstrap_status=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
