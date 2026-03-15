from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from pathlib import Path as RealPath
from unittest.mock import patch


def _load_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "scripts" / "ops" / "run_cycle_reliable.py"
    spec = importlib.util.spec_from_file_location("run_cycle_reliable_script", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RunCycleReliableTests(unittest.TestCase):
    def test_reads_cycle_written_snapshot_instead_of_rewriting_it(self) -> None:
        module = _load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            snapshot_path = repo_root / "data" / "journal" / "freshness_snapshot.json"
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_payload = {
                "written_at_utc": "2026-03-15T03:05:00+00:00",
                "cycle_status": "PASS",
                "postflight_status": "PASS",
                "overall_status": "PASS",
                "freshness": {"overall_status": "PASS", "checks": []},
            }
            snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")

            preflight_payload = {"lags": {"market_data": "ok"}, "actions": []}
            step_results = [
                (0, json.dumps(preflight_payload), ""),
                (0, "cycle ok", ""),
            ]

            def fake_run_step(cmd, cwd, env):
                return step_results.pop(0)

            class FakeLockFile:
                def fileno(self):
                    return 0

                def close(self):
                    return None

            real_path_open = RealPath.open

            def fake_path_open(path_obj, *args, **kwargs):
                if path_obj == repo_root / "data" / "journal" / "cycle.lock":
                    return FakeLockFile()
                return real_path_open(path_obj, *args, **kwargs)

            with patch.object(module.Path, "resolve", return_value=repo_root / "scripts" / "ops" / "run_cycle_reliable.py"), \
                patch.object(module, "_load_dotenv"), \
                patch.object(module, "_run_step", side_effect=fake_run_step), \
                patch.object(module, "_append_log") as append_log, \
                patch.object(module, "time"), \
                patch.object(module, "fcntl") as fake_fcntl, \
                patch.object(module.Path, "open", new=fake_path_open), \
                patch.object(module.os, "getenv", side_effect=lambda key, default=None: default):
                fake_fcntl.LOCK_EX = 1
                fake_fcntl.LOCK_NB = 2
                fake_fcntl.LOCK_UN = 8
                fake_fcntl.flock.return_value = None
                rc = module.main()

            self.assertEqual(rc, 0)
            append_log.assert_called_once()
            logged_payload = append_log.call_args.args[1]
            self.assertEqual(logged_payload["freshness_rc"], 0)
            self.assertEqual(logged_payload["postflight_status"], "PASS")
            self.assertEqual(logged_payload["status"], "PASS")
            self.assertEqual(logged_payload["freshness_stdout_tail"], json.dumps(snapshot_payload["freshness"], ensure_ascii=True, indent=2))
            self.assertEqual(json.loads(snapshot_path.read_text(encoding="utf-8")), snapshot_payload)


if __name__ == "__main__":
    unittest.main()
