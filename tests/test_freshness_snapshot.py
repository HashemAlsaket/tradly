from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tradly.ops.freshness_snapshot import run_and_write_runtime_freshness_snapshot


class FreshnessSnapshotTests(unittest.TestCase):
    def test_run_and_write_runtime_freshness_snapshot_persists_dashboard_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            env = {"PYTHONPATH": "src"}
            cycle_started = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
            cycle_ended = datetime(2026, 3, 15, 3, 5, tzinfo=timezone.utc)
            stdout = json.dumps(
                {
                    "overall_status": "PASS",
                    "checks": [],
                    "metrics": {"latest_daily_bar_utc": "2026-03-14T21:00:00+00:00"},
                }
            )

            class Result:
                returncode = 0
                stderr = ""
                def __init__(self, stdout_text: str) -> None:
                    self.stdout = stdout_text

            with patch("tradly.ops.freshness_snapshot.subprocess.run", return_value=Result(stdout)):
                rc, out, err, payload = run_and_write_runtime_freshness_snapshot(
                    env,
                    repo_root=repo_root,
                    cycle_started_at_utc=cycle_started,
                    cycle_ended_at_utc=cycle_ended,
                    cycle_status="PASS",
                )

            self.assertEqual(rc, 0)
            self.assertEqual(out, stdout)
            self.assertEqual(err, "")
            self.assertEqual(payload, json.loads(stdout))

            snapshot_path = repo_root / "data" / "journal" / "freshness_snapshot.json"
            self.assertTrue(snapshot_path.exists())
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["overall_status"], "PASS")
            self.assertEqual(snapshot["postflight_status"], "PASS")
            self.assertEqual(snapshot["cycle_status"], "PASS")
            self.assertEqual(snapshot["cycle_started_at_utc"], cycle_started.isoformat())
            self.assertEqual(snapshot["cycle_ended_at_utc"], cycle_ended.isoformat())
            self.assertEqual(snapshot["freshness"]["metrics"]["latest_daily_bar_utc"], "2026-03-14T21:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
