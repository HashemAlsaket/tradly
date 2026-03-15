from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import app


class DashboardRunLoadingTests(unittest.TestCase):
    def test_load_latest_run_artifact_uses_latest_dated_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir)
            older = runs_dir / "2026-03-14"
            newer = runs_dir / "2026-03-15"
            older.mkdir()
            newer.mkdir()
            (older / "ensemble_v1.json").write_text(
                json.dumps({"run_timestamp_utc": "2026-03-14T21:00:00+00:00", "rows": [{"scope_id": "OLD"}]}),
                encoding="utf-8",
            )
            (newer / "ensemble_v1.json").write_text(
                json.dumps({"run_timestamp_utc": "2026-03-15T05:43:27+00:00", "rows": [{"scope_id": "NEW"}]}),
                encoding="utf-8",
            )

            with patch.object(app, "RUNS_DIR", runs_dir):
                payload, path = app._load_latest_run_artifact("ensemble_v1.json")

            self.assertEqual(path, newer / "ensemble_v1.json")
            self.assertEqual(payload["rows"][0]["scope_id"], "NEW")


if __name__ == "__main__":
    unittest.main()
