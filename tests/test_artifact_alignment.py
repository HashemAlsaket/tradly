from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.services.artifact_alignment import assess_artifact_alignment


class ArtifactAlignmentTests(unittest.TestCase):
    def test_valid_when_artifact_is_recent(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        result = assess_artifact_alignment(
            artifact_name="market_regime_v1",
            payload={"run_timestamp_utc": "2026-03-15T00:30:00+00:00"},
            now_utc=now_utc,
            max_age=timedelta(hours=6),
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.reason_codes, ())

    def test_invalid_when_artifact_is_stale(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        result = assess_artifact_alignment(
            artifact_name="sector_movement_v1",
            payload={"run_timestamp_utc": "2026-03-14T18:00:00+00:00"},
            now_utc=now_utc,
            max_age=timedelta(hours=6),
        )

        self.assertFalse(result.valid)
        self.assertIn("sector_movement_v1_stale_for_downstream", result.reason_codes)


if __name__ == "__main__":
    unittest.main()
