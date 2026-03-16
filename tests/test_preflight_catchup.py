from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.ops.preflight_catchup import _classify_macro_age_days, _intraday_source_status


class PreflightCatchupTests(unittest.TestCase):
    def test_macro_age_is_fresh_inside_warn_threshold(self) -> None:
        self.assertEqual(
            _classify_macro_age_days(age_days=2, warn_after_days=2, block_after_days=5),
            "fresh",
        )

    def test_macro_age_is_warning_between_warn_and_block_thresholds(self) -> None:
        self.assertEqual(
            _classify_macro_age_days(age_days=3, warn_after_days=2, block_after_days=5),
            "warning",
        )

    def test_macro_age_is_stale_beyond_block_threshold(self) -> None:
        self.assertEqual(
            _classify_macro_age_days(age_days=6, warn_after_days=2, block_after_days=5),
            "stale",
        )

    def test_intraday_source_not_required_on_weekend(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=None,
                now_utc=datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc),
                market_session="weekend",
                max_age_sec=1200,
            ),
            ("not_required", None),
        )

    def test_intraday_source_missing_when_session_active(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=None,
                now_utc=datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc),
                market_session="market_hours",
                max_age_sec=1200,
            ),
            ("missing", None),
        )


if __name__ == "__main__":
    unittest.main()
