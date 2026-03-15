from __future__ import annotations

import unittest

from tradly.ops.preflight_catchup import _classify_macro_age_days


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


if __name__ == "__main__":
    unittest.main()
