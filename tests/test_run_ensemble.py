from __future__ import annotations

import unittest

from tradly.pipeline.run_ensemble import _input_status


class RunEnsembleTests(unittest.TestCase):
    def test_input_status_downgrades_when_upstream_thin_is_widespread(self) -> None:
        rows = [
            {"coverage_state": "sufficient_evidence", "why_code": ["upstream_lane_thin"]}
            for _ in range(20)
        ] + [
            {"coverage_state": "sufficient_evidence", "why_code": []}
            for _ in range(4)
        ]
        self.assertEqual(_input_status(rows), "thin_evidence")

    def test_input_status_ready_when_rows_are_clean(self) -> None:
        rows = [
            {"coverage_state": "sufficient_evidence", "why_code": ["symbol_movement_supports_bullish"]}
            for _ in range(10)
        ]
        self.assertEqual(_input_status(rows), "ready")


if __name__ == "__main__":
    unittest.main()
