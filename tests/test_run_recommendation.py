from __future__ import annotations

import unittest

from tradly.pipeline.run_recommendation import _input_status


class RunRecommendationTests(unittest.TestCase):
    def test_input_status_inherits_ensemble_thin_evidence(self) -> None:
        status = _input_status(
            {"input_audit": {"status": "thin_evidence"}, "quality_audit": {"status": "pass"}},
            [{"scope_id": "NVDA"}],
        )
        self.assertEqual(status, "thin_evidence")

    def test_input_status_falls_back_to_ready_when_ensemble_is_ready(self) -> None:
        status = _input_status(
            {"input_audit": {"status": "ready"}, "quality_audit": {"status": "pass"}},
            [{"scope_id": "NVDA"}],
        )
        self.assertEqual(status, "ready")


if __name__ == "__main__":
    unittest.main()
