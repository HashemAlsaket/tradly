from __future__ import annotations

import unittest

from tradly.services.investability_gate import apply_investability_gate


class InvestabilityGateTests(unittest.TestCase):
    def test_investable_keeps_buy_action(self) -> None:
        result = apply_investability_gate("Buy", "investable")
        self.assertEqual(result.final_action, "Buy")
        self.assertFalse(result.blocked)
        self.assertIsNone(result.reason_code)

    def test_probably_not_halal_forces_watch(self) -> None:
        result = apply_investability_gate("Strong Buy", "probably_not_halal")
        self.assertEqual(result.final_action, "Watch")
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason_code, "investability_blocked")

    def test_not_halal_forces_watch(self) -> None:
        result = apply_investability_gate("Buy", "not_halal")
        self.assertEqual(result.final_action, "Watch")
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason_code, "investability_blocked")

    def test_review_required_forces_watch_for_buy(self) -> None:
        result = apply_investability_gate("Buy", "review_required")
        self.assertEqual(result.final_action, "Watch")
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason_code, "investability_review_required")

    def test_review_required_allows_trim(self) -> None:
        result = apply_investability_gate("Trim", "review_required")
        self.assertEqual(result.final_action, "Trim")
        self.assertFalse(result.blocked)

    def test_unknown_status_forces_watch(self) -> None:
        result = apply_investability_gate("Buy", "unknown")
        self.assertEqual(result.final_action, "Watch")
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason_code, "investability_unknown_status")


if __name__ == "__main__":
    unittest.main()
