from __future__ import annotations

import unittest

from dashboard.app import _action_for_horizon, _best_decision


class DashboardDecisionTests(unittest.TestCase):
    def test_action_for_horizon_maps_actionable_bullish_to_buy(self) -> None:
        action = _action_for_horizon(
            {
                "state": "actionable",
                "signal_direction": "bullish",
                "confidence_score": 72,
            }
        )
        self.assertEqual(action, "Buy")

    def test_action_for_horizon_maps_actionable_bearish_to_sell_trim(self) -> None:
        action = _action_for_horizon(
            {
                "state": "actionable",
                "signal_direction": "bearish",
                "confidence_score": 60,
            }
        )
        self.assertEqual(action, "Sell/Trim")

    def test_best_decision_prefers_higher_priority_action(self) -> None:
        action, horizon = _best_decision(
            {
                "1to3d": {"state": "research_only", "signal_direction": "bullish", "confidence_score": 65},
                "1to2w": {"state": "actionable", "signal_direction": "bullish", "confidence_score": 70},
                "2to6w": {"state": "research_only", "signal_direction": "bearish", "confidence_score": 55},
            }
        )
        self.assertEqual(action, "Buy")
        self.assertEqual(horizon, "1to2w")


if __name__ == "__main__":
    unittest.main()
