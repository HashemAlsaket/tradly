from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.models.recommendation import action_for_horizon, build_recommendation_rows


class RecommendationTests(unittest.TestCase):
    def test_action_for_horizon_maps_actionable_bullish_to_buy(self) -> None:
        self.assertEqual(
            action_for_horizon(
                {
                    "state": "actionable",
                    "signal_direction": "bullish",
                    "confidence_score": 72,
                    "execution_ready": True,
                }
            ),
            "Buy",
        )

    def test_action_for_horizon_maps_actionable_bullish_not_ready_to_defer_buy(self) -> None:
        self.assertEqual(
            action_for_horizon(
                {
                    "state": "actionable",
                    "signal_direction": "bullish",
                    "confidence_score": 72,
                    "execution_ready": False,
                }
            ),
            "Defer Buy",
        )

    def test_action_for_horizon_maps_actionable_bearish_not_ready_to_defer_trim(self) -> None:
        self.assertEqual(
            action_for_horizon(
                {
                    "state": "actionable",
                    "signal_direction": "bearish",
                    "confidence_score": 72,
                    "execution_ready": False,
                }
            ),
            "Defer Trim",
        )

    def test_build_recommendation_rows_prefers_shorter_horizon_on_exact_tie(self) -> None:
        rows = build_recommendation_rows(
            ensemble_rows=[
                {
                    "scope_id": "NVDA",
                    "confidence_score": 70,
                    "horizon_summary": {
                        "1to3d": {
                            "state": "actionable",
                            "signal_direction": "bullish",
                            "confidence_score": 70,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": 50.0,
                            "why_code": ["market_context_supportive"],
                            "execution_ready": True,
                        },
                        "1to2w": {
                            "state": "actionable",
                            "signal_direction": "bullish",
                            "confidence_score": 70,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": 50.0,
                            "why_code": ["market_context_supportive"],
                            "execution_ready": True,
                        },
                        "2to6w": {
                            "state": "research_only",
                            "signal_direction": "bullish",
                            "confidence_score": 60,
                            "confidence_label": "medium",
                            "coverage_state": "thin_evidence",
                            "score_normalized": 30.0,
                            "why_code": ["market_context_headwind"],
                            "execution_ready": True,
                        },
                    },
                }
            ],
            now_utc=datetime(2026, 3, 15, 19, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["recommended_action"], "Buy")
        self.assertEqual(rows[0]["recommended_horizon"], "1to3d")

    def test_build_recommendation_rows_marks_market_headwind_long_as_contrarian(self) -> None:
        rows = build_recommendation_rows(
            ensemble_rows=[
                {
                    "scope_id": "NVDA",
                    "confidence_score": 68,
                    "horizon_summary": {
                        "1to3d": {
                            "state": "research_only",
                            "signal_direction": "neutral",
                            "confidence_score": 40,
                            "confidence_label": "low",
                            "coverage_state": "thin_evidence",
                            "score_normalized": 0.0,
                            "why_code": ["ensemble_signal_mixed"],
                            "execution_ready": False,
                        },
                        "1to2w": {
                            "state": "actionable",
                            "signal_direction": "bullish",
                            "confidence_score": 68,
                            "confidence_label": "medium",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": 42.0,
                            "why_code": ["market_context_headwind", "symbol_movement_supports_bullish"],
                            "execution_ready": True,
                        },
                        "2to6w": {
                            "state": "research_only",
                            "signal_direction": "bullish",
                            "confidence_score": 60,
                            "confidence_label": "medium",
                            "coverage_state": "thin_evidence",
                            "score_normalized": 35.0,
                            "why_code": ["market_context_headwind"],
                            "execution_ready": True,
                        },
                    },
                }
            ],
            now_utc=datetime(2026, 3, 15, 19, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["recommendation_class"], "contrarian_long")


if __name__ == "__main__":
    unittest.main()
