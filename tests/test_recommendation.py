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
        self.assertEqual(rows[0]["evidence_balance_class"], "mixed_weak")
        self.assertEqual(rows[0]["regime_alignment"], "mixed")
        self.assertEqual(rows[0]["recommendation_class"], "mixed_weak_long")

    def test_build_recommendation_rows_marks_multi_confirm_headwind_long_as_mixed_strong(self) -> None:
        rows = build_recommendation_rows(
            ensemble_rows=[
                {
                    "scope_id": "NVDA",
                    "confidence_score": 72,
                    "horizon_summary": {
                        "1to2w": {
                            "state": "actionable",
                            "signal_direction": "bullish",
                            "confidence_score": 72,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": 35.0,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_supportive",
                                "symbol_news_supports_bullish",
                                "sector_news_supportive",
                            ],
                            "execution_ready": True,
                        },
                    },
                }
            ],
            now_utc=datetime(2026, 3, 15, 19, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["evidence_balance_class"], "mixed_strong")
        self.assertEqual(rows[0]["regime_alignment"], "mixed")
        self.assertEqual(rows[0]["recommendation_class"], "mixed_strong_long")

    def test_build_recommendation_rows_emits_explicit_symbol_for_symbol_scope(self) -> None:
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
                        }
                    },
                }
            ],
            now_utc=datetime(2026, 3, 15, 19, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["scope_id"], "NVDA")
        self.assertEqual(rows[0]["symbol"], "NVDA")

    def test_build_recommendation_rows_compresses_offhours_tactical_confidence(self) -> None:
        rows = build_recommendation_rows(
            ensemble_rows=[
                {
                    "scope_id": "NKE",
                    "confidence_score": 84,
                    "horizon_summary": {
                        "1to3d": {
                            "state": "actionable",
                            "signal_direction": "bearish",
                            "confidence_score": 84,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -76.9435,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "symbol_movement_supports_bearish",
                                "sector_news_supportive",
                            ],
                            "execution_ready": True,
                        },
                        "1to2w": {
                            "state": "actionable",
                            "signal_direction": "bearish",
                            "confidence_score": 55,
                            "confidence_label": "medium",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -22.3877,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "sector_news_supportive",
                                "component_conflict_high",
                            ],
                            "execution_ready": True,
                        },
                    },
                }
            ],
            now_utc=datetime(2026, 3, 19, 4, 55, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["recommended_action"], "Sell/Trim")
        self.assertEqual(rows[0]["actionability_class"], "tactical_offhours_fragile")
        self.assertLess(rows[0]["confidence_score"], 84)
        self.assertEqual(rows[0]["base_confidence_score"], 84)
        self.assertLess(rows[0]["confidence_adjustment"], 0)

    def test_build_recommendation_rows_adds_spread_to_aligned_shorts_from_cross_horizon_reinforcement(self) -> None:
        rows = build_recommendation_rows(
            ensemble_rows=[
                {
                    "scope_id": "GS",
                    "confidence_score": 55,
                    "horizon_summary": {
                        "1to3d": {
                            "state": "research_only",
                            "signal_direction": "bearish",
                            "confidence_score": 70,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -74.8616,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "symbol_movement_supports_bearish",
                                "upstream_lane_thin",
                            ],
                            "execution_ready": True,
                        },
                        "2to6w": {
                            "state": "actionable",
                            "signal_direction": "bearish",
                            "confidence_score": 55,
                            "confidence_label": "medium",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -52.2929,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "sector_news_headwind",
                            ],
                            "execution_ready": True,
                        },
                    },
                },
                {
                    "scope_id": "MA",
                    "confidence_score": 55,
                    "horizon_summary": {
                        "1to3d": {
                            "state": "research_only",
                            "signal_direction": "bearish",
                            "confidence_score": 70,
                            "confidence_label": "high",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -52.1958,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "symbol_movement_supports_bearish",
                                "upstream_lane_thin",
                            ],
                            "execution_ready": True,
                        },
                        "2to6w": {
                            "state": "actionable",
                            "signal_direction": "bearish",
                            "confidence_score": 55,
                            "confidence_label": "medium",
                            "coverage_state": "sufficient_evidence",
                            "score_normalized": -52.2929,
                            "why_code": [
                                "market_context_headwind",
                                "sector_context_headwind",
                                "sector_news_headwind",
                            ],
                            "execution_ready": True,
                        },
                    },
                },
            ],
            now_utc=datetime(2026, 3, 19, 4, 55, tzinfo=timezone.utc),
        )
        by_scope = {row["scope_id"]: row for row in rows}
        self.assertGreater(by_scope["GS"]["confidence_score"], by_scope["MA"]["confidence_score"])
        self.assertGreater(by_scope["GS"]["confidence_score"], 55)
        self.assertGreater(by_scope["MA"]["confidence_score"], 55)


if __name__ == "__main__":
    unittest.main()
