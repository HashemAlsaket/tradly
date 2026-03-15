from __future__ import annotations

from datetime import datetime
import unittest

from tradly.models.recommendation_review import build_review_rows


class RecommendationReviewTests(unittest.TestCase):
    def test_build_review_rows_classifies_promote_contrarian_and_defer(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "XOM",
                    "recommended_action": "Buy",
                    "recommended_horizon": "2to6w",
                    "recommendation_class": "aligned_long",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 72,
                    "execution_ready": True,
                    "source_state": "actionable",
                },
                {
                    "scope_id": "NVDA",
                    "recommended_action": "Buy",
                    "recommended_horizon": "2to6w",
                    "recommendation_class": "contrarian_long",
                    "regime_alignment": "contrarian",
                    "signal_direction": "bullish",
                    "confidence_score": 75,
                    "execution_ready": True,
                    "source_state": "actionable",
                },
                {
                    "scope_id": "QQQ",
                    "recommended_action": "Defer Trim",
                    "recommended_horizon": "1to3d",
                    "recommendation_class": "deferred_short",
                    "regime_alignment": "aligned",
                    "signal_direction": "bearish",
                    "confidence_score": 63,
                    "execution_ready": False,
                    "source_state": "actionable",
                },
            ],
            now_utc=datetime(2026, 3, 15, 20, 0, 0),
        )

        by_scope = {row["scope_id"]: row for row in rows}
        self.assertEqual(by_scope["XOM"]["review_disposition"], "promote")
        self.assertEqual(by_scope["XOM"]["review_bucket"], "top_longs")
        self.assertEqual(by_scope["NVDA"]["review_disposition"], "review_required")
        self.assertEqual(by_scope["NVDA"]["review_bucket"], "contrarian_review")
        self.assertEqual(by_scope["QQQ"]["review_disposition"], "defer")
        self.assertEqual(by_scope["QQQ"]["review_bucket"], "deferred")


if __name__ == "__main__":
    unittest.main()
