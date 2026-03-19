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
                    "evidence_balance_class": "aligned_strong",
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
                    "evidence_balance_class": "contrarian",
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
                    "evidence_balance_class": "aligned_lean",
                    "regime_alignment": "aligned",
                    "signal_direction": "bearish",
                    "confidence_score": 63,
                    "execution_ready": False,
                    "source_state": "actionable",
                },
                {
                    "scope_id": "NVDA_MIXED",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "recommendation_class": "mixed_weak_long",
                    "evidence_balance_class": "mixed_weak",
                    "regime_alignment": "mixed",
                    "signal_direction": "bullish",
                    "confidence_score": 74,
                    "execution_ready": True,
                    "source_state": "actionable",
                },
            ],
            now_utc=datetime(2026, 3, 15, 20, 0, 0),
        )

        by_scope = {row["scope_id"]: row for row in rows}
        self.assertEqual(by_scope["XOM"]["review_disposition"], "promote")
        self.assertEqual(by_scope["XOM"]["review_bucket"], "top_longs")
        self.assertEqual(by_scope["NVDA"]["review_disposition"], "review_required")
        self.assertEqual(by_scope["NVDA"]["review_bucket"], "contrarian_rebound")
        self.assertEqual(by_scope["QQQ"]["review_disposition"], "defer")
        self.assertEqual(by_scope["QQQ"]["review_bucket"], "deferred")
        self.assertEqual(by_scope["NVDA_MIXED"]["review_disposition"], "review_required")
        self.assertEqual(by_scope["NVDA_MIXED"]["review_bucket"], "manual_review")

    def test_build_review_rows_promotes_mixed_strong_high_confidence_long(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "NVDA",
                    "recommended_action": "Buy",
                    "recommended_horizon": "2to6w",
                    "recommendation_class": "mixed_strong_long",
                    "evidence_balance_class": "mixed_strong",
                    "regime_alignment": "mixed",
                    "signal_direction": "bullish",
                    "confidence_score": 75,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 15, 20, 0, 0),
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "mixed_strong_actionable")
        self.assertEqual(rows[0]["review_bucket"], "top_longs")

    def test_build_review_rows_defers_short_horizon_action_when_intraday_not_actionable(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "AAPL",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to3d",
                    "recommendation_class": "aligned_long",
                    "evidence_balance_class": "aligned_strong",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 80,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 16, 4, 0, 0),
            intraday_actionable=False,
        )
        self.assertEqual(rows[0]["review_disposition"], "defer")
        self.assertEqual(rows[0]["review_reason_code"], "intraday_freshness_not_ready")

    def test_healthcare_direct_news_thin_evidence_downgrades_promote(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "JNJ",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "recommendation_class": "aligned_long",
                    "evidence_balance_class": "aligned_strong",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 76,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 18, 15, 0, 0),
            symbol_metadata={
                "JNJ": {
                    "sector": "Healthcare",
                    "industry": "Drug Manufacturers - General",
                    "direct_news": True,
                    "onboarding_stage": "modeled_with_direct_news",
                    "roles": ["core_leader", "pharma_defensive"],
                }
            },
            symbol_news_rows_by_symbol={"JNJ": {"coverage_state": "thin_evidence"}},
        )
        self.assertEqual(rows[0]["review_disposition"], "review_required")
        self.assertEqual(rows[0]["review_reason_code"], "healthcare_thin_evidence")
        self.assertEqual(rows[0]["sector_subtype"], "pharma_defensive")

    def test_healthcare_tools_devices_promote_gets_specialized_reason(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "TMO",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "recommendation_class": "aligned_long",
                    "evidence_balance_class": "aligned_strong",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 74,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 18, 15, 0, 0),
            symbol_metadata={
                "TMO": {
                    "sector": "Healthcare",
                    "industry": "Diagnostics & Research",
                    "direct_news": False,
                    "onboarding_stage": "modeled",
                    "roles": ["quality_tools_devices"],
                }
            },
            symbol_news_rows_by_symbol={},
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "healthcare_tools_devices_actionable")
        self.assertEqual(rows[0]["sector_subtype"], "quality_tools_devices")


if __name__ == "__main__":
    unittest.main()
