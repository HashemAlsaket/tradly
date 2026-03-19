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
        self.assertEqual(by_scope["NVDA_MIXED"]["review_disposition"], "promote")
        self.assertEqual(by_scope["NVDA_MIXED"]["review_bucket"], "top_longs")

    def test_build_review_rows_promotes_mixed_weak_high_confidence_swing_long(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "AMD",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "recommendation_class": "mixed_weak_long",
                    "evidence_balance_class": "mixed_weak",
                    "regime_alignment": "mixed",
                    "signal_direction": "bullish",
                    "confidence_score": 67,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 15, 20, 0, 0),
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "mixed_cautious_actionable")

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

    def test_industrials_direct_news_thin_evidence_downgrades_promote(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "GE",
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
            now_utc=datetime(2026, 3, 19, 4, 0, 0),
            symbol_metadata={
                "GE": {
                    "sector": "Industrials",
                    "industry": "Aerospace & Defense",
                    "direct_news": True,
                    "onboarding_stage": "modeled_with_direct_news",
                    "roles": ["core_leader", "aerospace_defense"],
                }
            },
            symbol_news_rows_by_symbol={"GE": {"coverage_state": "thin_evidence"}},
        )
        self.assertEqual(rows[0]["review_disposition"], "review_required")
        self.assertEqual(rows[0]["review_reason_code"], "industrials_thin_evidence")
        self.assertEqual(rows[0]["sector_subtype"], "aerospace_defense")

    def test_industrials_promote_gets_specialized_reason(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "CAT",
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
            now_utc=datetime(2026, 3, 19, 4, 0, 0),
            symbol_metadata={
                "CAT": {
                    "sector": "Industrials",
                    "industry": "Farm & Heavy Construction Machinery",
                    "direct_news": False,
                    "onboarding_stage": "modeled",
                    "roles": ["core_leader", "heavy_equipment_capex"],
                }
            },
            symbol_news_rows_by_symbol={},
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "industrials_heavy_equipment_actionable")
        self.assertEqual(rows[0]["sector_subtype"], "heavy_equipment_capex")

    def test_event_risk_caps_buy_to_watch(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "MU",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to3d",
                    "recommendation_class": "aligned_long",
                    "evidence_balance_class": "aligned_strong",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 81,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 19, 2, 0, 0),
            event_risk_rows_by_symbol={
                "MU": {
                    "event_active": True,
                    "reaction_state": "beat_but_rejected",
                    "reaction_severity": "high",
                    "action_bias": "downgrade",
                    "hard_cap_buy_to_watch": True,
                }
            },
        )
        self.assertEqual(rows[0]["review_disposition"], "watch")
        self.assertEqual(rows[0]["review_reason_code"], "event_buy_capped_to_watch")
        self.assertEqual(rows[0]["event_reaction_state"], "beat_but_rejected")
        self.assertEqual(rows[0]["review_bucket"], "watch_event_damaged")

    def test_market_stress_turns_marginal_promote_into_review_required(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "CVX",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "recommendation_class": "aligned_long",
                    "evidence_balance_class": "aligned_strong",
                    "regime_alignment": "aligned",
                    "signal_direction": "bullish",
                    "confidence_score": 68,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 19, 3, 0, 0),
            market_row={
                "signal_direction": "bearish",
                "confidence_score": 73,
                "why_code": ["vix_elevated", "macro_rates_pressure", "macro_energy_stress"],
                "evidence": {"macro_hostility": {"macro_state": "risk_off"}},
            },
        )
        self.assertEqual(rows[0]["review_disposition"], "watch")
        self.assertEqual(rows[0]["review_reason_code"], "market_stress_watch")
        self.assertEqual(rows[0]["market_stress_level"], "high")
        self.assertEqual(rows[0]["review_bucket"], "watch_tape_blocked")

    def test_market_stress_keeps_strong_survivor_promoted(self) -> None:
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
                    "confidence_score": 78,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 19, 3, 0, 0),
            market_row={
                "signal_direction": "bearish",
                "confidence_score": 73,
                "why_code": ["vix_elevated", "macro_rates_pressure", "macro_energy_stress"],
                "evidence": {"macro_hostility": {"macro_state": "risk_off"}},
            },
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "risk_off_survivor")

    def test_market_stress_can_promote_high_confidence_mixed_review_required_buy(self) -> None:
        rows = build_review_rows(
            recommendation_rows=[
                {
                    "scope_id": "NVDA",
                    "recommended_action": "Buy",
                    "recommended_horizon": "2to6w",
                    "recommendation_class": "mixed_weak_long",
                    "evidence_balance_class": "mixed_weak",
                    "regime_alignment": "mixed",
                    "signal_direction": "bullish",
                    "confidence_score": 72,
                    "execution_ready": True,
                    "source_state": "actionable",
                }
            ],
            now_utc=datetime(2026, 3, 19, 3, 0, 0),
            market_row={
                "signal_direction": "bearish",
                "confidence_score": 79,
                "why_code": ["vix_elevated", "macro_rates_pressure", "macro_energy_stress"],
                "evidence": {"macro_hostility": {"macro_state": "risk_off"}},
            },
        )
        self.assertEqual(rows[0]["review_disposition"], "promote")
        self.assertEqual(rows[0]["review_reason_code"], "risk_off_survivor")


if __name__ == "__main__":
    unittest.main()
