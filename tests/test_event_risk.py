from __future__ import annotations

import unittest

from tradly.models.event_risk import build_event_risk_row


class EventRiskTests(unittest.TestCase):
    def test_build_event_risk_row_normalizes_active_event_damage(self) -> None:
        row = build_event_risk_row(
            symbol="MU",
            event_active=True,
            event_type="earnings",
            event_phase="post_event",
            event_timestamp_local="2026-03-18T16:30:00-04:00",
            event_source="earnings_watchlist",
            articles_considered=3,
            event_semantics={
                "reported_result_tone": "positive",
                "guidance_tone": "mixed",
                "dominant_positive": "beat estimates",
                "dominant_negative": "higher capex",
                "dominant_market_concern": "spending intensity",
                "summary_note": "Strong quarter with capex concern.",
            },
            reaction_interpretation={
                "reaction_state": "beat_but_rejected",
                "reaction_severity": "high",
                "confidence_adjustment": -18,
                "action_bias": "downgrade",
                "hard_cap_buy_to_watch": True,
                "reason_codes": ["event_damage", "capex_concern"],
                "summary_note": "Good print rejected by the tape.",
            },
            price_context={
                "price_reaction_pct": -5.2,
                "move_vs_qqq_pct": -4.0,
                "move_vs_sector_pct": -3.1,
                "market_session_state": "after_hours",
                "market_regime": "bearish",
                "macro_state": "macro_unstable",
            },
        )
        self.assertEqual(row["reaction_state"], "beat_but_rejected")
        self.assertEqual(row["reaction_severity"], "high")
        self.assertEqual(row["action_bias"], "downgrade")
        self.assertTrue(row["hard_cap_buy_to_watch"])
        self.assertEqual(row["confidence_adjustment"], -18)

    def test_build_event_risk_row_inactive_event_is_no_event_active(self) -> None:
        row = build_event_risk_row(
            symbol="NVDA",
            event_active=False,
            event_type="earnings",
            event_phase="inactive",
            event_timestamp_local=None,
            event_source="earnings_watchlist",
            articles_considered=0,
            event_semantics={},
            reaction_interpretation={},
            price_context={
                "price_reaction_pct": None,
                "move_vs_qqq_pct": None,
                "move_vs_sector_pct": None,
                "market_session_state": "after_hours",
                "market_regime": "bullish",
                "macro_state": "macro_stable",
            },
        )
        self.assertEqual(row["reaction_state"], "no_event_active")
        self.assertEqual(row["action_bias"], "hold")


if __name__ == "__main__":
    unittest.main()
