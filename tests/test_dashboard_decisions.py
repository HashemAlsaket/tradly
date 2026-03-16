from __future__ import annotations

import unittest

from dashboard.app import _action_section_blurb, _decision_rows, _render_action_board


class DashboardDecisionTests(unittest.TestCase):
    def test_decision_rows_use_review_artifact(self) -> None:
        rows = _decision_rows(
            {
                "rows": [
                    {
                        "symbol": "NVDA",
                        "scope_id": "NVDA",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to2w",
                        "confidence_score": 70,
                        "primary_reason_code": "market_context_headwind",
                        "execution_ready": True,
                        "recommendation_class": "contrarian_long",
                        "review_disposition": "review_required",
                        "review_bucket": "contrarian_rebound",
                    }
                ]
            }
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Symbol"], "NVDA")
        self.assertEqual(rows[0]["Action"], "Buy")
        self.assertEqual(rows[0]["Horizon"], "1to2w")
        self.assertEqual(rows[0]["Confidence"], 70)
        self.assertEqual(rows[0]["ReviewDisposition"], "review_required")

    def test_review_required_rows_do_not_need_to_live_in_watch_bucket(self) -> None:
        payload = {
            "rows": [
                {
                    "symbol": "NVDA",
                    "scope_id": "NVDA",
                    "recommended_action": "Buy",
                    "recommended_horizon": "1to2w",
                    "confidence_score": 70,
                    "primary_reason_code": "market_context_headwind",
                    "execution_ready": True,
                    "recommendation_class": "contrarian_long",
                    "review_disposition": "review_required",
                    "review_bucket": "contrarian_rebound",
                }
            ]
        }
        rows = _decision_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Symbol"], "NVDA")

    def test_decision_rows_prefer_symbol_over_scope_id(self) -> None:
        rows = _decision_rows(
            {
                "rows": [
                    {
                        "symbol": "NVDA",
                        "scope_id": "UNSET_SCOPE",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to2w",
                        "confidence_score": 70,
                        "primary_reason_code": "market_context_headwind",
                        "execution_ready": True,
                        "recommendation_class": "aligned_long",
                        "review_disposition": "review_required",
                        "review_bucket": "manual_review",
                    }
                ]
            }
        )
        self.assertEqual(rows[0]["Symbol"], "NVDA")

    def test_buy_blurb_softens_when_macro_is_hostile(self) -> None:
        blurb = _action_section_blurb(
            "Buy",
            {
                "rows": [
                    {
                        "evidence": {
                            "macro_hostility": {
                                "macro_state": "risk_off",
                            }
                        }
                    }
                ]
            },
        )
        self.assertEqual(blurb, "Bullish setups, macro not confirmed.")

    def test_buy_blurb_stays_default_when_macro_is_supportive(self) -> None:
        blurb = _action_section_blurb(
            "Buy",
            {
                "rows": [
                    {
                        "evidence": {
                            "macro_hostility": {
                                "macro_state": "risk_on_confirmed",
                            }
                        }
                    }
                ]
            },
        )
        self.assertEqual(blurb, "Best bullish setups.")


if __name__ == "__main__":
    unittest.main()
