from __future__ import annotations

import unittest

from dashboard.app import _action_section_blurb, _decision_rows, _latest_cycle_failed, _market_status_copy, _market_tape_caution, _render_action_board


class DashboardDecisionTests(unittest.TestCase):
    def test_latest_cycle_failed_true_when_snapshot_failed(self) -> None:
        self.assertTrue(
            _latest_cycle_failed(
                {"overall_status": "FAIL"},
                {"status": "PASS", "postflight_status": "PASS"},
            )
        )

    def test_latest_cycle_failed_true_when_cycle_failed(self) -> None:
        self.assertTrue(
            _latest_cycle_failed(
                {"overall_status": "PASS"},
                {"status": "FAIL", "postflight_status": "FAIL"},
            )
        )

    def test_latest_cycle_failed_false_for_clean_pass(self) -> None:
        self.assertFalse(
            _latest_cycle_failed(
                {"overall_status": "PASS"},
                {"status": "PASS", "postflight_status": "PASS"},
            )
        )

    def test_latest_cycle_failed_false_when_snapshot_is_newer_than_failed_run(self) -> None:
        self.assertFalse(
            _latest_cycle_failed(
                {"overall_status": "PASS", "written_at_utc": "2026-03-19T04:15:42+00:00"},
                {"status": "FAIL", "postflight_status": "FAIL", "started_at_utc": "2026-03-19T04:11:14+00:00"},
            )
        )

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
                        "display_confidence_score": 66,
                        "primary_reason_code": "market_context_headwind",
                        "execution_ready": True,
                        "recommendation_class": "contrarian_long",
                        "review_disposition": "review_required",
                        "review_bucket": "contrarian_rebound",
                        "sector": "Technology",
                        "event_active": True,
                        "event_reaction_state": "beat_but_rejected",
                        "market_stress_level": "high",
                    }
                ]
            }
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Symbol"], "NVDA")
        self.assertEqual(rows[0]["Action"], "Buy")
        self.assertEqual(rows[0]["Horizon"], "1to2w")
        self.assertEqual(rows[0]["Confidence"], 66)
        self.assertEqual(rows[0]["RawConfidence"], 70)
        self.assertEqual(rows[0]["ReviewDisposition"], "review_required")
        self.assertEqual(rows[0]["Sector"], "Technology")
        self.assertEqual(rows[0]["ReviewState"], "Contrarian Rebound")
        self.assertTrue(rows[0]["EventActive"])
        self.assertEqual(rows[0]["EventReaction"], "beat_but_rejected")

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

    def test_decision_rows_humanize_watch_bucket(self) -> None:
        rows = _decision_rows(
            {
                "rows": [
                    {
                        "scope_id": "MU",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to2w",
                        "confidence_score": 70,
                        "primary_reason_code": "market_context_headwind",
                        "execution_ready": True,
                        "recommendation_class": "aligned_long",
                        "review_disposition": "watch",
                        "review_bucket": "watch_event_damaged",
                        "review_reason_code": "event_buy_capped_to_watch",
                    }
                ]
            }
        )
        self.assertEqual(rows[0]["ReviewState"], "Watch - Event Damaged")
        self.assertEqual(rows[0]["ReviewReasonText"], "event damaged; wait for confirmation")

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
                        "signal_direction": "bearish",
                        "confidence_score": 73,
                        "evidence": {
                            "macro_hostility": {
                                "macro_state": "risk_off",
                            }
                        },
                    }
                ]
            },
        )
        self.assertEqual(blurb, "Only the strongest survivors. Hostile tape, stay selective.")

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

    def test_market_tape_caution_uses_hostile_tape_when_bearish_stress_codes_present(self) -> None:
        caution = _market_tape_caution(
            {
                "rows": [
                    {
                        "signal_direction": "bearish",
                        "confidence_score": 73,
                        "why_code": ["vix_elevated", "macro_rates_pressure"],
                    }
                ]
            }
        )
        self.assertEqual(caution, "Hostile tape")

    def test_market_status_copy_labels_overnight(self) -> None:
        value, note = _market_status_copy(
            {"market_session_state": "overnight"},
            {"last_cash_session_date": "2026-03-18"},
            __import__("datetime").datetime(2026, 3, 19, 4, 15, 0, tzinfo=__import__("datetime").timezone.utc),
        )
        self.assertEqual(value, "Overnight")
        self.assertIn("Last cash session", note)


if __name__ == "__main__":
    unittest.main()
