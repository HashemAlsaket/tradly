from __future__ import annotations

from datetime import datetime, timezone
import unittest

from tradly.analytics.recommendation_scorecard import build_scorecard_rows, summarize_scorecard


class RecommendationScorecardTests(unittest.TestCase):
    def test_scores_bullish_and_bearish_recommendations(self) -> None:
        recommendation_rows = [
            {
                "scope_id": "NVDA",
                "recommended_action": "Buy",
                "recommended_horizon": "1to3d",
                "recommendation_class": "long",
                "regime_alignment": "contrarian",
                "confidence_score": 75,
                "as_of_utc": "2026-03-10T20:00:00+00:00",
            },
            {
                "scope_id": "QQQ",
                "recommended_action": "Sell/Trim",
                "recommended_horizon": "1to3d",
                "recommendation_class": "short",
                "regime_alignment": "aligned",
                "confidence_score": 68,
                "as_of_utc": "2026-03-10T20:00:00+00:00",
            },
        ]
        bars_by_symbol = {
            "NVDA": [
                {"ts_utc": datetime(2026, 3, 11, 4, 0, tzinfo=timezone.utc), "close": 100.0},
                {"ts_utc": datetime(2026, 3, 12, 4, 0, tzinfo=timezone.utc), "close": 105.0},
                {"ts_utc": datetime(2026, 3, 13, 4, 0, tzinfo=timezone.utc), "close": 110.0},
            ],
            "QQQ": [
                {"ts_utc": datetime(2026, 3, 11, 4, 0, tzinfo=timezone.utc), "close": 200.0},
                {"ts_utc": datetime(2026, 3, 12, 4, 0, tzinfo=timezone.utc), "close": 190.0},
                {"ts_utc": datetime(2026, 3, 13, 4, 0, tzinfo=timezone.utc), "close": 180.0},
            ],
        }

        rows = build_scorecard_rows(recommendation_rows=recommendation_rows, bars_by_symbol=bars_by_symbol)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["evaluation_status"], "scored")
        self.assertEqual(rows[0]["outcome_label"], "correct")
        self.assertAlmostEqual(rows[0]["realized_return_pct"], 10.0)
        self.assertAlmostEqual(rows[0]["directional_return_pct"], 10.0)

        self.assertEqual(rows[1]["evaluation_status"], "scored")
        self.assertEqual(rows[1]["outcome_label"], "correct")
        self.assertAlmostEqual(rows[1]["realized_return_pct"], -10.0)
        self.assertAlmostEqual(rows[1]["directional_return_pct"], 10.0)

        summary = summarize_scorecard(rows)
        self.assertEqual(summary["correct_count"], 2)
        self.assertEqual(summary["pending_count"], 0)
        self.assertEqual(summary["scored_count"], 2)
        self.assertAlmostEqual(summary["hit_rate"], 1.0)

    def test_marks_rows_pending_when_exit_bar_not_available(self) -> None:
        recommendation_rows = [
            {
                "scope_id": "NVDA",
                "recommended_action": "Buy",
                "recommended_horizon": "1to2w",
                "recommendation_class": "long",
                "regime_alignment": "contrarian",
                "confidence_score": 75,
                "as_of_utc": "2026-03-10T20:00:00+00:00",
            }
        ]
        bars_by_symbol = {
            "NVDA": [
                {"ts_utc": datetime(2026, 3, 11, 4, 0, tzinfo=timezone.utc), "close": 100.0},
                {"ts_utc": datetime(2026, 3, 12, 4, 0, tzinfo=timezone.utc), "close": 105.0},
            ]
        }

        rows = build_scorecard_rows(recommendation_rows=recommendation_rows, bars_by_symbol=bars_by_symbol)

        self.assertEqual(rows[0]["evaluation_status"], "pending")
        self.assertEqual(rows[0]["pending_reason"], "waiting_for_exit_bar")
        self.assertIsNone(rows[0]["directional_return_pct"])

    def test_preserves_unsupported_rows_as_not_scored(self) -> None:
        recommendation_rows = [
            {
                "scope_id": "NVDA",
                "recommended_action": "Buy",
                "recommended_horizon": "intraday",
                "recommendation_class": "long",
                "regime_alignment": "contrarian",
                "confidence_score": 75,
                "as_of_utc": "2026-03-10T20:00:00+00:00",
            },
            {
                "scope_id": "QQQ",
                "recommended_action": "Sell/Trim",
                "recommended_horizon": "1to3d",
                "recommendation_class": "short",
                "regime_alignment": "aligned",
                "confidence_score": 68,
            },
        ]

        rows = build_scorecard_rows(recommendation_rows=recommendation_rows, bars_by_symbol={})

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["evaluation_status"], "not_scored")
        self.assertEqual(rows[0]["not_scored_reason"], "unsupported_horizon")
        self.assertEqual(rows[1]["evaluation_status"], "not_scored")
        self.assertEqual(rows[1]["not_scored_reason"], "missing_as_of_utc")

        summary = summarize_scorecard(rows)
        self.assertEqual(summary["total_recommendations"], 2)
        self.assertEqual(summary["not_scored_count"], 2)


if __name__ == "__main__":
    unittest.main()
