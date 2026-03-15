from __future__ import annotations

import unittest

from pathlib import Path

from tradly.pipeline.run_recommendation_scorecard_history import _aggregate_run_summaries, _is_history_compatible


class RecommendationScorecardHistoryTests(unittest.TestCase):
    def test_aggregate_run_summaries_rolls_up_counts(self) -> None:
        payloads = [
            (
                None,
                {
                    "summary": {
                        "total_recommendations": 10,
                        "pending_count": 2,
                        "scored_count": 8,
                        "not_scored_count": 0,
                        "correct_count": 5,
                        "incorrect_count": 2,
                        "flat_count": 1,
                    },
                    "rows": [
                        {"directional_return_pct": 2.0},
                        {"directional_return_pct": -1.0},
                    ],
                },
            ),
            (
                None,
                {
                    "summary": {
                        "total_recommendations": 4,
                        "pending_count": 4,
                        "scored_count": 0,
                        "not_scored_count": 0,
                        "correct_count": 0,
                        "incorrect_count": 0,
                        "flat_count": 0,
                    },
                    "rows": [],
                },
            ),
        ]
        result = _aggregate_run_summaries(payloads)
        summary = result["summary"]
        self.assertEqual(summary["run_count"], 2)
        self.assertEqual(summary["total_recommendations"], 14)
        self.assertEqual(summary["scored_count"], 8)
        self.assertEqual(summary["pending_count"], 6)
        self.assertAlmostEqual(summary["average_directional_return_pct"], 0.5)
        self.assertAlmostEqual(summary["hit_rate"], 0.625)
        self.assertIn("unknown", result["by_review_bucket"])
        self.assertEqual(result["by_review_bucket"]["unknown"]["total_recommendations"], 2)

    def test_history_compatible_requires_review_cohort_and_review_fields(self) -> None:
        good_payload = {
            "cohort_model_id": "recommendation_review_v1",
            "cohort_run_timestamp_utc": "2026-03-15T20:00:00+00:00",
            "rows": [
                {
                    "review_disposition": "promote",
                    "review_bucket": "top_shorts",
                }
            ],
        }
        bad_payload = {
            "cohort_model_id": "recommendation_review_v1",
            "rows": [{}],
        }
        self.assertTrue(_is_history_compatible(good_payload))
        self.assertFalse(_is_history_compatible(bad_payload))


if __name__ == "__main__":
    unittest.main()
