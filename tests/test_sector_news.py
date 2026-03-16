from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.sector_news import SectorNewsItem, build_sector_news_rows


class SectorNewsTests(unittest.TestCase):
    def test_sector_specific_news_drives_near_term_row(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Technology": ["AAPL", "MSFT", "NVDA"]},
            interpretations_by_sector={
                "Technology": [
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="1",
                        published_at_utc=now_utc - timedelta(hours=4),
                        interpreted_at_utc=now_utc - timedelta(hours=3),
                        bucket="sector",
                        impact_scope="technology",
                        impact_direction="bullish",
                        impact_horizon="1to3d",
                        confidence_label="high",
                        relevance_symbols=("NVDA", "MSFT"),
                        thesis_tags=("ai",),
                        market_impact_note="Positive sector catalyst.",
                    )
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "near_term")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertEqual(row["signal_direction"], "bullish")

    def test_sector_row_can_use_symbol_bucket_via_member_linkage(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Financial Services": ["JPM", "BAC"]},
            interpretations_by_sector={
                "Financial Services": [
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="2",
                        published_at_utc=now_utc - timedelta(days=1),
                        interpreted_at_utc=now_utc - timedelta(hours=20),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bearish",
                        impact_horizon="1to2w",
                        confidence_label="medium",
                        relevance_symbols=("JPM",),
                        thesis_tags=("credit",),
                        market_impact_note="Bank-specific caution with sector relevance.",
                    )
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "swing_term")
        self.assertEqual(row["horizon_primary"], "1to2w")
        self.assertEqual(row["signal_direction"], "bearish")
        self.assertLessEqual(row["confidence_score"], 75)

    def test_position_term_sector_row_can_emit_2to6w(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Healthcare": ["LLY", "UNH"]},
            interpretations_by_sector={
                "Healthcare": [
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="2b",
                        published_at_utc=now_utc - timedelta(days=3),
                        interpreted_at_utc=now_utc - timedelta(hours=60),
                        bucket="sector",
                        impact_scope="healthcare",
                        impact_direction="bullish",
                        impact_horizon="2to6w",
                        confidence_label="high",
                        relevance_symbols=("LLY",),
                        thesis_tags=("drug",),
                        market_impact_note="Durable healthcare catalyst one.",
                    ),
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="2c",
                        published_at_utc=now_utc - timedelta(days=4),
                        interpreted_at_utc=now_utc - timedelta(hours=90),
                        bucket="sector",
                        impact_scope="healthcare",
                        impact_direction="bullish",
                        impact_horizon="2to6w",
                        confidence_label="medium",
                        relevance_symbols=("UNH",),
                        thesis_tags=("insurer",),
                        market_impact_note="Durable healthcare catalyst two.",
                    ),
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "position_term")
        self.assertEqual(row["horizon_primary"], "2to6w")
        self.assertIn("1to2w", row["horizon_secondary"])
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreaterEqual(row["confidence_score"], 60)

    def test_lane_diagnostics_are_canonical_top_level_only(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Healthcare": ["LLY", "UNH"]},
            interpretations_by_sector={
                "Healthcare": [
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="3",
                        published_at_utc=now_utc - timedelta(hours=6),
                        interpreted_at_utc=now_utc - timedelta(hours=5),
                        bucket="sector",
                        impact_scope="healthcare",
                        impact_direction="bullish",
                        impact_horizon="1to2w",
                        confidence_label="high",
                        relevance_symbols=("LLY",),
                        thesis_tags=("drug",),
                        market_impact_note="Healthcare catalyst.",
                    )
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertIn("lane_diagnostics", row)
        self.assertNotIn("lane_diagnostics", row["diagnostics"])
        self.assertNotIn("lane_diagnostics", row["evidence"])

    def test_two_article_swing_term_confidence_is_soft_capped(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Communication Services": ["GOOGL", "META", "NFLX"]},
            interpretations_by_sector={
                "Communication Services": [
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="4",
                        published_at_utc=now_utc - timedelta(hours=8),
                        interpreted_at_utc=now_utc - timedelta(hours=7),
                        bucket="sector",
                        impact_scope="communication_services",
                        impact_direction="bullish",
                        impact_horizon="1to2w",
                        confidence_label="high",
                        relevance_symbols=("META",),
                        thesis_tags=("ads",),
                        market_impact_note="Positive sector catalyst one.",
                    ),
                    SectorNewsItem(
                        provider="marketaux",
                        provider_news_id="5",
                        published_at_utc=now_utc - timedelta(hours=10),
                        interpreted_at_utc=now_utc - timedelta(hours=9),
                        bucket="sector",
                        impact_scope="communication_services",
                        impact_direction="bullish",
                        impact_horizon="1to2w",
                        confidence_label="high",
                        relevance_symbols=("GOOGL",),
                        thesis_tags=("streaming",),
                        market_impact_note="Positive sector catalyst two.",
                    ),
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertLessEqual(row["confidence_score"], 82)
        self.assertIn("limited_sector_catalyst_breadth", row["why_code"])

    def test_sector_with_no_recent_news_is_insufficient(self) -> None:
        now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
        rows = build_sector_news_rows(
            sector_members={"Utilities": ["NEE", "DUK"]},
            interpretations_by_sector={},
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["coverage_state"], "insufficient_evidence")
        self.assertEqual(row["confidence_score"], 20)


if __name__ == "__main__":
    unittest.main()
