from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.symbol_news import SymbolNewsItem, build_symbol_news_rows


class SymbolNewsTests(unittest.TestCase):
    def test_symbol_specific_news_drives_near_term_row(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        rows = build_symbol_news_rows(
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology", "industry": "Semiconductors"}},
            model_symbols=["NVDA"],
            interpretations_by_symbol={
                "NVDA": [
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="1",
                        published_at_utc=now_utc - timedelta(hours=6),
                        interpreted_at_utc=now_utc - timedelta(hours=5),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bullish",
                        impact_horizon="1to3d",
                        confidence_label="high",
                        relevance_symbols=("NVDA",),
                        thesis_tags=("ai_demand",),
                        market_impact_note="Positive demand update.",
                    ),
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="2",
                        published_at_utc=now_utc - timedelta(hours=10),
                        interpreted_at_utc=now_utc - timedelta(hours=9),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bullish",
                        impact_horizon="1to3d",
                        confidence_label="medium",
                        relevance_symbols=("NVDA",),
                        thesis_tags=("guidance",),
                        market_impact_note="Positive guidance.",
                    ),
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "near_term")
        self.assertEqual(row["signal_direction"], "bullish")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreater(row["confidence_score"], 60)

    def test_symbol_with_no_recent_news_is_insufficient(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        rows = build_symbol_news_rows(
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology", "industry": "Consumer Electronics"}},
            model_symbols=["AAPL"],
            interpretations_by_symbol={},
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["coverage_state"], "insufficient_evidence")
        self.assertEqual(row["signal_direction"], "neutral")
        self.assertEqual(row["confidence_score"], 20)

    def test_sector_news_can_drive_swing_term_row(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        rows = build_symbol_news_rows(
            symbol_metadata={"JPM": {"asset_type": "stock", "sector": "Financial Services", "industry": "Banks"}},
            model_symbols=["JPM"],
            interpretations_by_symbol={
                "JPM": [
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="3",
                        published_at_utc=now_utc - timedelta(days=1),
                        interpreted_at_utc=now_utc - timedelta(hours=20),
                        bucket="sector",
                        impact_scope="financial_services",
                        impact_direction="bearish",
                        impact_horizon="1to2w",
                        confidence_label="medium",
                        relevance_symbols=("JPM", "BAC"),
                        thesis_tags=("banks",),
                        market_impact_note="Financials pressured by credit concerns.",
                    ),
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="4",
                        published_at_utc=now_utc - timedelta(days=2),
                        interpreted_at_utc=now_utc - timedelta(hours=28),
                        bucket="sector",
                        impact_scope="financial_services",
                        impact_direction="bearish",
                        impact_horizon="1to2w",
                        confidence_label="high",
                        relevance_symbols=("JPM",),
                        thesis_tags=("credit",),
                        market_impact_note="Longer-dated credit headwinds for banks.",
                    ),
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="4b",
                        published_at_utc=now_utc - timedelta(days=4),
                        interpreted_at_utc=now_utc - timedelta(hours=60),
                        bucket="sector",
                        impact_scope="financial_services",
                        impact_direction="bearish",
                        impact_horizon="2to6w",
                        confidence_label="high",
                        relevance_symbols=("JPM",),
                        thesis_tags=("credit",),
                        market_impact_note="Longer-dated credit headwinds for banks.",
                    ),
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "swing_term")
        self.assertEqual(row["horizon_primary"], "1to2w")
        self.assertEqual(row["signal_direction"], "bearish")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")

    def test_position_term_news_can_drive_2to6w_row(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        rows = build_symbol_news_rows(
            symbol_metadata={"CRM": {"asset_type": "stock", "sector": "Technology", "industry": "Software"}},
            model_symbols=["CRM"],
            interpretations_by_symbol={
                "CRM": [
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="6",
                        published_at_utc=now_utc - timedelta(days=3),
                        interpreted_at_utc=now_utc - timedelta(hours=60),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bullish",
                        impact_horizon="2to6w",
                        confidence_label="high",
                        relevance_symbols=("CRM",),
                        thesis_tags=("platform",),
                        market_impact_note="Durable platform demand.",
                    ),
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="7",
                        published_at_utc=now_utc - timedelta(days=5),
                        interpreted_at_utc=now_utc - timedelta(hours=110),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bullish",
                        impact_horizon="2to6w",
                        confidence_label="medium",
                        relevance_symbols=("CRM",),
                        thesis_tags=("enterprise",),
                        market_impact_note="Broader enterprise follow-through.",
                    ),
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertEqual(row["lane_primary"], "position_term")
        self.assertEqual(row["horizon_primary"], "2to6w")
        self.assertIn("1to2w", row["horizon_secondary"])
        self.assertEqual(row["signal_direction"], "bullish")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreaterEqual(row["confidence_score"], 60)
        self.assertIn("position_term", row["lane_secondary"] + [row["lane_primary"]])

    def test_single_article_swing_term_confidence_is_capped(self) -> None:
        now_utc = datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc)
        rows = build_symbol_news_rows(
            symbol_metadata={"MU": {"asset_type": "stock", "sector": "Technology", "industry": "Semiconductors"}},
            model_symbols=["MU"],
            interpretations_by_symbol={
                "MU": [
                    SymbolNewsItem(
                        provider="marketaux",
                        provider_news_id="5",
                        published_at_utc=now_utc - timedelta(hours=6),
                        interpreted_at_utc=now_utc - timedelta(hours=5),
                        bucket="symbol",
                        impact_scope="symbol_specific",
                        impact_direction="bullish",
                        impact_horizon="1to2w",
                        confidence_label="high",
                        relevance_symbols=("MU",),
                        thesis_tags=("ai",),
                        market_impact_note="Single positive catalyst.",
                    )
                ]
            },
            now_utc=now_utc,
        )
        row = rows[0]
        self.assertLessEqual(row["confidence_score"], 75)
        self.assertIn("single_catalyst_only", row["why_code"])


if __name__ == "__main__":
    unittest.main()
