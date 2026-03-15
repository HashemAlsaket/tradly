from __future__ import annotations

import unittest

from tradly.pipeline.interpret_news_llm import _normalize_record, _validate_record


class InterpretNewsContractTests(unittest.TestCase):
    def test_accepts_sector_specific_record_with_extended_scope(self) -> None:
        ok, reason = _validate_record(
            {
                "provider": "marketaux",
                "provider_news_id": "abc123",
                "bucket": "sector",
                "impact_scope": "financial_services",
                "impact_direction": "bearish",
                "impact_horizon": "1to2w",
                "relevance_symbols": ["JPM", "BAC"],
                "thesis_tags": ["banks", "credit"],
                "market_impact_note": "Bank funding concerns could pressure financials over the next two weeks.",
                "confidence_label": "medium",
                "based_on_provided_evidence": True,
                "calculation_performed": False,
            }
        )

        self.assertTrue(ok, reason)

    def test_accepts_symbol_specific_record_with_longer_horizon(self) -> None:
        ok, reason = _validate_record(
            {
                "provider": "marketaux",
                "provider_news_id": "xyz789",
                "bucket": "symbol",
                "impact_scope": "symbol_specific",
                "impact_direction": "bullish",
                "impact_horizon": "2to6w",
                "relevance_symbols": ["NVDA"],
                "thesis_tags": ["ai_demand", "guidance"],
                "market_impact_note": "The guidance update suggests demand strength could support the stock for several weeks.",
                "confidence_label": "high",
                "based_on_provided_evidence": True,
                "calculation_performed": False,
            }
        )

        self.assertTrue(ok, reason)

    def test_rejects_unknown_scope(self) -> None:
        ok, reason = _validate_record(
            {
                "provider": "marketaux",
                "provider_news_id": "bad1",
                "bucket": "sector",
                "impact_scope": "made_up_scope",
                "impact_direction": "bullish",
                "impact_horizon": "1to3d",
                "relevance_symbols": [],
                "thesis_tags": [],
                "market_impact_note": "Invalid scope should fail validation.",
                "confidence_label": "low",
                "based_on_provided_evidence": True,
                "calculation_performed": False,
            }
        )

        self.assertFalse(ok)
        self.assertEqual(reason, "impact_scope_invalid")

    def test_normalizes_common_sector_scope_aliases_before_validation(self) -> None:
        row = _normalize_record(
            {
                "provider": "marketaux",
                "provider_news_id": "alias1",
                "bucket": "sector",
                "impact_scope": "consumer discretionary",
                "impact_direction": "bullish",
                "impact_horizon": "1to2w",
                "relevance_symbols": ["AMZN"],
                "thesis_tags": ["spending"],
                "market_impact_note": "Consumer discretionary strength could support the sector over the next two weeks.",
                "confidence_label": "medium",
                "based_on_provided_evidence": True,
                "calculation_performed": False,
            }
        )

        self.assertEqual(row["impact_scope"], "consumer_cyclical")
        ok, reason = _validate_record(row)
        self.assertTrue(ok, reason)

    def test_normalizes_symbol_specific_alias_before_validation(self) -> None:
        row = _normalize_record(
            {
                "provider": "marketaux",
                "provider_news_id": "alias2",
                "bucket": "symbol",
                "impact_scope": "symbol specific",
                "impact_direction": "bullish",
                "impact_horizon": "1to3d",
                "relevance_symbols": ["NVDA"],
                "thesis_tags": ["guidance"],
                "market_impact_note": "The update supports the stock over the next few sessions.",
                "confidence_label": "high",
                "based_on_provided_evidence": True,
                "calculation_performed": False,
            }
        )

        self.assertEqual(row["impact_scope"], "symbol_specific")
        ok, reason = _validate_record(row)
        self.assertTrue(ok, reason)


if __name__ == "__main__":
    unittest.main()
