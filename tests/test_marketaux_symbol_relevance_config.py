from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from tradly.pipeline.ingest_news_budgeted import _min_symbol_relevance as ingest_min_symbol_relevance
from tradly.pipeline.seed_news_marketaux import _min_symbol_relevance as seed_min_symbol_relevance


class MarketauxSymbolRelevanceConfigTests(unittest.TestCase):
    def test_defaults_match_between_ingest_and_seed(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(ingest_min_symbol_relevance(), 15.0)
            self.assertEqual(seed_min_symbol_relevance(), 15.0)

    def test_env_override_applies_to_both_paths(self) -> None:
        with patch.dict(os.environ, {"TRADLY_MARKETAUX_MIN_SYMBOL_RELEVANCE": "22.5"}, clear=False):
            self.assertEqual(ingest_min_symbol_relevance(), 22.5)
            self.assertEqual(seed_min_symbol_relevance(), 22.5)


if __name__ == "__main__":
    unittest.main()
