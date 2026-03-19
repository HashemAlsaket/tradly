from __future__ import annotations

import unittest

from tradly.config.model_registry import get_model_registry_entry


class ModelRegistryTests(unittest.TestCase):
    def test_live_directional_2to6w_models_are_registered(self) -> None:
        self.assertIn("2to6w", get_model_registry_entry("market_regime_v1").supported_horizons)
        self.assertIn("2to6w", get_model_registry_entry("sector_movement_v1").supported_horizons)
        self.assertIn("2to6w", get_model_registry_entry("symbol_movement_v1").supported_horizons)
        self.assertIn("2to6w", get_model_registry_entry("symbol_news_v1").supported_horizons)
        self.assertIn("2to6w", get_model_registry_entry("sector_news_v1").supported_horizons)
        self.assertIn("2to6w", get_model_registry_entry("ensemble_v1").supported_horizons)

    def test_portfolio_policy_v1_is_registered_for_supported_horizons(self) -> None:
        entry = get_model_registry_entry("portfolio_policy_v1")
        self.assertEqual(entry.scope, "symbol")
        self.assertIn("1to3d", entry.supported_horizons)
        self.assertIn("1to2w", entry.supported_horizons)
        self.assertIn("2to6w", entry.supported_horizons)

    def test_event_risk_v1_is_registered_and_required_downstream(self) -> None:
        event_entry = get_model_registry_entry("event_risk_v1")
        review_entry = get_model_registry_entry("recommendation_review_v1")
        portfolio_entry = get_model_registry_entry("portfolio_policy_v1")

        self.assertEqual(event_entry.scope, "symbol")
        self.assertIn("earnings_watchlist", event_entry.required_inputs)
        self.assertIn("recommendation_v1", review_entry.required_inputs)
        self.assertIn("event_risk_v1", review_entry.required_inputs)
        self.assertIn("event_risk_v1", portfolio_entry.required_inputs)


if __name__ == "__main__":
    unittest.main()
