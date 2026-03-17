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


if __name__ == "__main__":
    unittest.main()
