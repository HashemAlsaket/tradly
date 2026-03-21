from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tradly.services.universe_registry import load_normalized_registry, normalize_registry_row


class UniverseRegistryTests(unittest.TestCase):
    def test_normalize_registry_row_derives_flags_from_stage(self) -> None:
        row = normalize_registry_row(
            {
                "symbol": "JNJ",
                "onboarding_stage": "modeled_with_direct_news",
                "active": False,
                "market_data": False,
                "model": False,
                "direct_news": False,
            }
        )
        self.assertTrue(row["active"])
        self.assertTrue(row["market_data"])
        self.assertTrue(row["model"])
        self.assertTrue(row["direct_news"])

    def test_load_normalized_registry_normalizes_all_symbol_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "universe_registry.json"
            path.write_text(
                json.dumps(
                    {
                        "symbols": [
                            {
                                "symbol": "CVS",
                                "onboarding_stage": "market_data_only",
                                "active": True,
                                "market_data": False,
                                "model": True,
                                "direct_news": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            payload = load_normalized_registry(path)

        row = payload["symbols"][0]
        self.assertFalse(row["active"])
        self.assertTrue(row["market_data"])
        self.assertFalse(row["model"])
        self.assertFalse(row["direct_news"])


if __name__ == "__main__":
    unittest.main()
