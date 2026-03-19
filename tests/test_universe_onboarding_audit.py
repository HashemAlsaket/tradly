from __future__ import annotations

import unittest

from tradly.pipeline.run_universe_onboarding_audit import _build_checks, _expected_flags


class UniverseOnboardingAuditTest(unittest.TestCase):
    def test_market_data_only_checks_are_skipped_for_non_required_layers(self) -> None:
        flags = _expected_flags({"onboarding_stage": "market_data_only", "portfolio_eligible": False})
        checks = _build_checks(
            symbol="GILD",
            sector_proxy="XLV",
            flags=flags,
            instrument_symbols={"GILD"},
            market_data_symbols={"GILD", "XLV"},
            model_symbols=set(),
            direct_news_symbols=set(),
            portfolio_symbols=set(),
            daily_dates={("GILD", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"GILD"},
            watermark_symbols=set(),
            snapshot_symbols={"GILD"},
            symbol_movement_symbols=set(),
            recommendation_symbols=set(),
            symbol_news_symbols=set(),
            portfolio_policy_symbols=set(),
        )

        self.assertEqual(checks["instrument_loaded"]["status"], "pass")
        self.assertEqual(checks["in_market_data_scope"]["status"], "pass")
        self.assertEqual(checks["in_model_scope"]["status"], "skipped")
        self.assertFalse(checks["in_model_scope"]["required"])
        self.assertEqual(checks["recommendation_present"]["status"], "skipped")
        self.assertEqual(checks["in_direct_news_scope"]["status"], "skipped")
        self.assertEqual(checks["portfolio_row_present"]["status"], "skipped")

    def test_modeled_with_direct_news_requires_direct_news_checks(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled_with_direct_news", "portfolio_eligible": False})
        checks = _build_checks(
            symbol="JNJ",
            sector_proxy="XLV",
            flags=flags,
            instrument_symbols={"JNJ"},
            market_data_symbols={"JNJ", "XLV"},
            model_symbols={"JNJ"},
            direct_news_symbols=set(),
            portfolio_symbols=set(),
            daily_dates={("JNJ", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"JNJ"},
            watermark_symbols=set(),
            snapshot_symbols={"JNJ"},
            symbol_movement_symbols={"JNJ"},
            recommendation_symbols={"JNJ"},
            symbol_news_symbols=set(),
            portfolio_policy_symbols=set(),
        )

        self.assertTrue(checks["in_direct_news_scope"]["required"])
        self.assertEqual(checks["in_direct_news_scope"]["status"], "fail")
        self.assertEqual(checks["symbol_news_present"]["status"], "fail")
        self.assertEqual(checks["portfolio_row_present"]["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
