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

    def test_industrials_sector_proxy_is_checked_with_xli(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled", "portfolio_eligible": True})
        checks = _build_checks(
            symbol="CAT",
            sector_proxy="XLI",
            flags=flags,
            instrument_symbols={"CAT"},
            market_data_symbols={"CAT", "XLI"},
            model_symbols={"CAT"},
            direct_news_symbols=set(),
            portfolio_symbols={"CAT"},
            daily_dates={("CAT", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"CAT"},
            watermark_symbols=set(),
            snapshot_symbols={"CAT"},
            symbol_movement_symbols={"CAT"},
            recommendation_symbols={"CAT"},
            symbol_news_symbols=set(),
            portfolio_policy_symbols={"CAT"},
        )

        self.assertEqual(checks["sector_proxy_present"]["status"], "pass")

    def test_technology_sector_proxy_is_checked_with_xlk(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled_with_direct_news", "portfolio_eligible": True})
        checks = _build_checks(
            symbol="MSFT",
            sector_proxy="XLK",
            flags=flags,
            instrument_symbols={"MSFT"},
            market_data_symbols={"MSFT", "XLK"},
            model_symbols={"MSFT"},
            direct_news_symbols={"MSFT"},
            portfolio_symbols={"MSFT"},
            daily_dates={("MSFT", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"MSFT"},
            watermark_symbols=set(),
            snapshot_symbols={"MSFT"},
            symbol_movement_symbols={"MSFT"},
            recommendation_symbols={"MSFT"},
            symbol_news_symbols={"MSFT"},
            portfolio_policy_symbols={"MSFT"},
        )

        self.assertEqual(checks["sector_proxy_present"]["status"], "pass")

    def test_consumer_defensive_sector_proxy_is_checked_with_xlp(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled_with_direct_news", "portfolio_eligible": True})
        checks = _build_checks(
            symbol="WMT",
            sector_proxy="XLP",
            flags=flags,
            instrument_symbols={"WMT"},
            market_data_symbols={"WMT", "XLP"},
            model_symbols={"WMT"},
            direct_news_symbols={"WMT"},
            portfolio_symbols={"WMT"},
            daily_dates={("WMT", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"WMT"},
            watermark_symbols=set(),
            snapshot_symbols={"WMT"},
            symbol_movement_symbols={"WMT"},
            recommendation_symbols={"WMT"},
            symbol_news_symbols={"WMT"},
            portfolio_policy_symbols={"WMT"},
        )

        self.assertEqual(checks["sector_proxy_present"]["status"], "pass")

    def test_communication_services_sector_proxy_is_checked_with_xlc(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled_with_direct_news", "portfolio_eligible": True})
        checks = _build_checks(
            symbol="META",
            sector_proxy="XLC",
            flags=flags,
            instrument_symbols={"META"},
            market_data_symbols={"META", "XLC"},
            model_symbols={"META"},
            direct_news_symbols={"META"},
            portfolio_symbols={"META"},
            daily_dates={("META", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"META"},
            watermark_symbols=set(),
            snapshot_symbols={"META"},
            symbol_movement_symbols={"META"},
            recommendation_symbols={"META"},
            symbol_news_symbols={"META"},
            portfolio_policy_symbols={"META"},
        )

        self.assertEqual(checks["sector_proxy_present"]["status"], "pass")

    def test_energy_sector_proxy_is_checked_with_xle(self) -> None:
        flags = _expected_flags({"onboarding_stage": "modeled_with_direct_news", "portfolio_eligible": True})
        checks = _build_checks(
            symbol="CVX",
            sector_proxy="XLE",
            flags=flags,
            instrument_symbols={"CVX"},
            market_data_symbols={"CVX", "XLE"},
            model_symbols={"CVX"},
            direct_news_symbols={"CVX"},
            portfolio_symbols={"CVX"},
            daily_dates={("CVX", "2026-03-18")},
            latest_daily_market_date="2026-03-18",
            intraday_symbols={"CVX"},
            watermark_symbols=set(),
            snapshot_symbols={"CVX"},
            symbol_movement_symbols={"CVX"},
            recommendation_symbols={"CVX"},
            symbol_news_symbols={"CVX"},
            portfolio_policy_symbols={"CVX"},
        )

        self.assertEqual(checks["sector_proxy_present"]["status"], "pass")


if __name__ == "__main__":
    unittest.main()
