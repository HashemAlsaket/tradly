from __future__ import annotations

import unittest

from tradly.ops.runtime_freshness_audit import (
    FreshnessCheck,
    _freshness_mode,
    _medium_horizon_thesis_usable,
)


class RuntimeFreshnessAuditTests(unittest.TestCase):
    def test_weekend_and_holiday_use_closed_calendar_mode(self) -> None:
        self.assertEqual(_freshness_mode(market_session="weekend"), "closed_calendar")
        self.assertEqual(_freshness_mode(market_session="holiday"), "closed_calendar")

    def test_market_hours_and_regular_offhours_modes(self) -> None:
        self.assertEqual(_freshness_mode(market_session="market_hours"), "market_hours")
        self.assertEqual(_freshness_mode(market_session="pre_market"), "offhours")
        self.assertEqual(_freshness_mode(market_session="after_hours"), "offhours")

    def test_medium_horizon_thesis_usable_when_core_checks_pass(self) -> None:
        checks = [
            FreshnessCheck("market_daily_bar_recency", "PASS", ""),
            FreshnessCheck("news_pull_recency", "PASS", ""),
            FreshnessCheck("news_interpretation_recency", "PASS", ""),
        ]
        self.assertTrue(
            _medium_horizon_thesis_usable(
                market_bar_status="current_for_calendar",
                checks=checks,
                pending_uninterpreted_24h=0,
            )
        )

    def test_medium_horizon_thesis_not_usable_when_recent_backlog_remains(self) -> None:
        checks = [
            FreshnessCheck("market_daily_bar_recency", "PASS", ""),
            FreshnessCheck("news_pull_recency", "PASS", ""),
            FreshnessCheck("news_interpretation_recency", "PASS", ""),
        ]
        self.assertFalse(
            _medium_horizon_thesis_usable(
                market_bar_status="current_for_calendar",
                checks=checks,
                pending_uninterpreted_24h=1,
            )
        )

    def test_medium_horizon_thesis_not_usable_when_news_is_not_fresh(self) -> None:
        checks = [
            FreshnessCheck("market_daily_bar_recency", "PASS", ""),
            FreshnessCheck("news_pull_recency", "FAIL", ""),
            FreshnessCheck("news_interpretation_recency", "PASS", ""),
        ]
        self.assertFalse(
            _medium_horizon_thesis_usable(
                market_bar_status="current_for_calendar",
                checks=checks,
                pending_uninterpreted_24h=0,
            )
        )


if __name__ == "__main__":
    unittest.main()
