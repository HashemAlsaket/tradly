from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.ops.runtime_freshness_audit import (
    FreshnessCheck,
    _intraday_source_status,
    _medium_horizon_thesis_usable,
)
from tradly.services.session_freshness_policy import (
    freshness_mode_for_policy,
    freshness_policy_for_session,
)


class RuntimeFreshnessAuditTests(unittest.TestCase):
    def test_weekend_and_holiday_use_closed_calendar_mode(self) -> None:
        self.assertEqual(freshness_policy_for_session("weekend"), "closed_calendar_relaxed")
        self.assertEqual(freshness_policy_for_session("holiday"), "closed_calendar_relaxed")
        self.assertEqual(freshness_mode_for_policy("closed_calendar_relaxed"), "closed_calendar")

    def test_market_hours_and_regular_offhours_modes(self) -> None:
        self.assertEqual(freshness_policy_for_session("market_hours"), "market_hours_strict")
        self.assertEqual(freshness_policy_for_session("pre_market"), "premarket_tradable")
        self.assertEqual(freshness_policy_for_session("after_hours"), "after_hours_relaxed")
        self.assertEqual(freshness_mode_for_policy("market_hours_strict"), "market_hours")
        self.assertEqual(freshness_mode_for_policy("premarket_tradable"), "offhours")
        self.assertEqual(freshness_mode_for_policy("after_hours_relaxed"), "offhours")

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

    def test_intraday_source_not_required_on_closed_calendar(self) -> None:
        status, age = _intraday_source_status(
            latest_ts=None,
            now_utc=datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc),
            freshness_policy="closed_calendar_relaxed",
            max_age_sec=1200,
        )
        self.assertEqual((status, age), ("not_required", None))

    def test_intraday_source_stale_when_active_session_and_old(self) -> None:
        status, age = _intraday_source_status(
            latest_ts=datetime(2026, 3, 16, 13, 0),
            now_utc=datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc),
            freshness_policy="market_hours_strict",
            max_age_sec=1200,
        )
        self.assertEqual(status, "stale")
        self.assertEqual(age, 7200)

    def test_intraday_source_stale_but_tracked_in_after_hours(self) -> None:
        status, age = _intraday_source_status(
            latest_ts=datetime(2026, 3, 16, 20, 30),
            now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
            freshness_policy="after_hours_relaxed",
            max_age_sec=1200,
        )
        self.assertEqual(status, "stale")
        self.assertEqual(age, 1800)


if __name__ == "__main__":
    unittest.main()
