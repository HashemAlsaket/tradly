from __future__ import annotations

import unittest

from tradly.ops.runtime_freshness_audit import _freshness_mode


class RuntimeFreshnessAuditTests(unittest.TestCase):
    def test_weekend_and_holiday_use_closed_calendar_mode(self) -> None:
        self.assertEqual(_freshness_mode(market_session="weekend"), "closed_calendar")
        self.assertEqual(_freshness_mode(market_session="holiday"), "closed_calendar")

    def test_market_hours_and_regular_offhours_modes(self) -> None:
        self.assertEqual(_freshness_mode(market_session="market_hours"), "market_hours")
        self.assertEqual(_freshness_mode(market_session="pre_market"), "offhours")
        self.assertEqual(_freshness_mode(market_session="after_hours"), "offhours")


if __name__ == "__main__":
    unittest.main()
