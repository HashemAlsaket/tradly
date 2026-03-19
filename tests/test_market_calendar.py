from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.services.market_calendar import market_session_state


class MarketCalendarTests(unittest.TestCase):
    def test_overnight_session_before_four_am_eastern(self) -> None:
        self.assertEqual(
            market_session_state(datetime(2026, 3, 19, 4, 15, tzinfo=timezone.utc)),
            "overnight",
        )

    def test_pre_market_session_after_four_am_eastern(self) -> None:
        self.assertEqual(
            market_session_state(datetime(2026, 3, 19, 9, 15, tzinfo=timezone.utc)),
            "pre_market",
        )
