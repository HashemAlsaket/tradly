from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.services.time_context import get_time_context


class TimeContextTests(unittest.TestCase):
    def test_time_context_returns_utc_and_local(self) -> None:
        fixed_now = datetime(2026, 3, 11, 18, 0, tzinfo=timezone.utc)
        ctx = get_time_context("America/Chicago", now_utc=fixed_now)

        self.assertEqual(ctx.now_utc, fixed_now)
        self.assertEqual(ctx.local_timezone, "America/Chicago")
        self.assertIsNotNone(ctx.now_local.tzinfo)

    def test_time_context_rejects_naive_now(self) -> None:
        with self.assertRaises(ValueError):
            get_time_context("America/Chicago", now_utc=datetime(2026, 3, 11, 18, 0))

    def test_time_context_rejects_invalid_timezone(self) -> None:
        fixed_now = datetime(2026, 3, 11, 18, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            get_time_context("Bad/Timezone", now_utc=fixed_now)


if __name__ == "__main__":
    unittest.main()
