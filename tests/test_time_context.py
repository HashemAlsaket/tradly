from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone

from tradly.services.time_context import NOW_UTC_OVERRIDE_ENV, get_time_context


class TimeContextTests(unittest.TestCase):
    def test_get_time_context_uses_env_override_when_present(self) -> None:
        previous = os.environ.get(NOW_UTC_OVERRIDE_ENV)
        try:
            os.environ[NOW_UTC_OVERRIDE_ENV] = "2026-03-19T03:56:37.627610+00:00"
            ctx = get_time_context()
            self.assertEqual(
                ctx.now_utc,
                datetime(2026, 3, 19, 3, 56, 37, 627610, tzinfo=timezone.utc),
            )
        finally:
            if previous is None:
                os.environ.pop(NOW_UTC_OVERRIDE_ENV, None)
            else:
                os.environ[NOW_UTC_OVERRIDE_ENV] = previous

    def test_explicit_now_utc_beats_env_override(self) -> None:
        previous = os.environ.get(NOW_UTC_OVERRIDE_ENV)
        try:
            os.environ[NOW_UTC_OVERRIDE_ENV] = "2026-03-19T03:56:37.627610+00:00"
            explicit = datetime(2026, 3, 19, 4, 2, 11, 862918, tzinfo=timezone.utc)
            ctx = get_time_context(now_utc=explicit)
            self.assertEqual(ctx.now_utc, explicit)
        finally:
            if previous is None:
                os.environ.pop(NOW_UTC_OVERRIDE_ENV, None)
            else:
                os.environ[NOW_UTC_OVERRIDE_ENV] = previous
