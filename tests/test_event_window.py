from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tradly.services.event_window import load_event_windows


class EventWindowTests(unittest.TestCase):
    def _write_watchlist(self, payload: dict) -> Path:
        tmpdir = Path(tempfile.mkdtemp(prefix="tradly_event_window_"))
        path = tmpdir / "earnings_watchlist.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_load_event_windows_marks_pre_post_and_inactive(self) -> None:
        path = self._write_watchlist(
            {
                "events": [
                    {
                        "symbol": "MU",
                        "company": "Micron Technology",
                        "report_date_et": "2026-03-18",
                        "call_time_et": "16:30",
                        "notes": "Q2 earnings",
                        "source_note": "manual",
                    },
                    {
                        "symbol": "NVDA",
                        "company": "NVIDIA",
                        "report_date_et": "2026-03-25",
                        "call_time_et": "16:20",
                        "notes": "Pending",
                        "source_note": "manual",
                    },
                ]
            }
        )

        pre_event = load_event_windows(
            watchlist_path=path,
            now_utc=datetime(2026, 3, 18, 19, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(pre_event["MU"].event_phase, "pre_event")
        self.assertTrue(pre_event["MU"].event_active)
        self.assertEqual(pre_event["NVDA"].event_phase, "inactive")

        post_event = load_event_windows(
            watchlist_path=path,
            now_utc=datetime(2026, 3, 18, 22, 45, tzinfo=timezone.utc),
        )
        self.assertEqual(post_event["MU"].event_phase, "post_event")

        digestion = load_event_windows(
            watchlist_path=path,
            now_utc=datetime(2026, 3, 19, 20, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(digestion["MU"].event_phase, "digestion_window")

    def test_load_event_windows_handles_missing_schedule_as_inactive(self) -> None:
        path = self._write_watchlist(
            {
                "events": [
                    {
                        "symbol": "SNDK",
                        "company": "SanDisk",
                        "report_date_et": None,
                        "call_time_et": None,
                        "notes": "Pending",
                        "source_note": "manual",
                    }
                ]
            }
        )
        windows = load_event_windows(
            watchlist_path=path,
            now_utc=datetime(2026, 3, 18, 20, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(windows["SNDK"].event_phase, "inactive")
        self.assertFalse(windows["SNDK"].event_active)


if __name__ == "__main__":
    unittest.main()
