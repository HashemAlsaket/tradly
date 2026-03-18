from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from tradly.services.news_bucket_health import load_news_bucket_health, summarize_news_bucket_health


class NewsBucketHealthTests(unittest.TestCase):
    def test_required_bucket_without_recent_success_is_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE pipeline_watermarks (
                      source_name TEXT NOT NULL,
                      scope_key TEXT NOT NULL,
                      watermark_ts_utc TIMESTAMP,
                      watermark_meta_json TEXT,
                      updated_at_utc TIMESTAMP NOT NULL,
                      PRIMARY KEY (source_name, scope_key)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE news_pull_usage (
                      usage_id TEXT PRIMARY KEY,
                      provider TEXT NOT NULL,
                      bucket TEXT NOT NULL,
                      symbols_csv TEXT NOT NULL,
                      request_count INTEGER NOT NULL,
                      request_date_utc DATE NOT NULL,
                      response_status TEXT NOT NULL,
                      detail TEXT,
                      new_events_upserted INTEGER NOT NULL DEFAULT 0,
                      new_symbol_links_upserted INTEGER NOT NULL DEFAULT 0,
                      created_at_utc TIMESTAMP NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO news_pull_usage VALUES
                    ('u1', 'marketaux', 'core_semis', 'NVDA,AMD', 1, DATE '2026-03-17', 'http_error', '', 0, 0, TIMESTAMP '2026-03-17 16:50:00')
                    """
                )
                rows = load_news_bucket_health(
                    conn,
                    request_date_local=date(2026, 3, 17),
                    now_utc=datetime(2026, 3, 17, 16, 53, tzinfo=timezone.utc),
                    max_age_sec=2700,
                )
            finally:
                conn.close()

        required_failures, optional_warnings, by_bucket = summarize_news_bucket_health(rows)
        self.assertIn("core_semis", required_failures)
        self.assertEqual(optional_warnings, ["asia_semis", "asia_macro"])
        self.assertEqual(by_bucket["core_semis"]["status"], "stale")

    def test_recent_success_with_no_new_events_is_still_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE pipeline_watermarks (
                      source_name TEXT NOT NULL,
                      scope_key TEXT NOT NULL,
                      watermark_ts_utc TIMESTAMP,
                      watermark_meta_json TEXT,
                      updated_at_utc TIMESTAMP NOT NULL,
                      PRIMARY KEY (source_name, scope_key)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE news_pull_usage (
                      usage_id TEXT PRIMARY KEY,
                      provider TEXT NOT NULL,
                      bucket TEXT NOT NULL,
                      symbols_csv TEXT NOT NULL,
                      request_count INTEGER NOT NULL,
                      request_date_utc DATE NOT NULL,
                      response_status TEXT NOT NULL,
                      detail TEXT,
                      new_events_upserted INTEGER NOT NULL DEFAULT 0,
                      new_symbol_links_upserted INTEGER NOT NULL DEFAULT 0,
                      created_at_utc TIMESTAMP NOT NULL
                    )
                    """
                )
                for bucket in ("core_semis", "us_macro", "sector_context", "event_reserve"):
                    conn.execute(
                        """
                        INSERT INTO news_pull_usage VALUES
                        (?, 'marketaux', ?, 'NVDA,AMD', 1, DATE '2026-03-17', 'success', '', 0, 0, TIMESTAMP '2026-03-17 16:50:00')
                        """,
                        (f"u_{bucket}", bucket),
                    )
                rows = load_news_bucket_health(
                    conn,
                    request_date_local=date(2026, 3, 17),
                    now_utc=datetime(2026, 3, 17, 16, 53, tzinfo=timezone.utc),
                    max_age_sec=2700,
                )
            finally:
                conn.close()

        required_failures, _optional_warnings, by_bucket = summarize_news_bucket_health(rows)
        self.assertEqual(required_failures, [])
        self.assertEqual(by_bucket["core_semis"]["status"], "fresh")


if __name__ == "__main__":
    unittest.main()
