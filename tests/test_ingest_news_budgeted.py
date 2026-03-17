from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb

from tradly.pipeline.ingest_news_budgeted import (
    NEWS_WATERMARK_SOURCE,
    _effective_published_after,
    _load_news_watermarks,
    _parse_marketaux_published_at,
    _should_continue_news_pagination,
    _upsert_news_watermarks,
)


class IngestNewsBudgetedTests(unittest.TestCase):
    def test_effective_published_after_prefers_env_override(self) -> None:
        self.assertEqual(
            _effective_published_after(
                "2026-03-16T14:00:00+00:00",
                datetime(2026, 3, 16, 13, 55, 0),
            ),
            "2026-03-16T14:00:00",
        )

    def test_effective_published_after_falls_back_to_watermark(self) -> None:
        self.assertEqual(
            _effective_published_after(
                "",
                datetime(2026, 3, 16, 13, 55, 0),
            ),
            "2026-03-16T13:55:00",
        )

    def test_should_continue_pagination_only_when_page_is_newer_than_watermark(self) -> None:
        previous_watermark = datetime(2026, 3, 16, 14, 0, 0)
        newer_page = [
            {"published_at": "2026-03-16T14:10:00+00:00"},
            {"published_at": "2026-03-16T14:05:00+00:00"},
        ]
        older_page = [
            {"published_at": "2026-03-16T14:00:00+00:00"},
            {"published_at": "2026-03-16T13:59:00+00:00"},
        ]
        self.assertTrue(
            _should_continue_news_pagination(
                page_articles=newer_page,
                previous_watermark=previous_watermark,
            )
        )
        self.assertFalse(
            _should_continue_news_pagination(
                page_articles=older_page,
                previous_watermark=previous_watermark,
            )
        )

    def test_parse_marketaux_published_at_normalizes_to_db_utc(self) -> None:
        parsed = _parse_marketaux_published_at("2026-03-16T14:05:00+00:00")
        self.assertEqual(parsed, datetime(2026, 3, 16, 14, 5, 0))

    def test_upsert_and_load_news_watermarks(self) -> None:
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
                _upsert_news_watermarks(
                    conn,
                    {
                        "core_semis": datetime(2026, 3, 16, 14, 5, 0),
                        "us_macro": datetime(2026, 3, 16, 14, 6, 0),
                    },
                    datetime(2026, 3, 16, 14, 7, 0),
                )
                loaded = _load_news_watermarks(conn, ["core_semis", "us_macro"])
                rows = conn.execute(
                    """
                    SELECT source_name, scope_key
                    FROM pipeline_watermarks
                    ORDER BY scope_key
                    """
                ).fetchall()
            finally:
                conn.close()

        self.assertEqual(
            rows,
            [
                (NEWS_WATERMARK_SOURCE, "core_semis"),
                (NEWS_WATERMARK_SOURCE, "us_macro"),
            ],
        )
        self.assertEqual(loaded["core_semis"], datetime(2026, 3, 16, 14, 5, 0))
        self.assertEqual(loaded["us_macro"], datetime(2026, 3, 16, 14, 6, 0))


if __name__ == "__main__":
    unittest.main()
