from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from tradly.pipeline.ingest_market_bars import BACKFILL_DATA_STATUS, PROVIDER_SOURCE, _upsert_market_bars
from tradly.pipeline.ingest_market_bars_1m import (
    WATERMARK_SOURCE_NAME,
    _build_minute_agg_url,
    _ensure_pipeline_watermarks_table,
    _filter_rows_newer_than_watermark,
    _normalize_minute_bar_row,
    _request_window_from_watermark,
    _upsert_1m_watermarks,
)


class TestIngestMarketBars1M(unittest.TestCase):
    def test_build_minute_agg_url_uses_massive_host_and_minute_params(self) -> None:
        url = _build_minute_agg_url("AAPL", "secret", "2026-02-15", "2026-03-16")
        self.assertIn("https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/2026-02-15/2026-03-16", url)
        self.assertIn("adjusted=true", url)
        self.assertIn("sort=asc", url)
        self.assertIn("limit=50000", url)
        self.assertIn("apiKey=secret", url)

    def test_normalize_minute_bar_row_maps_values_to_market_bars(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc)
        row = _normalize_minute_bar_row(
            symbol="AAPL",
            bar={"t": 1773633660000, "o": 200.0, "h": 200.5, "l": 199.8, "c": 200.2, "v": 0, "vw": 200.1},
            ingested_at=ingested_at,
        )
        self.assertEqual(row[0], "AAPL")
        self.assertEqual(row[1], "1m")
        self.assertEqual(row[7], 200.2)
        self.assertEqual(row[8], 0.0)
        self.assertEqual(row[10], BACKFILL_DATA_STATUS)
        self.assertEqual(row[11], PROVIDER_SOURCE)

    def test_normalize_minute_bar_row_rejects_malformed_ohlc(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc)
        with self.assertRaisesRegex(RuntimeError, "AAPL:malformed_ohlc"):
            _normalize_minute_bar_row(
                symbol="AAPL",
                bar={"t": 1773633660000, "o": 201.0, "h": 200.5, "l": 199.8, "c": 200.2, "v": 10, "vw": 200.1},
                ingested_at=ingested_at,
            )

    def test_upsert_market_bars_accepts_minute_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE market_bars (
                      symbol TEXT NOT NULL,
                      timeframe TEXT NOT NULL,
                      ts_utc TIMESTAMP NOT NULL,
                      as_of_utc TIMESTAMP NOT NULL,
                      open DOUBLE,
                      high DOUBLE,
                      low DOUBLE,
                      close DOUBLE,
                      volume DOUBLE,
                      vwap DOUBLE,
                      data_status TEXT,
                      source TEXT NOT NULL,
                      correction_seq INTEGER NOT NULL DEFAULT 0,
                      ingested_at_utc TIMESTAMP NOT NULL,
                      updated_at_utc TIMESTAMP NOT NULL,
                      PRIMARY KEY (symbol, timeframe, ts_utc, correction_seq)
                    )
                    """
                )
            finally:
                conn.close()

            _upsert_market_bars(
                db_path,
                [
                    (
                        "AAPL",
                        "1m",
                        datetime(2026, 3, 13, 14, 31),
                        datetime(2026, 3, 13, 14, 31),
                        201.0,
                        201.2,
                        200.9,
                        201.1,
                        1250.0,
                        201.05,
                        BACKFILL_DATA_STATUS,
                        PROVIDER_SOURCE,
                        0,
                        datetime(2026, 3, 16, 4, 0),
                        datetime(2026, 3, 16, 4, 0),
                    )
                ],
            )

            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                row = conn.execute(
                    """
                    SELECT symbol, timeframe, close, volume, source
                    FROM market_bars
                    WHERE symbol='AAPL' AND timeframe='1m'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row, ("AAPL", "1m", 201.1, 1250.0, "massive"))

    def test_request_window_from_watermark_uses_guard_day(self) -> None:
        start_date, end_date = _request_window_from_watermark(
            default_start_date="2026-02-15",
            default_end_date="2026-03-16",
            watermark_ts_utc=datetime(2026, 3, 16, 15, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(start_date, "2026-03-15")
        self.assertEqual(end_date, "2026-03-16")

    def test_filter_rows_newer_than_watermark_discards_old_rows(self) -> None:
        watermark = datetime(2026, 3, 16, 14, 31, tzinfo=timezone.utc)
        rows = [
            ("AAPL", "1m", datetime(2026, 3, 16, 14, 30, tzinfo=timezone.utc)),
            ("AAPL", "1m", datetime(2026, 3, 16, 14, 31, tzinfo=timezone.utc)),
            ("AAPL", "1m", datetime(2026, 3, 16, 14, 32, tzinfo=timezone.utc)),
        ]
        filtered = _filter_rows_newer_than_watermark(rows, watermark)
        self.assertEqual([row[2] for row in filtered], [datetime(2026, 3, 16, 14, 32, tzinfo=timezone.utc)])

    def test_upsert_1m_watermarks_persists_symbol_progress(self) -> None:
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
            finally:
                conn.close()

            _upsert_1m_watermarks(
                db_path,
                {"AAPL": datetime(2026, 3, 16, 14, 32, tzinfo=timezone.utc)},
                datetime(2026, 3, 16, 14, 33, tzinfo=timezone.utc),
            )

            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                row = conn.execute(
                    "SELECT source_name, scope_key, watermark_ts_utc FROM pipeline_watermarks"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], WATERMARK_SOURCE_NAME)
            self.assertEqual(row[1], "AAPL")

    def test_ensure_pipeline_watermarks_table_bootstraps_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                _ensure_pipeline_watermarks_table(conn)
                exists = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = 'pipeline_watermarks'
                    """
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(exists, 1)


if __name__ == "__main__":
    unittest.main()
