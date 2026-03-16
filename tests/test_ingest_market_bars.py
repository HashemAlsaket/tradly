from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from tradly.pipeline.ingest_market_bars import (
    BACKFILL_DATA_STATUS,
    BACKFILL_MODE_CUTOVER,
    BACKFILL_MODE_VALIDATE,
    PROVIDER_SOURCE,
    _build_daily_agg_url,
    _get_market_data_api_key,
    _get_backfill_mode,
    _normalize_daily_bar_row,
    _upsert_market_bars,
    _write_validation_artifact,
)


class TestIngestMarketBars(unittest.TestCase):
    def test_prefers_massive_api_key(self) -> None:
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "massive-key", "POLYGON_API_KEY": "legacy-key"}, clear=True):
            self.assertEqual(_get_market_data_api_key(), "massive-key")

    def test_falls_back_to_polygon_api_key(self) -> None:
        with patch.dict(os.environ, {"POLYGON_API_KEY": "legacy-key"}, clear=True):
            self.assertIsNone(_get_market_data_api_key())

    def test_backfill_mode_defaults_to_validate(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_backfill_mode(), BACKFILL_MODE_VALIDATE)

    def test_backfill_mode_accepts_cutover(self) -> None:
        with patch.dict(os.environ, {"TRADLY_MARKET_BACKFILL_MODE": "cutover"}, clear=True):
            self.assertEqual(_get_backfill_mode(), BACKFILL_MODE_CUTOVER)

    def test_build_daily_agg_url_uses_massive_host_and_daily_params(self) -> None:
        url = _build_daily_agg_url("AAPL", "secret", "2026-01-01", "2026-03-16")
        self.assertIn("https://api.massive.com/v2/aggs/ticker/AAPL/range/1/day/2026-01-01/2026-03-16", url)
        self.assertIn("adjusted=true", url)
        self.assertIn("sort=asc", url)
        self.assertIn("limit=50000", url)
        self.assertIn("apiKey=secret", url)

    def test_normalize_daily_bar_row_maps_massive_values_to_market_bars(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc)
        row = _normalize_daily_bar_row(
            symbol="AAPL",
            bar={"t": 1773633600000, "o": 200.0, "h": 210.0, "l": 198.0, "c": 205.0, "v": 123456, "vw": 204.2},
            ingested_at=ingested_at,
        )
        self.assertEqual(row[0], "AAPL")
        self.assertEqual(row[1], "1d")
        self.assertEqual(row[4], 200.0)
        self.assertEqual(row[5], 210.0)
        self.assertEqual(row[6], 198.0)
        self.assertEqual(row[7], 205.0)
        self.assertEqual(row[8], 123456.0)
        self.assertEqual(row[9], 204.2)
        self.assertEqual(row[10], BACKFILL_DATA_STATUS)
        self.assertEqual(row[11], PROVIDER_SOURCE)
        self.assertEqual(row[12], 0)

    def test_normalize_daily_bar_row_rejects_malformed_ohlc(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc)
        with self.assertRaisesRegex(RuntimeError, "AAPL:malformed_ohlc"):
            _normalize_daily_bar_row(
                symbol="AAPL",
                bar={"t": 1773633600000, "o": 211.0, "h": 210.0, "l": 198.0, "c": 205.0, "v": 123456, "vw": 204.2},
                ingested_at=ingested_at,
            )

    def test_upsert_market_bars_replaces_overlapping_row_source(self) -> None:
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
                conn.execute(
                    """
                    INSERT INTO market_bars VALUES
                    ('AAPL', '1d', TIMESTAMP '2026-03-13 04:00:00', TIMESTAMP '2026-03-13 04:00:00',
                     200, 210, 198, 205, 1000, 204.0, 'DELAYED', 'polygon', 0,
                     TIMESTAMP '2026-03-14 00:00:00', TIMESTAMP '2026-03-14 00:00:00')
                    """
                )
            finally:
                conn.close()

            _upsert_market_bars(
                db_path,
                [
                    (
                        "AAPL",
                        "1d",
                        datetime(2026, 3, 13, 4, 0),
                        datetime(2026, 3, 13, 4, 0),
                        201.0,
                        211.0,
                        199.0,
                        206.0,
                        1200.0,
                        205.0,
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
                    SELECT open, high, low, close, volume, vwap, data_status, source
                    FROM market_bars
                    WHERE symbol='AAPL' AND timeframe='1d'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row, (201.0, 211.0, 199.0, 206.0, 1200.0, 205.0, "DELAYED", "massive"))

    def test_write_validation_artifact_persists_expected_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            rows = [
                (
                    "AAPL",
                    "1d",
                    datetime(2026, 3, 13, 4, 0),
                    datetime(2026, 3, 13, 4, 0),
                    201.0,
                    211.0,
                    199.0,
                    206.0,
                    1200.0,
                    205.0,
                    BACKFILL_DATA_STATUS,
                    PROVIDER_SOURCE,
                    0,
                    datetime(2026, 3, 16, 4, 0),
                    datetime(2026, 3, 16, 4, 0),
                )
            ]
            artifact_path = _write_validation_artifact(
                repo_root=repo_root,
                run_date="2026-03-16",
                mode=BACKFILL_MODE_VALIDATE,
                start_date="2026-01-01",
                end_date="2026-03-16",
                scoped_symbols=["AAPL", "QQQ"],
                summary=[("AAPL", 1, BACKFILL_DATA_STATUS)],
                rows_to_upsert=rows,
                errors=[],
                now_utc=datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc),
                now_local=datetime(2026, 3, 15, 23, 0, tzinfo=timezone.utc),
                local_timezone="America/Chicago",
            )
            payload = __import__("json").loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["provider"], "massive")
            self.assertEqual(payload["mode"], "validate")
            self.assertEqual(payload["scope_size"], 2)
            self.assertEqual(payload["prepared_row_count"], 1)
            self.assertFalse(payload["write_applied"])
            self.assertEqual(payload["sample_rows"][0]["source"], "massive")


if __name__ == "__main__":
    unittest.main()
