from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from tradly.pipeline.ingest_market_bars import BACKFILL_DATA_STATUS, PROVIDER_SOURCE, _upsert_market_bars
from tradly.pipeline.ingest_market_bars_1m import _build_minute_agg_url, _normalize_minute_bar_row


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


if __name__ == "__main__":
    unittest.main()
