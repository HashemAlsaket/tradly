from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from tradly.pipeline.ingest_market_snapshots import (
    SNAPSHOT_DATA_STATUS,
    _build_snapshot_url,
    _normalize_snapshot_row,
    _upsert_market_snapshots,
    _write_artifact,
)


class TestIngestMarketSnapshots(unittest.TestCase):
    def test_build_snapshot_url_uses_massive_host(self) -> None:
        url = _build_snapshot_url("AAPL", "secret")
        self.assertEqual(
            url,
            "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/AAPL?apiKey=secret",
        )

    def test_normalize_snapshot_row_maps_payload(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 30, tzinfo=timezone.utc)
        row = _normalize_snapshot_row(
            symbol="AAPL",
            payload={
                "updated": 1_773_689_400_000_000_000,
                "todaysChange": 1.25,
                "todaysChangePerc": 0.64,
                "day": {"o": 200.0, "h": 202.0, "l": 199.5, "c": 201.25, "v": 123456.0, "vw": 200.9},
                "prevDay": {"c": 200.0},
                "lastTrade": {"p": 201.3, "s": 100, "t": 1_773_689_399_000_000_000},
                "lastQuote": {"p": 201.2, "s": 12, "P": 201.4, "S": 15, "t": 1_773_689_398_000_000_000},
            },
            ingested_at=ingested_at,
        )
        self.assertEqual(row[0], "AAPL")
        self.assertEqual(row[2], 201.3)
        self.assertEqual(row[5], 201.2)
        self.assertEqual(row[7], 201.4)
        self.assertEqual(row[13], 201.25)
        self.assertEqual(row[15], 200.0)
        self.assertEqual(row[16], 1.25)
        self.assertEqual(row[17], 0.64)
        self.assertEqual(row[19], "snapshot")
        self.assertEqual(row[20], SNAPSHOT_DATA_STATUS)
        self.assertEqual(row[21], "massive")

    def test_normalize_snapshot_row_rejects_invalid_bid_ask(self) -> None:
        ingested_at = datetime(2026, 3, 16, 4, 30, tzinfo=timezone.utc)
        with self.assertRaisesRegex(RuntimeError, "AAPL:invalid_bid_ask"):
            _normalize_snapshot_row(
                symbol="AAPL",
                payload={
                    "updated": 1_773_689_400_000_000_000,
                    "lastQuote": {"p": 202.0, "P": 201.0},
                },
                ingested_at=ingested_at,
            )

    def test_upsert_market_snapshots_writes_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                      symbol TEXT NOT NULL,
                      as_of_utc TIMESTAMP NOT NULL,
                      last_trade_price DOUBLE,
                      last_trade_size DOUBLE,
                      last_trade_ts_utc TIMESTAMP,
                      bid_price DOUBLE,
                      bid_size DOUBLE,
                      ask_price DOUBLE,
                      ask_size DOUBLE,
                      last_quote_ts_utc TIMESTAMP,
                      session_open DOUBLE,
                      session_high DOUBLE,
                      session_low DOUBLE,
                      session_close DOUBLE,
                      session_volume DOUBLE,
                      prev_close DOUBLE,
                      change DOUBLE,
                      change_pct DOUBLE,
                      day_vwap DOUBLE,
                      market_status TEXT,
                      data_status TEXT,
                      source TEXT,
                      ingested_at_utc TIMESTAMP,
                      updated_at_utc TIMESTAMP,
                      PRIMARY KEY (symbol, as_of_utc)
                    )
                    """
                )
            finally:
                conn.close()

            _upsert_market_snapshots(
                db_path,
                [
                    (
                        "AAPL",
                        datetime(2026, 3, 16, 4, 0),
                        201.3,
                        100.0,
                        datetime(2026, 3, 16, 3, 59, 59),
                        201.2,
                        12.0,
                        201.4,
                        15.0,
                        datetime(2026, 3, 16, 3, 59, 58),
                        200.0,
                        202.0,
                        199.5,
                        201.25,
                        123456.0,
                        200.0,
                        1.25,
                        0.64,
                        200.9,
                        "snapshot",
                        SNAPSHOT_DATA_STATUS,
                        "massive",
                        datetime(2026, 3, 16, 4, 30),
                        datetime(2026, 3, 16, 4, 30),
                    )
                ],
            )

            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                row = conn.execute(
                    """
                    SELECT symbol, last_trade_price, bid_price, ask_price, data_status, source
                    FROM market_snapshots
                    WHERE symbol='AAPL'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row, ("AAPL", 201.3, 201.2, 201.4, SNAPSHOT_DATA_STATUS, "massive"))

    def test_write_artifact_persists_expected_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            rows = [
                (
                    "AAPL",
                    datetime(2026, 3, 16, 4, 0),
                    201.3,
                    100.0,
                    datetime(2026, 3, 16, 3, 59, 59),
                    201.2,
                    12.0,
                    201.4,
                    15.0,
                    datetime(2026, 3, 16, 3, 59, 58),
                    200.0,
                    202.0,
                    199.5,
                    201.25,
                    123456.0,
                    200.0,
                    1.25,
                    0.64,
                    200.9,
                    "snapshot",
                    SNAPSHOT_DATA_STATUS,
                    "massive",
                    datetime(2026, 3, 16, 4, 30),
                    datetime(2026, 3, 16, 4, 30),
                )
            ]
            artifact_path = _write_artifact(
                repo_root=repo_root,
                run_date="2026-03-16",
                mode="validate",
                scoped_symbols=["AAPL", "QQQ"],
                summary=[("AAPL", SNAPSHOT_DATA_STATUS)],
                rows=rows,
                errors=[],
                now_utc=datetime(2026, 3, 16, 4, 30, tzinfo=timezone.utc),
                now_local=datetime(2026, 3, 15, 23, 30, tzinfo=timezone.utc),
                local_timezone="America/Chicago",
            )
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["provider"], "massive")
            self.assertEqual(payload["artifact_type"], "market_snapshots")
            self.assertEqual(payload["scope_size"], 2)
            self.assertEqual(payload["prepared_row_count"], 1)
            self.assertFalse(payload["write_applied"])
            self.assertEqual(payload["sample_rows"][0]["source"], "massive")


if __name__ == "__main__":
    unittest.main()
