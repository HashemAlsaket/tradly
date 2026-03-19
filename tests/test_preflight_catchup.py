from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from tradly.ops.preflight_catchup import (
    _intraday_source_status,
    _load_1m_watermark_coverage,
    _load_1m_watermark_max,
    _load_missing_daily_symbols,
    _load_missing_snapshot_symbols,
    _load_macro_refresh_state,
)
from tradly.services.market_watermarks import load_1m_watermark_coverage as shared_load_1m_watermark_coverage


class PreflightCatchupTests(unittest.TestCase):
    def test_load_macro_refresh_state_requires_all_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE macro_points (
                      series_id TEXT NOT NULL,
                      ts_utc TIMESTAMP NOT NULL,
                      as_of_utc TIMESTAMP NOT NULL,
                      value DOUBLE,
                      source TEXT NOT NULL,
                      ingested_at_utc TIMESTAMP NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO macro_points VALUES
                    ('DGS2', '2026-03-12 00:00:00', '2026-03-16 18:00:00', 4.1, 'fred', '2026-03-16 18:00:00'),
                    ('DGS10', '2026-03-12 00:00:00', '2026-03-16 18:01:00', 4.2, 'fred', '2026-03-16 18:01:00'),
                    ('DFF', '2026-03-12 00:00:00', '2026-03-16 18:02:00', 4.3, 'fred', '2026-03-16 18:02:00')
                    """
                )
                oldest_refresh, coverage_complete, latest_obs_by_series = _load_macro_refresh_state(conn)
            finally:
                conn.close()

            self.assertEqual(oldest_refresh, datetime(2026, 3, 16, 18, 0, tzinfo=timezone.utc))
            self.assertFalse(coverage_complete)
            self.assertEqual(
                latest_obs_by_series,
                {"DFF": "2026-03-12", "DGS10": "2026-03-12", "DGS2": "2026-03-12"},
            )

    def test_load_macro_refresh_state_returns_oldest_refresh_across_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE macro_points (
                      series_id TEXT NOT NULL,
                      ts_utc TIMESTAMP NOT NULL,
                      as_of_utc TIMESTAMP NOT NULL,
                      value DOUBLE,
                      source TEXT NOT NULL,
                      ingested_at_utc TIMESTAMP NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO macro_points VALUES
                    ('DGS2', '2026-03-12 00:00:00', '2026-03-16 18:00:00', 4.1, 'fred', '2026-03-16 18:00:00'),
                    ('DGS10', '2026-03-12 00:00:00', '2026-03-16 18:01:00', 4.2, 'fred', '2026-03-16 18:01:00'),
                    ('DFF', '2026-03-12 00:00:00', '2026-03-16 18:02:00', 4.3, 'fred', '2026-03-16 18:02:00'),
                    ('VIXCLS', '2026-03-13 00:00:00', '2026-03-16 17:59:00', 22.0, 'fred', '2026-03-16 17:59:00')
                    """
                )
                oldest_refresh, coverage_complete, latest_obs_by_series = _load_macro_refresh_state(conn)
            finally:
                conn.close()

            self.assertEqual(oldest_refresh, datetime(2026, 3, 16, 17, 59, tzinfo=timezone.utc))
            self.assertTrue(coverage_complete)
            self.assertEqual(latest_obs_by_series["VIXCLS"], "2026-03-13")

    def test_intraday_source_not_required_on_weekend(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=None,
                now_utc=datetime(2026, 3, 16, 4, 0, tzinfo=timezone.utc),
                freshness_policy="closed_calendar_relaxed",
                max_age_sec=1200,
            ),
            ("not_required", None),
        )

    def test_intraday_source_missing_when_session_active(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=None,
                now_utc=datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc),
                freshness_policy="market_hours_strict",
                max_age_sec=1200,
            ),
            ("missing", None),
        )

    def test_intraday_source_stale_under_after_hours_policy(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=datetime(2026, 3, 16, 20, 30),
                now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
                freshness_policy="after_hours_relaxed",
                max_age_sec=1200,
            ),
            ("stale", 1800),
        )

    def test_intraday_source_stale_under_premarket_tradable_policy(self) -> None:
        self.assertEqual(
            _intraday_source_status(
                latest_ts=datetime(2026, 3, 17, 20, 0),
                now_utc=datetime(2026, 3, 18, 8, 1, tzinfo=timezone.utc),
                freshness_policy="premarket_tradable",
                max_age_sec=1200,
            ),
            ("stale", 43260),
        )

    def test_load_1m_watermark_max_uses_oldest_symbol_watermark(self) -> None:
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
                    INSERT INTO pipeline_watermarks VALUES
                    ('market_bars_1m', 'AAPL', '2026-03-16 14:31:00', '{}', '2026-03-16 14:32:00'),
                    ('market_bars_1m', 'MSFT', '2026-03-16 14:29:00', '{}', '2026-03-16 14:32:00')
                    """
                )
                value = _load_1m_watermark_max(conn)
            finally:
                conn.close()

            self.assertEqual(value, datetime(2026, 3, 16, 14, 29))

    def test_load_1m_watermark_coverage_flags_missing_scope_symbols(self) -> None:
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
                    INSERT INTO pipeline_watermarks VALUES
                    ('market_bars_1m', 'AAPL', '2026-03-16 14:31:00', '{}', '2026-03-16 14:32:00')
                    """
                )
                min_watermark, coverage_complete, coverage_count = _load_1m_watermark_coverage(conn, ["AAPL", "MSFT"])
            finally:
                conn.close()

            self.assertEqual(min_watermark, datetime(2026, 3, 16, 14, 31))
            self.assertFalse(coverage_complete)
            self.assertEqual(coverage_count, 1)

    def test_shared_watermark_helper_matches_preflight_helper(self) -> None:
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
                    INSERT INTO pipeline_watermarks VALUES
                    ('market_bars_1m', 'AAPL', '2026-03-16 14:31:00', '{}', '2026-03-16 14:32:00'),
                    ('market_bars_1m', 'MSFT', '2026-03-16 14:29:00', '{}', '2026-03-16 14:32:00')
                    """
                )
                expected = _load_1m_watermark_coverage(conn, ["AAPL", "MSFT"])
                actual = shared_load_1m_watermark_coverage(conn, ["AAPL", "MSFT"])
            finally:
                conn.close()

            self.assertEqual(actual, expected)

    def test_load_missing_daily_symbols_flags_new_scope_symbol_without_daily_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE market_bars (
                      symbol TEXT,
                      ts_utc TIMESTAMP,
                      timeframe TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO market_bars VALUES
                    ('JNJ', '2026-03-17 04:00:00', '1d')
                    """
                )
                missing = _load_missing_daily_symbols(conn, ["JNJ", "MRK"], datetime(2026, 3, 17, tzinfo=timezone.utc).date())
            finally:
                conn.close()

        self.assertEqual(missing, ["MRK"])

    def test_load_missing_snapshot_symbols_flags_new_scope_symbol_without_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                      symbol TEXT,
                      as_of_utc TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO market_snapshots VALUES
                    ('JNJ', '2026-03-18 15:00:00')
                    """
                )
                missing = _load_missing_snapshot_symbols(conn, ["JNJ", "MRK"])
            finally:
                conn.close()

        self.assertEqual(missing, ["MRK"])


if __name__ == "__main__":
    unittest.main()
