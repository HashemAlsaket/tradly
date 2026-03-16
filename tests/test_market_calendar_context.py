from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from tradly.ops.refresh_market_calendar_context import refresh_market_calendar_context
from tradly.services.market_calendar import build_trading_calendar_row


class TradingCalendarContextTests(unittest.TestCase):
    def test_build_trading_calendar_row_marks_weekend_and_previous_cash_session(self) -> None:
        row = build_trading_calendar_row(date(2026, 3, 15))
        self.assertEqual(row.market_calendar_state, "weekend")
        self.assertTrue(row.is_weekend)
        self.assertFalse(row.is_trading_day)
        self.assertEqual(row.day_name, "Sunday")
        self.assertEqual(row.day_of_week, 0)
        self.assertEqual(row.last_cash_session_date.isoformat(), "2026-03-13")

    def test_refresh_market_calendar_context_backfills_calendar_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            data_dir = repo_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = data_dir / "tradly.duckdb"
            schema_sql = (Path(__file__).resolve().parents[1] / "db" / "schema_v1.sql").read_text(encoding="utf-8")

            import duckdb

            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(schema_sql)
                conn.execute(
                    """
                    INSERT INTO instruments (
                      symbol, asset_type, sector, industry, halal_flag, active, as_of_utc, ingested_at_utc
                    ) VALUES ('AAPL', 'stock', 'Technology', 'Consumer Electronics', 'allowed', TRUE, TIMESTAMP '2026-03-15 00:00:00', TIMESTAMP '2026-03-15 00:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO market_bars (
                      symbol, timeframe, ts_utc, as_of_utc, open, high, low, close, volume, vwap, data_status, source, correction_seq, ingested_at_utc, updated_at_utc
                    ) VALUES (
                      'AAPL', '1d', TIMESTAMP '2026-03-13 21:00:00', TIMESTAMP '2026-03-13 21:00:00',
                      200, 201, 198, 199, 1000000, 199.5, 'DELAYED', 'test', 0, TIMESTAMP '2026-03-13 21:05:00', TIMESTAMP '2026-03-13 21:05:00'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO news_events (
                      provider, provider_news_id, published_at_utc, as_of_utc, source_name, source_quality, headline, summary, url,
                      sentiment_label, sentiment_score, extraction_confidence, ingested_at_utc
                    ) VALUES (
                      'test', 'n1', TIMESTAMP '2026-03-15 16:00:00', TIMESTAMP '2026-03-15 16:00:00', 'wire', 'high', 'Weekend note', 'summary', 'https://example.com',
                      'neutral', 0.0, 0.9, TIMESTAMP '2026-03-15 16:05:00'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO news_interpretations (
                      provider, provider_news_id, model, prompt_version, bucket, impact_scope, impact_direction, impact_horizon,
                      relevance_symbols_json, thesis_tags_json, market_impact_note, confidence_label,
                      based_on_provided_evidence, calculation_performed, interpreted_at_utc, ingested_at_utc
                    ) VALUES (
                      'test', 'n1', 'gpt', 'news_interpreter_v1', 'symbol', 'symbol_specific', 'bullish', '1to2w',
                      '["AAPL"]', '["rebound"]', 'Weekend interpretation.', 'medium',
                      TRUE, FALSE, TIMESTAMP '2026-03-15 17:00:00', TIMESTAMP '2026-03-15 17:00:00'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO news_pull_usage (
                      usage_id, provider, bucket, symbols_csv, request_count, request_date_utc, response_status, detail,
                      new_events_upserted, new_symbol_links_upserted, created_at_utc
                    ) VALUES (
                      'u1', 'test', 'broad', 'AAPL', 1, DATE '2026-03-15', 'success', '',
                      1, 1, TIMESTAMP '2026-03-15 15:00:00'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO macro_points (
                      series_id, ts_utc, as_of_utc, value, source, ingested_at_utc
                    ) VALUES (
                      'DGS10', TIMESTAMP '2026-03-15 12:00:00', TIMESTAMP '2026-03-15 12:00:00', 4.2, 'test', TIMESTAMP '2026-03-15 12:05:00'
                    )
                    """
                )
            finally:
                conn.close()

            payload = refresh_market_calendar_context(repo_root=repo_root)
            self.assertEqual(payload["status"], "PASS")

            conn = duckdb.connect(str(db_path), read_only=True)
            try:
                market_row = conn.execute(
                    "SELECT calendar_date, day_name, market_calendar_state, last_cash_session_date FROM market_bars WHERE symbol='AAPL'"
                ).fetchone()
                self.assertEqual(market_row[0].isoformat(), "2026-03-13")
                self.assertEqual(market_row[1], "Friday")
                self.assertEqual(market_row[2], "trading_day")
                self.assertEqual(market_row[3].isoformat(), "2026-03-13")

                news_row = conn.execute(
                    "SELECT calendar_date, day_name, market_calendar_state, last_cash_session_date FROM news_events WHERE provider='test' AND provider_news_id='n1'"
                ).fetchone()
                self.assertEqual(news_row[0].isoformat(), "2026-03-15")
                self.assertEqual(news_row[1], "Sunday")
                self.assertEqual(news_row[2], "weekend")
                self.assertEqual(news_row[3].isoformat(), "2026-03-13")

                pull_row = conn.execute(
                    "SELECT day_name, market_calendar_state, last_cash_session_date FROM news_pull_usage WHERE usage_id='u1'"
                ).fetchone()
                self.assertEqual(pull_row[0], "Sunday")
                self.assertEqual(pull_row[1], "weekend")
                self.assertEqual(pull_row[2].isoformat(), "2026-03-13")

                calendar_count = conn.execute("SELECT COUNT(*) FROM market_calendar").fetchone()[0]
                self.assertGreater(calendar_count, 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
