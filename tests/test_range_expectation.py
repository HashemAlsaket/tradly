from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.range_expectation import DailyBar, build_range_expectation_rows


def _bars(*, count: int, start_close: float, step: float, spike_every: int = 0) -> list[DailyBar]:
    now_utc = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
    bars: list[DailyBar] = []
    close = start_close
    for idx in range(count):
        ts = now_utc - timedelta(days=count - idx)
        move = step
        if spike_every and idx % spike_every == 0:
            move *= 2.5
        open_ = close
        high = close * (1.01 + abs(move) * 0.2)
        low = close * (0.99 - abs(move) * 0.1)
        close = close * (1.0 + move)
        bars.append(
            DailyBar(
                ts_utc=ts,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=1_000_000.0,
                data_status="DELAYED",
            )
        )
    return bars


class RangeExpectationTests(unittest.TestCase):
    def test_builds_lane_aware_row_with_expected_move_fields(self) -> None:
        rows = build_range_expectation_rows(
            bars_by_symbol={"NVDA": _bars(count=70, start_close=100.0, step=0.01, spike_every=5)},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
            expected_min_market_date=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc).date(),
        )
        row = rows[0]
        self.assertEqual(row["model_id"], "range_expectation_v1")
        self.assertIn("swing_term", row["lane_diagnostics"])
        self.assertIn("position_term", row["lane_diagnostics"])
        self.assertIn("expected_move_pct", row["lane_diagnostics"]["swing_term"])
        self.assertEqual(row["signal_direction"], "neutral")

    def test_missing_history_becomes_insufficient(self) -> None:
        rows = build_range_expectation_rows(
            bars_by_symbol={"AAPL": _bars(count=10, start_close=100.0, step=0.005)},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["AAPL"],
            now_utc=datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
            expected_min_market_date=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc).date(),
        )
        row = rows[0]
        self.assertEqual(row["coverage_state"], "insufficient_evidence")
        self.assertEqual(row["confidence_score"], 20)

    def test_range_expansion_sets_expanding_why_code(self) -> None:
        bars = _bars(count=70, start_close=100.0, step=0.015, spike_every=2)
        rows = build_range_expectation_rows(
            bars_by_symbol={"COIN": bars},
            symbol_metadata={"COIN": {"asset_type": "stock", "sector": "Financial Services"}},
            model_symbols=["COIN"],
            now_utc=datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
            expected_min_market_date=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc).date(),
        )
        row = rows[0]
        self.assertIn("expected_range_expanding", row["why_code"])

    def test_subtle_range_regime_gets_soft_confidence_cap(self) -> None:
        bars = _bars(count=70, start_close=100.0, step=0.001)
        rows = build_range_expectation_rows(
            bars_by_symbol={"WMT": bars},
            symbol_metadata={"WMT": {"asset_type": "stock", "sector": "Consumer Defensive"}},
            model_symbols=["WMT"],
            now_utc=datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
            expected_min_market_date=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc).date(),
        )
        row = rows[0]
        self.assertLessEqual(row["confidence_score"], 60)
        self.assertIn("range_regime_subtle", row["why_code"])


if __name__ == "__main__":
    unittest.main()
