from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.market_regime import Bar
from tradly.models.sector_movement import build_sector_movement_rows


def _make_bars(*, close_start: float, step: float, status: str = "DELAYED") -> list[Bar]:
    start = datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)
    bars: list[Bar] = []
    for idx in range(61):
        bars.append(
            Bar(
                ts_utc=start + timedelta(days=idx),
                close=close_start + step * idx,
                volume=1_000_000.0,
                data_status=status,
            )
        )
    return bars


class SectorMovementObservabilityTests(unittest.TestCase):
    def test_sector_row_emits_diagnostics_block(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=-0.2),
            "QQQ": _make_bars(close_start=400.0, step=-0.1),
            "IWM": _make_bars(close_start=200.0, step=-0.05),
            "DIA": _make_bars(close_start=350.0, step=-0.08),
            "VTI": _make_bars(close_start=250.0, step=-0.07),
            "XLK": _make_bars(close_start=180.0, step=0.15),
        }
        sector_members = {"Technology": ["AAPL", "MSFT", "NVDA"]}
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)

        rows = build_sector_movement_rows(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            sector_members=sector_members,
        )

        technology_row = next(row for row in rows if row["scope_id"] == "Technology")
        diagnostics = technology_row["diagnostics"]

        self.assertIn("normalization", diagnostics)
        self.assertIn("latency_assessment", diagnostics)
        self.assertIn("confidence_inputs", diagnostics)
        self.assertIn("cap_reasons", diagnostics)
        self.assertIn("audit_flags", diagnostics)
        self.assertEqual(diagnostics["latency_assessment"]["latency_class"], "delayed_material")
        self.assertEqual(diagnostics["confidence_inputs"]["informative_feature_count"], 2)
        self.assertEqual(diagnostics["normalization"]["raw_scale"], 140.0)
        self.assertEqual(technology_row["lane_primary"], "near_term")
        self.assertIn("near_term", technology_row["lane_diagnostics"])
        self.assertIn("swing_term", technology_row["lane_diagnostics"])
        self.assertIn("position_term", technology_row["lane_diagnostics"])

    def test_sector_row_emits_swing_lane_when_relative_move_is_small(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=-0.2),
            "QQQ": _make_bars(close_start=400.0, step=-0.1),
            "IWM": _make_bars(close_start=200.0, step=-0.05),
            "DIA": _make_bars(close_start=350.0, step=-0.08),
            "VTI": _make_bars(close_start=250.0, step=-0.07),
            "XLK": _make_bars(close_start=180.0, step=-0.08),
        }
        sector_members = {"Technology": ["AAPL", "MSFT", "NVDA"]}
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)

        rows = build_sector_movement_rows(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            sector_members=sector_members,
        )

        technology_row = next(row for row in rows if row["scope_id"] == "Technology")
        self.assertEqual(technology_row["lane_primary"], "swing_term")
        self.assertEqual(technology_row["horizon_primary"], "1to2w")

    def test_sector_row_emits_position_term_with_slower_horizon_support(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=0.02),
            "QQQ": _make_bars(close_start=400.0, step=0.01),
            "IWM": _make_bars(close_start=200.0, step=0.01),
            "DIA": _make_bars(close_start=350.0, step=0.015),
            "VTI": _make_bars(close_start=250.0, step=0.01),
            "XLK": _make_bars(close_start=180.0, step=0.12),
        }
        sector_members = {"Technology": ["AAPL", "MSFT", "NVDA"]}
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)

        rows = build_sector_movement_rows(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            sector_members=sector_members,
        )

        technology_row = next(row for row in rows if row["scope_id"] == "Technology")
        position_term = technology_row["lane_diagnostics"]["position_term"]

        self.assertEqual(position_term["canonical_horizon"], "2to6w")
        self.assertEqual(position_term["coverage_state"], "sufficient_evidence")
        self.assertTrue(position_term["lane_data_freshness_ok"])
        self.assertGreaterEqual(position_term["confidence_score"], technology_row["lane_diagnostics"]["swing_term"]["confidence_score"])


if __name__ == "__main__":
    unittest.main()
