from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.market_regime import Bar, IntradayBar, SnapshotPoint
from tradly.models.symbol_movement import build_symbol_movement_rows


def _make_bars(*, close_start: float, step: float, volume: float = 1_500_000.0, status: str = "DELAYED") -> list[Bar]:
    start = datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)
    bars: list[Bar] = []
    for idx in range(61):
        bars.append(
            Bar(
                ts_utc=start + timedelta(days=idx),
                close=close_start + step * idx,
                volume=volume,
                data_status=status,
            )
        )
    return bars


def _make_position_bars(*, close_start: float, early_step: float, late_step: float, volume: float = 1_500_000.0, status: str = "DELAYED") -> list[Bar]:
    start = datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)
    bars: list[Bar] = []
    close = close_start
    for idx in range(61):
        step = early_step if idx < 40 else late_step
        close += step
        bars.append(
            Bar(
                ts_utc=start + timedelta(days=idx),
                close=close,
                volume=volume,
                data_status=status,
            )
        )
    return bars


def _make_intraday_bars(*, close_start: float, step: float, volume: float = 25_000.0, status: str = "DELAYED") -> list[IntradayBar]:
    start = datetime(2026, 3, 16, 9, 30, tzinfo=timezone.utc)
    bars: list[IntradayBar] = []
    for idx in range(5):
        bars.append(
            IntradayBar(
                ts_utc=start + timedelta(minutes=idx),
                close=close_start + step * idx,
                volume=volume,
                data_status=status,
            )
        )
    return bars


class SymbolMovementTests(unittest.TestCase):
    def test_stock_symbol_row_uses_market_and_sector_overlays(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bearish",
                "score_normalized": -20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 80,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "swing_term": {
                        "confidence_score": 78,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.02},
            },
            sector_rows_by_scope={
                "Technology": {
                    "score_normalized": 18.0,
                    "evidence": {"sector_proxy_r20": 0.015},
                    "lane_diagnostics": {
                        "near_term": {
                            "confidence_score": 72,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "swing_term": {
                            "confidence_score": 74,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "position_term": {
                            "confidence_score": 78,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                    },
                }
            },
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )

        row = rows[0]
        self.assertEqual(row["model_id"], "symbol_movement_v1")
        self.assertEqual(row["model_scope"], "symbol")
        self.assertEqual(row["scope_id"], "AAPL")
        self.assertTrue(row["evidence"]["sector_overlay_present"])
        self.assertEqual(row["evidence"]["market_data_latency_minutes"], 15)
        self.assertIn("market_data_delayed_15m", row["why_code"])
        self.assertIn("normalization", row["diagnostics"])
        self.assertIn("confidence_inputs", row["diagnostics"])
        self.assertTrue(row["diagnostics"]["overlay_alignment"]["market_overlay_fresh"])
        self.assertEqual(row["diagnostics"]["overlay_alignment"]["market_overlay_lane_id"], "near_term")
        self.assertEqual(row["evidence"]["intraday_overlay"]["symbol_intraday_overlay_state"], "unavailable")

    def test_etf_symbol_does_not_require_sector_overlay(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"SPY": _make_bars(close_start=500.0, step=0.5)},
            symbol_metadata={"SPY": {"asset_type": "etf", "sector": "ETF"}},
            market_regime_row={
                "signal_direction": "neutral",
                "score_normalized": 0.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 80,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "swing_term": {
                        "confidence_score": 78,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.01},
            },
            sector_rows_by_scope={},
            model_symbols=["SPY"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )

        row = rows[0]
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertFalse(row["evidence"]["sector_overlay_present"])
        self.assertNotIn("sector_overlay_missing", row["why_code"])

    def test_stock_symbol_row_downgrades_when_sector_overlay_is_stale(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bearish",
                "score_normalized": -20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 80,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "swing_term": {
                        "confidence_score": 78,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.02},
            },
            sector_rows_by_scope={
                "Technology": {
                    "score_normalized": 18.0,
                    "evidence": {"sector_proxy_r20": 0.015},
                    "lane_diagnostics": {
                        "near_term": {
                            "confidence_score": 72,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "swing_term": {
                            "confidence_score": 74,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "position_term": {
                            "confidence_score": 78,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                    },
                }
            },
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=False,
        )

        row = rows[0]
        self.assertEqual(row["coverage_state"], "thin_evidence")
        self.assertIn("sector_overlay_stale", row["why_code"])
        self.assertFalse(row["diagnostics"]["overlay_alignment"]["sector_overlay_fresh"])

    def test_near_term_symbol_inherits_thin_market_lane(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bearish",
                "score_normalized": -82.0,
                "confidence_score": 82,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 49,
                        "coverage_state": "thin_evidence",
                        "lane_data_freshness_ok": False,
                    },
                    "swing_term": {
                        "confidence_score": 82,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.02},
            },
            sector_rows_by_scope={
                "Technology": {
                    "score_normalized": 18.0,
                    "evidence": {"sector_proxy_r20": 0.015},
                    "lane_diagnostics": {
                        "near_term": {
                            "confidence_score": 72,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "swing_term": {
                            "confidence_score": 74,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "position_term": {
                            "confidence_score": 78,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                    },
                }
            },
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )

        row = rows[0]
        self.assertEqual(row["horizon_primary"], "1to3d")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreater(row["confidence_score"], 49)
        self.assertLess(row["confidence_score"], 80)
        self.assertIn("market_overlay_lane_thin", row["why_code"])

    def test_swing_term_symbol_can_stay_usable_when_market_swing_lane_is_strong(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"KO": _make_bars(close_start=60.0, step=0.01)},
            symbol_metadata={"KO": {"asset_type": "stock", "sector": "Consumer Defensive"}},
            market_regime_row={
                "signal_direction": "bearish",
                "score_normalized": -82.0,
                "confidence_score": 82,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 49,
                        "coverage_state": "thin_evidence",
                        "lane_data_freshness_ok": False,
                    },
                    "swing_term": {
                        "confidence_score": 82,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.0},
            },
            sector_rows_by_scope={
                "Consumer Defensive": {
                    "score_normalized": 5.0,
                    "evidence": {"sector_proxy_r20": 0.03},
                    "lane_diagnostics": {
                        "near_term": {
                            "confidence_score": 70,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "swing_term": {
                            "confidence_score": 76,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "position_term": {
                            "confidence_score": 80,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                    },
                }
            },
            model_symbols=["KO"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )

        row = rows[0]
        self.assertEqual(row["horizon_primary"], "1to2w")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreaterEqual(row["confidence_score"], 50)
        self.assertEqual(row["diagnostics"]["overlay_alignment"]["market_overlay_lane_id"], "swing_term")

    def test_position_term_symbol_uses_2to6w_market_and_sector_lanes(self) -> None:
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"ADBE": _make_position_bars(close_start=500.0, early_step=1.2, late_step=0.08)},
            symbol_metadata={"ADBE": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bullish",
                "score_normalized": 35.0,
                "confidence_score": 84,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {
                        "confidence_score": 49,
                        "coverage_state": "thin_evidence",
                        "lane_data_freshness_ok": False,
                    },
                    "swing_term": {
                        "confidence_score": 78,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                    "position_term": {
                        "confidence_score": 84,
                        "coverage_state": "sufficient_evidence",
                        "lane_data_freshness_ok": True,
                    },
                },
                "evidence": {"spy_r20": 0.0},
            },
            sector_rows_by_scope={
                "Technology": {
                    "score_normalized": 22.0,
                    "evidence": {"sector_proxy_r20": 0.0},
                    "lane_diagnostics": {
                        "near_term": {
                            "confidence_score": 68,
                            "coverage_state": "thin_evidence",
                            "lane_data_freshness_ok": False,
                        },
                        "swing_term": {
                            "confidence_score": 74,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                        "position_term": {
                            "confidence_score": 79,
                            "coverage_state": "sufficient_evidence",
                            "lane_data_freshness_ok": True,
                        },
                    },
                }
            },
            model_symbols=["ADBE"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )

        row = rows[0]
        self.assertEqual(row["horizon_primary"], "2to6w")
        self.assertIn("1to2w", row["horizon_secondary"])
        self.assertIn("1to3d", row["horizon_secondary"])
        self.assertEqual(row["diagnostics"]["overlay_alignment"]["market_overlay_lane_id"], "position_term")
        self.assertEqual(row["diagnostics"]["overlay_alignment"]["sector_overlay_lane_id"], "position_term")
        self.assertEqual(row["coverage_state"], "sufficient_evidence")
        self.assertGreaterEqual(row["confidence_score"], 50)

    def test_symbol_intraday_overlay_can_confirm_bullish_setup(self) -> None:
        now_utc = datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            intraday_bars_by_symbol={
                "AAPL": _make_intraday_bars(close_start=241.0, step=0.4),
                "SPY": _make_intraday_bars(close_start=529.0, step=0.05),
            },
            latest_snapshots_by_symbol={},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bullish",
                "score_normalized": 20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {"confidence_score": 80, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "swing_term": {"confidence_score": 78, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "position_term": {"confidence_score": 84, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                },
                "evidence": {"spy_r20": 0.02, "spy_latest_close": 528.0},
            },
            sector_rows_by_scope={},
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )
        row = rows[0]
        overlay = row["evidence"]["intraday_overlay"]
        self.assertEqual(overlay["symbol_intraday_overlay_state"], "confirming")
        self.assertEqual(overlay["symbol_intraday_overlay_freshness"], "minute_confirmed")
        self.assertIn("symbol_intraday_confirming", row["why_code"])

    def test_symbol_intraday_overlay_can_fade_bullish_setup(self) -> None:
        now_utc = datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            intraday_bars_by_symbol={
                "AAPL": _make_intraday_bars(close_start=239.0, step=-0.5),
                "SPY": _make_intraday_bars(close_start=529.0, step=0.05),
            },
            latest_snapshots_by_symbol={},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bullish",
                "score_normalized": 20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {"confidence_score": 80, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "swing_term": {"confidence_score": 78, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "position_term": {"confidence_score": 84, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                },
                "evidence": {"spy_r20": 0.02, "spy_latest_close": 528.0},
            },
            sector_rows_by_scope={},
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )
        row = rows[0]
        overlay = row["evidence"]["intraday_overlay"]
        self.assertEqual(overlay["symbol_intraday_overlay_state"], "fading")
        self.assertIn("symbol_intraday_fading", row["why_code"])

    def test_symbol_intraday_overlay_snapshot_only(self) -> None:
        now_utc = datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            intraday_bars_by_symbol={},
            latest_snapshots_by_symbol={
                "AAPL": SnapshotPoint(
                    as_of_utc=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
                    last_trade_price=242.0,
                    prev_close=240.0,
                    change_pct=0.83,
                    day_vwap=None,
                    market_status="open",
                    data_status="REALTIME",
                )
            },
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bullish",
                "score_normalized": 20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {"confidence_score": 80, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "swing_term": {"confidence_score": 78, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "position_term": {"confidence_score": 84, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                },
                "evidence": {"spy_r20": 0.02, "spy_latest_close": 528.0},
            },
            sector_rows_by_scope={},
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )
        overlay = rows[0]["evidence"]["intraday_overlay"]
        self.assertEqual(overlay["symbol_intraday_overlay_freshness"], "snapshot_only")

    def test_symbol_intraday_overlay_flags_sector_and_market_lag(self) -> None:
        now_utc = datetime(2026, 3, 16, 15, 0, tzinfo=timezone.utc)
        rows = build_symbol_movement_rows(
            bars_by_symbol={"AAPL": _make_bars(close_start=180.0, step=1.0)},
            intraday_bars_by_symbol={
                "AAPL": _make_intraday_bars(close_start=239.0, step=-0.3),
                "SPY": _make_intraday_bars(close_start=529.0, step=0.2),
            },
            latest_snapshots_by_symbol={},
            symbol_metadata={"AAPL": {"asset_type": "stock", "sector": "Technology"}},
            market_regime_row={
                "signal_direction": "bullish",
                "score_normalized": 20.0,
                "confidence_score": 80,
                "coverage_state": "sufficient_evidence",
                "lane_diagnostics": {
                    "near_term": {"confidence_score": 80, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "swing_term": {"confidence_score": 78, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                    "position_term": {"confidence_score": 84, "coverage_state": "sufficient_evidence", "lane_data_freshness_ok": True},
                },
                "evidence": {"spy_r20": 0.02, "spy_latest_close": 528.0},
            },
            sector_rows_by_scope={
                "Technology": {
                    "evidence": {
                        "intraday_overlay": {
                            "proxy_intraday_return_pct": 0.01,
                            "proxy_snapshot_change_pct": 0.01,
                        }
                    }
                }
            },
            model_symbols=["AAPL"],
            now_utc=now_utc,
            market_overlay_fresh=True,
            sector_overlay_fresh=True,
        )
        row = rows[0]
        self.assertIn("symbol_lagging_sector_intraday", row["why_code"])
        self.assertIn("symbol_lagging_market_intraday", row["why_code"])


if __name__ == "__main__":
    unittest.main()
