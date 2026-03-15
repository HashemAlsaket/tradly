from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.models.calibration import (
    ConfidenceInputs,
    assess_latency,
    audit_model_artifact,
    compute_confidence,
    normalize_score,
)
from tradly.models.market_regime import Bar, REGIME_SYMBOLS, build_market_regime_row


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


class CalibrationAndMarketRegimeTests(unittest.TestCase):
    def test_normalize_score_is_bounded_and_monotonic(self) -> None:
        low = normalize_score(score_raw=-1.0, raw_scale=2.0)
        mid = normalize_score(score_raw=0.0, raw_scale=2.0)
        high = normalize_score(score_raw=1.0, raw_scale=2.0)

        self.assertGreaterEqual(low, -100.0)
        self.assertLessEqual(high, 100.0)
        self.assertLess(low, mid)
        self.assertLess(mid, high)

    def test_assess_latency_stale_forces_thin_evidence_and_confidence_cap(self) -> None:
        assessment = assess_latency(data_status="DELAYED", recency_ok=False, horizon="1to3d")

        self.assertEqual(assessment.latency_class, "stale")
        self.assertEqual(assessment.freshness_cap, 60)
        self.assertEqual(assessment.confidence_cap, 49)
        self.assertEqual(assessment.forced_coverage_state, "thin_evidence")

    def test_compute_confidence_weak_signal_caps_are_monotonic(self) -> None:
        def _confidence(signal_strength: float) -> int:
            return compute_confidence(
                ConfidenceInputs(
                    evidence_density_score=100,
                    feature_agreement_score=100,
                    freshness_score=85,
                    stability_score=90,
                    coverage_score=100,
                    coverage_state="sufficient_evidence",
                    signal_strength=signal_strength,
                    informative_feature_count=3,
                    independent_informative_feature_count=3,
                )
            )

        very_weak = _confidence(0.03)
        weak = _confidence(0.08)
        modest = _confidence(0.12)
        strong = _confidence(0.25)

        self.assertLessEqual(very_weak, weak)
        self.assertLessEqual(weak, modest)
        self.assertLessEqual(modest, strong)

    def test_audit_model_artifact_accepts_list_data_status_shape(self) -> None:
        audit = audit_model_artifact(
            [
                {
                    "score_normalized": 20.0,
                    "confidence_score": 60,
                    "signal_direction": "bullish",
                    "coverage_state": "sufficient_evidence",
                    "horizon_primary": "1to3d",
                    "why_code": ["market_data_delayed_15m"],
                    "evidence": {
                        "data_status": ["DELAYED"],
                        "market_data_latency_minutes": 15,
                        "latency_class": "delayed_material",
                    },
                }
            ]
        )

        self.assertEqual(audit.status, "pass")
        self.assertNotIn("delayed_data_missing_latency_metadata", audit.failure_reasons)

    def test_market_regime_row_emits_shared_latency_fields(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=-1.0),
            "QQQ": _make_bars(close_start=400.0, step=-0.5),
            "VIXY": _make_bars(close_start=25.0, step=0.15),
            "TLT": _make_bars(close_start=100.0, step=-0.2),
            "IEF": _make_bars(close_start=95.0, step=-0.1),
            "SHY": _make_bars(close_start=82.0, step=-0.02),
        }
        now_utc = datetime(2026, 3, 2, 20, 0, tzinfo=timezone.utc)

        row = build_market_regime_row(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            latest_macro_ts_utc=datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc),
            latest_macro_news_ts_utc=datetime(2026, 3, 14, 18, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(row["model_id"], "market_regime_v1")
        self.assertEqual(row["model_scope"], "market")
        self.assertIn("market_data_delayed_15m", row["why_code"])
        self.assertEqual(row["evidence"]["market_data_latency_minutes"], 15)
        self.assertEqual(row["evidence"]["latency_class"], "delayed_material")
        self.assertEqual(row["evidence"]["freshness_score"], 70)
        self.assertNotIn("quality_audit", row["evidence"])
        self.assertTrue(set(REGIME_SYMBOLS).issubset(set(row["evidence"]["required_symbols"])))

    def test_market_regime_macro_warning_hits_near_term_harder_than_swing_term(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=-1.0),
            "QQQ": _make_bars(close_start=400.0, step=-0.5),
            "VIXY": _make_bars(close_start=25.0, step=0.15),
            "TLT": _make_bars(close_start=100.0, step=-0.2),
            "IEF": _make_bars(close_start=95.0, step=-0.1),
            "SHY": _make_bars(close_start=82.0, step=-0.02),
        }
        now_utc = datetime(2026, 3, 3, 3, 46, tzinfo=timezone.utc)

        row = build_market_regime_row(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            latest_macro_ts_utc=datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc),
            latest_macro_news_ts_utc=datetime(2026, 3, 2, 18, 0, tzinfo=timezone.utc),
        )

        near_term = row["lane_diagnostics"]["near_term"]
        swing_term = row["lane_diagnostics"]["swing_term"]

        self.assertEqual(near_term["coverage_state"], "thin_evidence")
        self.assertEqual(near_term["confidence_score"], 49)
        self.assertEqual(swing_term["coverage_state"], "sufficient_evidence")
        self.assertGreater(swing_term["confidence_score"], near_term["confidence_score"])
        self.assertEqual(row["lane_primary"], "swing_term")
        self.assertEqual(row["horizon_primary"], "1to2w")

    def test_market_regime_emits_position_term_with_slower_macro_tolerance(self) -> None:
        bars_by_symbol = {
            "SPY": _make_bars(close_start=500.0, step=-1.0),
            "QQQ": _make_bars(close_start=400.0, step=-0.5),
            "VIXY": _make_bars(close_start=25.0, step=0.15),
            "TLT": _make_bars(close_start=100.0, step=-0.2),
            "IEF": _make_bars(close_start=95.0, step=-0.1),
            "SHY": _make_bars(close_start=82.0, step=-0.02),
        }
        now_utc = datetime(2026, 3, 3, 3, 46, tzinfo=timezone.utc)

        row = build_market_regime_row(
            bars_by_symbol=bars_by_symbol,
            now_utc=now_utc,
            latest_macro_ts_utc=datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc),
            latest_macro_news_ts_utc=datetime(2026, 2, 27, 18, 0, tzinfo=timezone.utc),
        )

        position_term = row["lane_diagnostics"]["position_term"]
        self.assertIn("position_term", row["lane_diagnostics"])
        self.assertEqual(position_term["canonical_horizon"], "2to6w")
        self.assertEqual(position_term["coverage_state"], "sufficient_evidence")
        self.assertGreater(position_term["confidence_score"], row["lane_diagnostics"]["swing_term"]["confidence_score"])
        self.assertTrue(position_term["lane_data_freshness_ok"])


if __name__ == "__main__":
    unittest.main()
