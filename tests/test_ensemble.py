from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tradly.models.ensemble import build_ensemble_rows


def _lane(
    *,
    lane_id: str,
    direction: str,
    confidence: int,
    coverage_state: str = "sufficient_evidence",
    signal_strength: float = 0.7,
    score_normalized: float = 60.0,
    freshness_score: int = 100,
) -> dict:
    return {
        "lane_id": lane_id,
        "canonical_horizon": "1to3d" if lane_id == "near_term" else "1to2w" if lane_id == "swing_term" else "2to6w",
        "signal_direction": direction,
        "signal_strength": signal_strength,
        "confidence_score": confidence,
        "confidence_label": "high" if confidence >= 70 else "medium",
        "coverage_state": coverage_state,
        "freshness_score": freshness_score,
        "coverage_score": 100 if coverage_state == "sufficient_evidence" else 49,
        "why_code": [],
        "lane_data_freshness_ok": freshness_score >= 70,
        "score_raw": score_normalized,
        "score_normalized": score_normalized,
    }


def _row_with_lanes(scope_id: str, near: dict, swing: dict, position: dict | None = None) -> dict:
    if position is None:
        position = _lane(
            lane_id="position_term",
            direction=swing["signal_direction"],
            confidence=swing["confidence_score"],
            coverage_state=swing["coverage_state"],
            signal_strength=swing["signal_strength"],
            score_normalized=swing["score_normalized"],
            freshness_score=swing["freshness_score"],
        )
    return {
        "scope_id": scope_id,
        "lane_primary": "near_term",
        "horizon_primary": "1to3d",
        "signal_direction": near["signal_direction"],
        "signal_strength": near["signal_strength"],
        "confidence_score": near["confidence_score"],
        "confidence_label": near["confidence_label"],
        "coverage_state": near["coverage_state"],
        "score_raw": near["score_raw"],
        "score_normalized": near["score_normalized"],
        "why_code": near["why_code"],
        "lane_diagnostics": {
            "near_term": near,
            "swing_term": swing,
            "position_term": position,
        },
        "data_freshness_ok": True,
    }


def _symbol_movement_row(*, scope_id: str, horizon_primary: str, direction: str, confidence: int, coverage_state: str = "sufficient_evidence", signal_strength: float = 0.8, score_normalized: float = 70.0) -> dict:
    return {
        "scope_id": scope_id,
        "horizon_primary": horizon_primary,
        "signal_direction": direction,
        "signal_strength": signal_strength,
        "confidence_score": confidence,
        "confidence_label": "high" if confidence >= 70 else "medium",
        "coverage_state": coverage_state,
        "score_raw": score_normalized,
        "score_normalized": score_normalized,
        "why_code": [],
        "data_freshness_ok": True,
        "evidence": {"freshness_score": 70},
    }


class EnsembleTests(unittest.TestCase):
    def test_bullish_agreement_produces_bullish_row(self) -> None:
        market = _row_with_lanes("market", _lane(lane_id="near_term", direction="bullish", confidence=60), _lane(lane_id="swing_term", direction="bullish", confidence=80))
        sector = _row_with_lanes("Technology", _lane(lane_id="near_term", direction="bullish", confidence=65), _lane(lane_id="swing_term", direction="bullish", confidence=82))
        symbol_news = _row_with_lanes("NVDA", _lane(lane_id="near_term", direction="bullish", confidence=55), _lane(lane_id="swing_term", direction="bullish", confidence=75))
        sector_news = _row_with_lanes("Technology", _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25), _lane(lane_id="swing_term", direction="bullish", confidence=70))
        range_row = {
            "scope_id": "NVDA",
            "lane_diagnostics": {
                "near_term": {"expected_move_pct": 4.0, "confidence_score": 60},
                "swing_term": {"expected_move_pct": 8.0, "confidence_score": 70},
            },
        }
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"NVDA": _symbol_movement_row(scope_id="NVDA", horizon_primary="1to2w", direction="bullish", confidence=85)},
            symbol_news_rows_by_scope={"NVDA": symbol_news},
            sector_news_rows_by_scope={"Technology": sector_news},
            range_rows_by_scope={"NVDA": range_row},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertEqual(row["signal_direction"], "bullish")
        self.assertGreater(row["confidence_score"], 60)
        self.assertEqual(row["horizon_summary"]["1to2w"]["state"], "actionable")
        self.assertEqual(row["horizon_summary"]["2to6w"]["state"], "actionable")

    def test_single_component_signal_is_capped(self) -> None:
        neutral = _row_with_lanes("market", _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25), _lane(lane_id="swing_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25))
        rows = build_ensemble_rows(
            market_row=neutral,
            sector_rows_by_scope={"Technology": neutral},
            symbol_movement_rows_by_scope={"NVDA": _symbol_movement_row(scope_id="NVDA", horizon_primary="1to2w", direction="bullish", confidence=90)},
            symbol_news_rows_by_scope={},
            sector_news_rows_by_scope={},
            range_rows_by_scope={"NVDA": {"scope_id": "NVDA", "lane_diagnostics": {"swing_term": {"expected_move_pct": 9.0, "confidence_score": 70}, "near_term": {"expected_move_pct": 4.0, "confidence_score": 50}}}},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertLessEqual(row["confidence_score"], 65)
        self.assertEqual(row["horizon_summary"]["1to2w"]["state"], "research_only")

    def test_range_pressure_reduces_confidence(self) -> None:
        market = _row_with_lanes("market", _lane(lane_id="near_term", direction="bullish", confidence=60), _lane(lane_id="swing_term", direction="bullish", confidence=80))
        sector = _row_with_lanes("Technology", _lane(lane_id="near_term", direction="bullish", confidence=65), _lane(lane_id="swing_term", direction="bullish", confidence=82))
        symbol_news = _row_with_lanes("NVDA", _lane(lane_id="near_term", direction="bullish", confidence=55), _lane(lane_id="swing_term", direction="bullish", confidence=80))
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"NVDA": _symbol_movement_row(scope_id="NVDA", horizon_primary="1to2w", direction="bullish", confidence=85)},
            symbol_news_rows_by_scope={"NVDA": symbol_news},
            sector_news_rows_by_scope={},
            range_rows_by_scope={"NVDA": {"scope_id": "NVDA", "lane_diagnostics": {"swing_term": {"expected_move_pct": 18.0, "confidence_score": 90}, "near_term": {"expected_move_pct": 4.0, "confidence_score": 50}}}},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertIn("range_expanding_conviction_reduced", row["why_code"])

    def test_insufficient_near_term_becomes_blocked_in_horizon_summary(self) -> None:
        market = _row_with_lanes(
            "market",
            _lane(
                lane_id="near_term",
                direction="neutral",
                confidence=20,
                coverage_state="insufficient_evidence",
                signal_strength=0.0,
                score_normalized=0.0,
                freshness_score=25,
            ),
            _lane(lane_id="swing_term", direction="bullish", confidence=80),
        )
        sector = _row_with_lanes(
            "Technology",
            _lane(
                lane_id="near_term",
                direction="neutral",
                confidence=20,
                coverage_state="insufficient_evidence",
                signal_strength=0.0,
                score_normalized=0.0,
                freshness_score=25,
            ),
            _lane(lane_id="swing_term", direction="bullish", confidence=82),
        )
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"NVDA": _symbol_movement_row(scope_id="NVDA", horizon_primary="1to2w", direction="bullish", confidence=85)},
            symbol_news_rows_by_scope={},
            sector_news_rows_by_scope={},
            range_rows_by_scope={"NVDA": {"scope_id": "NVDA", "lane_diagnostics": {"swing_term": {"expected_move_pct": 8.0, "confidence_score": 70}, "near_term": {"expected_move_pct": 4.0, "confidence_score": 50}}}},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertEqual(row["horizon_summary"]["1to3d"]["state"], "blocked")
        self.assertEqual(row["horizon_summary"]["1to2w"]["state"], "actionable")

    def test_swing_term_can_still_be_actionable_with_conflict_if_confidence_is_decent(self) -> None:
        market = _row_with_lanes("market", _lane(lane_id="near_term", direction="bearish", confidence=60), _lane(lane_id="swing_term", direction="bearish", confidence=80))
        sector = _row_with_lanes("Technology", _lane(lane_id="near_term", direction="bullish", confidence=65), _lane(lane_id="swing_term", direction="bullish", confidence=82))
        sector_news = _row_with_lanes(
            "Technology",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="bullish", confidence=70),
        )
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"NVDA": _symbol_movement_row(scope_id="NVDA", horizon_primary="1to2w", direction="bearish", confidence=85)},
            symbol_news_rows_by_scope={},
            sector_news_rows_by_scope={"Technology": sector_news},
            range_rows_by_scope={"NVDA": {"scope_id": "NVDA", "lane_diagnostics": {"swing_term": {"expected_move_pct": 8.0, "confidence_score": 70}, "near_term": {"expected_move_pct": 4.0, "confidence_score": 50}}}},
            symbol_metadata={"NVDA": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["NVDA"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertIn("component_conflict_high", row["horizon_summary"]["1to2w"]["why_code"])
        self.assertEqual(row["horizon_summary"]["1to2w"]["state"], "actionable")

    def test_swing_term_can_be_actionable_from_sector_and_news_without_swing_primary_symbol_movement(self) -> None:
        market = _row_with_lanes("market", _lane(lane_id="near_term", direction="bearish", confidence=60), _lane(lane_id="swing_term", direction="bearish", confidence=82))
        sector = _row_with_lanes("Technology", _lane(lane_id="near_term", direction="bullish", confidence=65), _lane(lane_id="swing_term", direction="bullish", confidence=82))
        symbol_news = _row_with_lanes(
            "AVGO",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="bullish", confidence=84, score_normalized=76.0),
        )
        sector_news = _row_with_lanes(
            "Technology",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="bullish", confidence=83, score_normalized=72.0),
        )
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"AVGO": _symbol_movement_row(scope_id="AVGO", horizon_primary="1to3d", direction="bullish", confidence=84)},
            symbol_news_rows_by_scope={"AVGO": symbol_news},
            sector_news_rows_by_scope={"Technology": sector_news},
            range_rows_by_scope={"AVGO": {"scope_id": "AVGO", "lane_diagnostics": {"swing_term": {"expected_move_pct": 8.0, "confidence_score": 70}, "near_term": {"expected_move_pct": 4.0, "confidence_score": 50}, "position_term": {"expected_move_pct": 12.0, "confidence_score": 75}}}},
            symbol_metadata={"AVGO": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["AVGO"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertEqual(row["horizon_summary"]["1to2w"]["state"], "actionable")
        self.assertEqual(row["lane_diagnostics"]["swing_term"]["coverage_state"], "sufficient_evidence")

    def test_position_term_can_become_primary_lane(self) -> None:
        market = _row_with_lanes(
            "market",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="position_term", direction="bullish", confidence=84, score_normalized=72.0, freshness_score=90),
        )
        sector = _row_with_lanes(
            "Technology",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="position_term", direction="bullish", confidence=78, score_normalized=66.0, freshness_score=90),
        )
        symbol_news = _row_with_lanes(
            "CRM",
            _lane(lane_id="near_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="swing_term", direction="neutral", confidence=20, coverage_state="insufficient_evidence", signal_strength=0.0, score_normalized=0.0, freshness_score=25),
            _lane(lane_id="position_term", direction="bullish", confidence=80, score_normalized=68.0, freshness_score=90),
        )
        range_row = {
            "scope_id": "CRM",
            "lane_diagnostics": {
                "near_term": {"expected_move_pct": 4.0, "confidence_score": 50},
                "swing_term": {"expected_move_pct": 8.0, "confidence_score": 60},
                "position_term": {"expected_move_pct": 12.0, "confidence_score": 75},
            },
        }
        rows = build_ensemble_rows(
            market_row=market,
            sector_rows_by_scope={"Technology": sector},
            symbol_movement_rows_by_scope={"CRM": _symbol_movement_row(scope_id="CRM", horizon_primary="2to6w", direction="bullish", confidence=84)},
            symbol_news_rows_by_scope={"CRM": symbol_news},
            sector_news_rows_by_scope={},
            range_rows_by_scope={"CRM": range_row},
            symbol_metadata={"CRM": {"asset_type": "stock", "sector": "Technology"}},
            model_symbols=["CRM"],
            now_utc=datetime(2026, 3, 15, 5, 0, tzinfo=timezone.utc),
        )
        row = rows[0]
        self.assertEqual(row["horizon_primary"], "2to6w")
        self.assertEqual(row["horizon_summary"]["2to6w"]["state"], "actionable")


if __name__ == "__main__":
    unittest.main()
