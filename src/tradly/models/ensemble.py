from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from tradly.models.calibration import ConfidenceInputs, compute_confidence, confidence_label, normalize_score


LANE_TO_HORIZON = {
    "near_term": "1to3d",
    "swing_term": "1to2w",
    "position_term": "2to6w",
}
HORIZON_TO_LANE = {value: key for key, value in LANE_TO_HORIZON.items()}
COMPONENT_WEIGHTS = {
    "symbol_movement": 0.30,
    "symbol_news": 0.20,
    "sector_movement": 0.20,
    "market_regime": 0.15,
    "sector_news": 0.15,
}
RANGE_REFERENCE_PCT = {
    "near_term": 6.0,
    "swing_term": 10.0,
    "position_term": 16.0,
}
ENSEMBLE_RAW_SCALE = 40.0


@dataclass(frozen=True)
class ComponentLane:
    component_id: str
    scope_id: str
    lane_id: str
    canonical_horizon: str
    signal_direction: str
    signal_strength: float
    confidence_score: int
    coverage_state: str
    score_normalized: float
    why_code: tuple[str, ...]
    freshness_ok: bool
    freshness_score: int | None


def _lane_view(row: dict | None, lane_id: str, *, component_id: str, scope_id: str) -> ComponentLane:
    if not isinstance(row, dict):
        return ComponentLane(
            component_id=component_id,
            scope_id=scope_id,
            lane_id=lane_id,
            canonical_horizon=LANE_TO_HORIZON[lane_id],
            signal_direction="neutral",
            signal_strength=0.0,
            confidence_score=20,
            coverage_state="insufficient_evidence",
            score_normalized=0.0,
            why_code=(),
            freshness_ok=False,
            freshness_score=None,
        )

    lane_diagnostics = row.get("lane_diagnostics")
    if isinstance(lane_diagnostics, dict) and lane_id in lane_diagnostics and isinstance(lane_diagnostics[lane_id], dict):
        lane = lane_diagnostics[lane_id]
        return ComponentLane(
            component_id=component_id,
            scope_id=scope_id,
            lane_id=lane_id,
            canonical_horizon=str(lane.get("canonical_horizon", LANE_TO_HORIZON[lane_id])),
            signal_direction=str(lane.get("signal_direction", row.get("signal_direction", "neutral"))),
            signal_strength=float(lane.get("signal_strength", row.get("signal_strength", 0.0)) or 0.0),
            confidence_score=int(lane.get("confidence_score", row.get("confidence_score", 20)) or 20),
            coverage_state=str(lane.get("coverage_state", row.get("coverage_state", "insufficient_evidence"))),
            score_normalized=float(lane.get("score_normalized", row.get("score_normalized", 0.0)) or 0.0),
            why_code=tuple(str(code) for code in lane.get("why_code", []) if str(code).strip()),
            freshness_ok=bool(lane.get("lane_data_freshness_ok", row.get("data_freshness_ok", False))),
            freshness_score=int(lane.get("freshness_score", 0) or 0) if lane.get("freshness_score") is not None else None,
        )

    primary_lane_id = str(row.get("lane_primary") or HORIZON_TO_LANE.get(str(row.get("horizon_primary", "")), ""))
    if primary_lane_id == lane_id:
        evidence = row.get("evidence", {}) if isinstance(row.get("evidence"), dict) else {}
        return ComponentLane(
            component_id=component_id,
            scope_id=scope_id,
            lane_id=lane_id,
            canonical_horizon=str(row.get("horizon_primary", LANE_TO_HORIZON[lane_id])),
            signal_direction=str(row.get("signal_direction", "neutral")),
            signal_strength=float(row.get("signal_strength", 0.0) or 0.0),
            confidence_score=int(row.get("confidence_score", 20) or 20),
            coverage_state=str(row.get("coverage_state", "insufficient_evidence")),
            score_normalized=float(row.get("score_normalized", 0.0) or 0.0),
            why_code=tuple(str(code) for code in row.get("why_code", []) if str(code).strip()),
            freshness_ok=bool(row.get("data_freshness_ok", False)),
            freshness_score=int(evidence.get("freshness_score", 0) or 0) if evidence.get("freshness_score") is not None else None,
        )

    return ComponentLane(
        component_id=component_id,
        scope_id=scope_id,
        lane_id=lane_id,
        canonical_horizon=LANE_TO_HORIZON[lane_id],
        signal_direction="neutral",
        signal_strength=0.0,
        confidence_score=20,
        coverage_state="insufficient_evidence",
        score_normalized=0.0,
        why_code=(),
        freshness_ok=False,
        freshness_score=None,
    )


def _direction_polarity(direction: str) -> int:
    normalized = direction.strip().lower()
    if normalized == "bullish":
        return 1
    if normalized == "bearish":
        return -1
    return 0


def _coverage_multiplier(coverage_state: str) -> float:
    if coverage_state == "sufficient_evidence":
        return 1.0
    if coverage_state == "thin_evidence":
        return 0.5
    return 0.0


def _coverage_rank(coverage_state: str) -> int:
    return {"insufficient_evidence": 0, "thin_evidence": 1, "sufficient_evidence": 2}.get(coverage_state, 0)


def _range_haircut(lane_id: str, range_lane: ComponentLane | None, range_payload: dict | None) -> tuple[int, float]:
    if not isinstance(range_payload, dict):
        return 0, 0.0
    lane_diagnostics = range_payload.get("lane_diagnostics")
    if not isinstance(lane_diagnostics, dict):
        return 0, 0.0
    lane = lane_diagnostics.get(lane_id)
    if not isinstance(lane, dict):
        return 0, 0.0
    expected_move_pct = float(lane.get("expected_move_pct", 0.0) or 0.0)
    range_confidence_score = int(lane.get("confidence_score", 0) or 0)
    range_confidence_factor = range_confidence_score / 100.0
    range_pressure_score = max(
        0.0,
        min(2.0, (range_confidence_factor * expected_move_pct) / RANGE_REFERENCE_PCT[lane_id]),
    )
    haircut = round(max(0.0, range_pressure_score - 1.0) * 20)
    return haircut, range_pressure_score


def _horizon_state_for_lane(lane_output: dict[str, object], *, lane_id: str) -> str:
    coverage_state = str(lane_output.get("coverage_state", "insufficient_evidence"))
    if coverage_state == "insufficient_evidence":
        component_count = int(lane_output.get("component_count", 0) or 0)
        if component_count > 0:
            return "research_only"
        return "blocked"
    why_code = [str(code) for code in lane_output.get("why_code", [])]
    if coverage_state != "sufficient_evidence":
        return "research_only"
    if "upstream_lane_thin" in why_code:
        return "research_only"
    if "component_conflict_high" in why_code:
        if lane_id == "near_term":
            return "research_only"
        confidence_score = int(lane_output.get("confidence_score", 0) or 0)
        signal_direction = str(lane_output.get("signal_direction", "neutral")).strip().lower()
        if confidence_score < 50 or signal_direction == "neutral":
            return "research_only"
    if not bool(lane_output.get("lane_data_freshness_ok", False)):
        return "blocked"
    return "actionable"


def _build_horizon_summary(lane_outputs: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
    near_term = lane_outputs["near_term"]
    swing_term = lane_outputs["swing_term"]
    position_term = lane_outputs["position_term"]
    return {
        "1to3d": {
            "state": _horizon_state_for_lane(near_term, lane_id="near_term"),
            "signal_direction": near_term["signal_direction"],
            "signal_strength": near_term["signal_strength"],
            "confidence_score": near_term["confidence_score"],
            "confidence_label": near_term["confidence_label"],
            "coverage_state": near_term["coverage_state"],
            "score_normalized": near_term["score_normalized"],
            "why_code": list(near_term["why_code"]),
            "data_freshness_ok": bool(near_term["lane_data_freshness_ok"]),
        },
        "1to2w": {
            "state": _horizon_state_for_lane(swing_term, lane_id="swing_term"),
            "signal_direction": swing_term["signal_direction"],
            "signal_strength": swing_term["signal_strength"],
            "confidence_score": swing_term["confidence_score"],
            "confidence_label": swing_term["confidence_label"],
            "coverage_state": swing_term["coverage_state"],
            "score_normalized": swing_term["score_normalized"],
            "why_code": list(swing_term["why_code"]),
            "data_freshness_ok": bool(swing_term["lane_data_freshness_ok"]),
        },
        "2to6w": {
            "state": _horizon_state_for_lane(position_term, lane_id="position_term"),
            "signal_direction": position_term["signal_direction"],
            "signal_strength": position_term["signal_strength"],
            "confidence_score": position_term["confidence_score"],
            "confidence_label": position_term["confidence_label"],
            "coverage_state": position_term["coverage_state"],
            "score_normalized": position_term["score_normalized"],
            "why_code": list(position_term["why_code"]),
            "data_freshness_ok": bool(position_term["lane_data_freshness_ok"]),
        },
    }


def build_ensemble_rows(
    *,
    market_row: dict,
    sector_rows_by_scope: dict[str, dict],
    symbol_movement_rows_by_scope: dict[str, dict],
    symbol_news_rows_by_scope: dict[str, dict],
    sector_news_rows_by_scope: dict[str, dict],
    range_rows_by_scope: dict[str, dict],
    symbol_metadata: dict[str, dict[str, str]],
    model_symbols: list[str],
    now_utc: datetime,
) -> list[dict]:
    rows: list[dict] = []

    for symbol in model_symbols:
        metadata = symbol_metadata.get(symbol, {})
        sector = str(metadata.get("sector", "")).strip()
        asset_type = str(metadata.get("asset_type", "stock")).strip().lower() or "stock"

        lane_outputs: dict[str, dict[str, object]] = {}
        for lane_id in ("near_term", "swing_term", "position_term"):
            components = [
                _lane_view(market_row, lane_id, component_id="market_regime", scope_id="market"),
                _lane_view(sector_rows_by_scope.get(sector), lane_id, component_id="sector_movement", scope_id=sector),
                _lane_view(symbol_movement_rows_by_scope.get(symbol), lane_id, component_id="symbol_movement", scope_id=symbol),
                _lane_view(symbol_news_rows_by_scope.get(symbol), lane_id, component_id="symbol_news", scope_id=symbol),
                _lane_view(sector_news_rows_by_scope.get(sector), lane_id, component_id="sector_news", scope_id=sector),
            ]

            symbol_movement_lane = next(c for c in components if c.component_id == "symbol_movement")
            market_lane = next(c for c in components if c.component_id == "market_regime")
            sector_lane = next(c for c in components if c.component_id == "sector_movement")
            symbol_news_lane = next(c for c in components if c.component_id == "symbol_news")
            sector_news_lane = next(c for c in components if c.component_id == "sector_news")

            if market_lane.coverage_state == "insufficient_evidence":
                coverage_state = "insufficient_evidence"
            elif lane_id == "swing_term":
                if (
                    symbol_movement_lane.coverage_state == "sufficient_evidence"
                    and (
                        sector_lane.coverage_state != "insufficient_evidence"
                        or symbol_news_lane.coverage_state != "insufficient_evidence"
                        or sector_news_lane.coverage_state != "insufficient_evidence"
                    )
                ):
                    coverage_state = "sufficient_evidence"
                elif (
                    sector_lane.coverage_state == "sufficient_evidence"
                    and (
                        symbol_news_lane.coverage_state == "sufficient_evidence"
                        or sector_news_lane.coverage_state == "sufficient_evidence"
                    )
                ):
                    coverage_state = "sufficient_evidence"
                elif any(
                    component.coverage_state != "insufficient_evidence"
                    for component in (symbol_movement_lane, sector_lane, symbol_news_lane, sector_news_lane)
                ):
                    coverage_state = "thin_evidence"
                else:
                    coverage_state = "insufficient_evidence"
            elif lane_id == "position_term":
                if symbol_movement_lane.coverage_state == "sufficient_evidence":
                    coverage_state = "sufficient_evidence"
                elif (
                    sector_lane.coverage_state == "sufficient_evidence"
                    and (
                        symbol_news_lane.coverage_state == "sufficient_evidence"
                        or sector_news_lane.coverage_state == "sufficient_evidence"
                    )
                ):
                    coverage_state = "sufficient_evidence"
                elif any(
                    component.coverage_state != "insufficient_evidence"
                    for component in (sector_lane, symbol_news_lane, sector_news_lane)
                ):
                    coverage_state = "thin_evidence"
                else:
                    coverage_state = "insufficient_evidence"
            elif symbol_movement_lane.coverage_state == "insufficient_evidence":
                coverage_state = "insufficient_evidence"
            elif (
                symbol_movement_lane.coverage_state == "sufficient_evidence"
                and (
                    sector_lane.coverage_state != "insufficient_evidence"
                    or symbol_news_lane.coverage_state != "insufficient_evidence"
                )
                and market_lane.coverage_state != "insufficient_evidence"
            ):
                coverage_state = "sufficient_evidence"
            else:
                coverage_state = "thin_evidence"

            bullish_total = 0.0
            bearish_total = 0.0
            raw_contribution_sum = 0.0
            non_neutral_count = 0
            contributing_components: list[dict[str, object]] = []
            freshness_scores: list[int] = []
            for component in components:
                if component.coverage_state == "insufficient_evidence":
                    continue
                coverage_multiplier = _coverage_multiplier(component.coverage_state)
                if coverage_multiplier <= 0:
                    continue
                if component.freshness_score is not None:
                    freshness_scores.append(component.freshness_score)
                polarity = _direction_polarity(component.signal_direction)
                weighted = (
                    polarity
                    * COMPONENT_WEIGHTS[component.component_id]
                    * coverage_multiplier
                    * (component.confidence_score / 100.0)
                    * component.signal_strength
                )
                if polarity != 0:
                    non_neutral_count += 1
                    if polarity > 0:
                        bullish_total += abs(weighted)
                    else:
                        bearish_total += abs(weighted)
                raw_contribution_sum += weighted
                contributing_components.append(
                    {
                        "component_id": component.component_id,
                        "scope_id": component.scope_id,
                        "lane_id": component.lane_id,
                        "canonical_horizon": component.canonical_horizon,
                        "signal_direction": component.signal_direction,
                        "signal_strength": component.signal_strength,
                        "confidence_score": component.confidence_score,
                        "coverage_state": component.coverage_state,
                        "weighted_contribution": round(weighted, 6),
                    }
                )

            total_non_neutral_weight = bullish_total + bearish_total
            if raw_contribution_sum > 0.05:
                signal_direction = "bullish"
            elif raw_contribution_sum < -0.05:
                signal_direction = "bearish"
            else:
                signal_direction = "neutral"

            score_raw = raw_contribution_sum * 100.0
            score_normalized = normalize_score(score_raw=score_raw, raw_scale=ENSEMBLE_RAW_SCALE)
            signal_strength = round(abs(score_normalized) / 100.0, 4)

            agreement_ratio = 0.0 if total_non_neutral_weight <= 0 else max(bullish_total, bearish_total) / total_non_neutral_weight
            opposing_weighted_share = 0.0 if total_non_neutral_weight <= 0 else min(bullish_total, bearish_total) / total_non_neutral_weight
            conflict_penalty_points = round(25 * opposing_weighted_share)
            single_component_penalty = 10 if non_neutral_count == 1 else 0

            evidence_density_score = min(100, round(len(contributing_components) / len(components) * 100))
            feature_agreement_score = round(agreement_ratio * 100) if total_non_neutral_weight > 0 else 0
            freshness_score = round(sum(freshness_scores) / len(freshness_scores)) if freshness_scores else 25
            stability_score = max(25, round(100 - opposing_weighted_share * 100))
            coverage_score = 100 if coverage_state == "sufficient_evidence" else 49 if coverage_state == "thin_evidence" else 25

            base_confidence = compute_confidence(
                ConfidenceInputs(
                    evidence_density_score=evidence_density_score,
                    feature_agreement_score=feature_agreement_score,
                    freshness_score=freshness_score,
                    stability_score=stability_score,
                    coverage_score=coverage_score,
                    coverage_state=coverage_state,
                    signal_strength=signal_strength,
                    informative_feature_count=max(1, non_neutral_count),
                    independent_informative_feature_count=max(1, min(non_neutral_count, 4)),
                )
            )

            range_payload = range_rows_by_scope.get(symbol)
            range_haircut, range_pressure_score = _range_haircut(lane_id, None, range_payload)

            symbol_support_confidences = [
                c.confidence_score
                for c in components
                if c.component_id in {"symbol_movement", "symbol_news"} and _direction_polarity(c.signal_direction) != 0
            ]
            strongest_symbol_support_cap = max(symbol_support_confidences) if symbol_support_confidences else 55
            context_support_confidences = [
                c.confidence_score
                for c in components
                if c.component_id in {"market_regime", "sector_movement", "sector_news"} and _direction_polarity(c.signal_direction) != 0
            ]
            context_support_cap = max(context_support_confidences) if context_support_confidences else 70
            thin_context_cap = 70 if any(
                c.component_id in {"market_regime", "sector_movement", "sector_news"} and c.coverage_state == "thin_evidence"
                for c in components
            ) else 100
            single_component_cap = 65 if non_neutral_count == 1 else 100
            final_confidence_cap = min(
                strongest_symbol_support_cap,
                context_support_cap,
                thin_context_cap,
                single_component_cap,
            )

            confidence_score = base_confidence
            confidence_score = max(0, confidence_score - conflict_penalty_points - single_component_penalty)
            confidence_score = max(0, confidence_score - range_haircut)
            confidence_score = min(confidence_score, final_confidence_cap)

            why_code: list[str] = []
            component_reason_map = {
                ("symbol_movement", "bullish"): "symbol_movement_supports_bullish",
                ("symbol_movement", "bearish"): "symbol_movement_supports_bearish",
                ("symbol_news", "bullish"): "symbol_news_supports_bullish",
                ("symbol_news", "bearish"): "symbol_news_supports_bearish",
                ("sector_movement", "bullish"): "sector_context_supportive",
                ("sector_movement", "bearish"): "sector_context_headwind",
                ("sector_news", "bullish"): "sector_news_supportive",
                ("sector_news", "bearish"): "sector_news_headwind",
                ("market_regime", "bullish"): "market_context_supportive",
                ("market_regime", "bearish"): "market_context_headwind",
            }
            for component in components:
                code = component_reason_map.get((component.component_id, component.signal_direction))
                if code and code not in why_code:
                    why_code.append(code)
            if any(c.coverage_state == "thin_evidence" for c in components if c.component_id in {"market_regime", "sector_movement", "sector_news"}):
                why_code.append("upstream_lane_thin")
            if opposing_weighted_share > 0.20:
                why_code.append("component_conflict_high")
            if range_haircut > 0:
                why_code.append("range_expanding_conviction_reduced")
            if not why_code:
                why_code.append("ensemble_signal_mixed")

            lane_outputs[lane_id] = {
                "lane_id": lane_id,
                "canonical_horizon": LANE_TO_HORIZON[lane_id],
                "signal_direction": signal_direction,
                "signal_strength": signal_strength,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label(confidence_score),
                "coverage_state": coverage_state,
                "freshness_score": freshness_score,
                "coverage_score": coverage_score,
                "why_code": why_code,
                "lane_data_freshness_ok": freshness_score >= 70,
                "score_raw": round(score_raw, 4),
                "score_normalized": round(score_normalized, 4),
                "component_count": len(contributing_components),
                "non_neutral_count": non_neutral_count,
                "agreement_ratio": round(agreement_ratio, 4),
                "opposing_weighted_share": round(opposing_weighted_share, 4),
                "conflict_penalty_points": conflict_penalty_points + single_component_penalty,
                "range_confidence_haircut_points": range_haircut,
                "range_pressure_score": round(range_pressure_score, 4),
                "final_confidence_cap": final_confidence_cap,
                "component_inputs": contributing_components,
            }

        primary_lane_id = max(
            ("near_term", "swing_term", "position_term"),
            key=lambda lane: (
                _coverage_rank(str(lane_outputs[lane]["coverage_state"])),
                int(lane_outputs[lane]["confidence_score"]),
                abs(float(lane_outputs[lane]["score_normalized"])),
            ),
        )
        primary_lane = lane_outputs[primary_lane_id]
        secondary_lane_ids = [lane_id for lane_id in ("near_term", "swing_term", "position_term") if lane_id != primary_lane_id]
        horizon_summary = _build_horizon_summary(lane_outputs)

        rows.append(
            {
                "model_id": "ensemble_v1",
                "model_scope": "symbol",
                "scope_id": symbol,
                "horizon_primary": primary_lane["canonical_horizon"],
                "horizon_secondary": [lane_outputs[lane_id]["canonical_horizon"] for lane_id in secondary_lane_ids],
                "lane_primary": primary_lane_id,
                "lane_secondary": secondary_lane_ids,
                "signal_direction": primary_lane["signal_direction"],
                "signal_strength": primary_lane["signal_strength"],
                "confidence_score": primary_lane["confidence_score"],
                "confidence_label": primary_lane["confidence_label"],
                "coverage_state": primary_lane["coverage_state"],
                "score_raw": primary_lane["score_raw"],
                "score_normalized": primary_lane["score_normalized"],
                "why_code": primary_lane["why_code"],
                "horizon_summary": horizon_summary,
                "lane_diagnostics": lane_outputs,
                "diagnostics": {
                    "quality_inputs": {
                        "primary_lane": primary_lane_id,
                        "secondary_lanes": secondary_lane_ids,
                    }
                },
                "evidence": {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "sector": sector,
                },
                "as_of_utc": now_utc.isoformat(),
                "data_freshness_ok": bool(primary_lane["lane_data_freshness_ok"]),
            }
        )

    return rows
