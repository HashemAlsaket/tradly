from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import log1p

from tradly.models.calibration import ConfidenceInputs, compute_confidence, confidence_label, normalize_score


RAW_SCORE_SCALE = 70.0
ARTICLE_COUNT_CONFIDENCE_CAP = {
    1: {"near_term": 65, "swing_term": 75, "position_term": 80},
    2: {"near_term": 75, "swing_term": 82, "position_term": 86},
}
LANE_TO_HORIZONS = {
    "near_term": {"intraday", "1to3d"},
    "swing_term": {"1to2w"},
    "position_term": {"2to6w"},
}
LANE_CONFIDENCE_CEILING = {
    "near_term": 88,
    "swing_term": 86,
    "position_term": 84,
}
SECTOR_SCOPE_BY_NAME = {
    "Technology": "technology",
    "Healthcare": "healthcare",
    "Financial Services": "financial_services",
    "Industrials": "industrials",
    "Consumer Defensive": "consumer_defensive",
    "Communication Services": "communication_services",
    "Consumer Cyclical": "consumer_cyclical",
    "Basic Materials": "basic_materials",
    "Real Estate": "real_estate",
    "Utilities": "utilities",
    "Energy": "energy",
}
CONFIDENCE_WEIGHT = {
    "low": 0.6,
    "medium": 1.0,
    "high": 1.25,
}


@dataclass(frozen=True)
class SectorNewsItem:
    provider: str
    provider_news_id: str
    published_at_utc: datetime
    interpreted_at_utc: datetime
    bucket: str
    impact_scope: str
    impact_direction: str
    impact_horizon: str
    confidence_label: str
    relevance_symbols: tuple[str, ...]
    thesis_tags: tuple[str, ...]
    market_impact_note: str


def _direction_score(direction: str) -> float:
    normalized = direction.strip().lower()
    if normalized in {"bullish", "bullish_semis", "risk_on"}:
        return 1.0
    if normalized in {"bearish", "bearish_semis", "risk_off"}:
        return -1.0
    return 0.0


def _lane_for_horizon(horizon: str) -> str | None:
    normalized = horizon.strip()
    for lane_id, horizons in LANE_TO_HORIZONS.items():
        if normalized in horizons:
            return lane_id
    return None


def _confidence_weight(label: str) -> float:
    return CONFIDENCE_WEIGHT.get(label.strip().lower(), 0.6)


def _lane_confidence_cap(*, lane_id: str, article_count: int, total_weight: float) -> int:
    hard_cap = LANE_CONFIDENCE_CEILING[lane_id]
    if lane_id == "near_term":
        return hard_cap
    density_cap = 74 + min(article_count, 6) + round(min(total_weight, 4.0))
    return min(hard_cap, density_cap)


def _recency_weight(*, lane_id: str, age_hours: float) -> float:
    if lane_id == "near_term":
        if age_hours <= 24:
            return 1.0
        if age_hours <= 72:
            return 0.7
        if age_hours <= 168:
            return 0.45
        return 0.0
    if lane_id == "swing_term":
        if age_hours <= 72:
            return 1.0
        if age_hours <= 168:
            return 0.8
        if age_hours <= 336:
            return 0.55
        return 0.0
    if age_hours <= 168:
        return 1.0
    if age_hours <= 336:
        return 0.85
    if age_hours <= 504:
        return 0.65
    return 0.35 if age_hours <= 720 else 0.0


def _freshness_score(*, lane_id: str, latest_age_hours: float | None) -> int:
    if latest_age_hours is None:
        return 25
    if lane_id == "near_term":
        if latest_age_hours <= 24:
            return 100
        if latest_age_hours <= 72:
            return 70
        if latest_age_hours <= 168:
            return 50
        return 25
    if lane_id == "swing_term":
        if latest_age_hours <= 72:
            return 100
        if latest_age_hours <= 168:
            return 85
        if latest_age_hours <= 336:
            return 65
        return 30
    if latest_age_hours <= 168:
        return 100
    if latest_age_hours <= 504:
        return 90
    if latest_age_hours <= 720:
        return 75
    return 45


def build_sector_news_rows(
    *,
    sector_members: dict[str, list[str]],
    interpretations_by_sector: dict[str, list[SectorNewsItem]],
    now_utc: datetime,
) -> list[dict]:
    rows: list[dict] = []

    for sector, members in sector_members.items():
        if sector in {"ETF", "Macro"}:
            continue

        sector_scope = SECTOR_SCOPE_BY_NAME.get(sector, "")
        sector_items = interpretations_by_sector.get(sector, [])
        evidence: dict[str, object] = {
            "sector": sector,
            "sector_scope": sector_scope,
            "member_symbols": members,
            "recent_interpretation_count": len(sector_items),
        }

        lane_diagnostics: dict[str, dict[str, object]] = {}
        for lane_id in ("near_term", "swing_term", "position_term"):
            lane_items: list[tuple[SectorNewsItem, float]] = []
            for item in sector_items:
                if _lane_for_horizon(item.impact_horizon) != lane_id:
                    continue
                age_hours = max(0.0, (now_utc - item.interpreted_at_utc).total_seconds() / 3600.0)
                recency_weight = _recency_weight(lane_id=lane_id, age_hours=age_hours)
                if recency_weight <= 0:
                    continue
                if item.impact_scope == sector_scope or (item.impact_scope == "semis" and sector == "Technology"):
                    relevance = 1.0
                elif item.relevance_symbols and any(symbol in set(item.relevance_symbols) for symbol in members):
                    relevance = 0.75
                else:
                    relevance = 0.0
                if relevance <= 0:
                    continue
                lane_items.append((item, relevance * recency_weight * _confidence_weight(item.confidence_label)))

            article_count = len(lane_items)
            if article_count == 0:
                lane_diagnostics[lane_id] = {
                    "lane_id": lane_id,
                    "canonical_horizon": (
                        "1to3d" if lane_id == "near_term" else "1to2w" if lane_id == "swing_term" else "2to6w"
                    ),
                    "confidence_score": 20,
                    "confidence_label": "low",
                    "coverage_state": "insufficient_evidence",
                    "freshness_score": 25,
                    "coverage_score": 25,
                    "why_code": ["sector_news_coverage_missing"],
                    "lane_data_freshness_ok": False,
                    "article_count": 0,
                    "total_weight": 0.0,
                    "net_weight": 0.0,
                    "latest_interpreted_age_hours": None,
                }
                continue

            total_weight = sum(weight for _, weight in lane_items)
            net_weight = sum(_direction_score(item.impact_direction) * weight for item, weight in lane_items)
            direction_balance = 0.0 if total_weight <= 0 else net_weight / total_weight
            latest_age_hours = min(
                max(0.0, (now_utc - item.interpreted_at_utc).total_seconds() / 3600.0) for item, _ in lane_items
            )
            intensity = min(1.0, log1p(total_weight) / log1p(4.0))
            raw_score = direction_balance * intensity * 110.0
            score_normalized = normalize_score(score_raw=raw_score, raw_scale=RAW_SCORE_SCALE)
            signal_strength = round(abs(score_normalized) / 100.0, 4)

            if score_normalized >= 15:
                signal_direction = "bullish"
            elif score_normalized <= -15:
                signal_direction = "bearish"
            else:
                signal_direction = "neutral"

            freshness_score = _freshness_score(lane_id=lane_id, latest_age_hours=latest_age_hours)
            evidence_density_score = min(100, round(article_count / 3 * 100))
            feature_agreement_score = round(abs(direction_balance) * 100)
            conflict_ratio = 1.0 - abs(direction_balance)
            stability_score = max(20, round(100 - conflict_ratio * 80))

            if article_count >= 1 and freshness_score >= 70:
                coverage_state = "sufficient_evidence"
                coverage_score = 100
            else:
                coverage_state = "thin_evidence"
                coverage_score = 49

            confidence_score = compute_confidence(
                ConfidenceInputs(
                    evidence_density_score=evidence_density_score,
                    feature_agreement_score=feature_agreement_score,
                    freshness_score=freshness_score,
                    stability_score=stability_score,
                    coverage_score=coverage_score,
                    coverage_state=coverage_state,
                    signal_strength=signal_strength,
                    informative_feature_count=3,
                    independent_informative_feature_count=3,
                )
            )
            article_count_caps = ARTICLE_COUNT_CONFIDENCE_CAP.get(article_count)
            if article_count_caps is not None:
                confidence_score = min(confidence_score, article_count_caps[lane_id])
            confidence_score = min(
                confidence_score,
                _lane_confidence_cap(lane_id=lane_id, article_count=article_count, total_weight=total_weight),
            )

            why_code: list[str] = []
            if signal_direction == "bullish":
                why_code.append("sector_news_flow_bullish")
            elif signal_direction == "bearish":
                why_code.append("sector_news_flow_bearish")
            else:
                why_code.append("sector_news_flow_mixed")
            if article_count == 1:
                why_code.append("single_sector_catalyst_only")
            elif article_count == 2:
                why_code.append("limited_sector_catalyst_breadth")
            if freshness_score < 70:
                why_code.append("news_freshness_reduced")

            lane_diagnostics[lane_id] = {
                "lane_id": lane_id,
                "canonical_horizon": (
                    "1to3d" if lane_id == "near_term" else "1to2w" if lane_id == "swing_term" else "2to6w"
                ),
                "signal_direction": signal_direction,
                "signal_strength": signal_strength,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label(confidence_score),
                "coverage_state": coverage_state,
                "freshness_score": freshness_score,
                "coverage_score": coverage_score,
                "why_code": why_code,
                "lane_data_freshness_ok": freshness_score >= 70,
                "article_count": article_count,
                "total_weight": round(total_weight, 4),
                "net_weight": round(net_weight, 4),
                "latest_interpreted_age_hours": round(latest_age_hours, 2),
                "score_raw": round(raw_score, 4),
                "score_normalized": round(score_normalized, 4),
                "evidence_density_score": evidence_density_score,
                "feature_agreement_score": feature_agreement_score,
                "stability_score": stability_score,
            }

        near_term = lane_diagnostics["near_term"]
        swing_term = lane_diagnostics["swing_term"]
        position_term = lane_diagnostics["position_term"]
        if (
            near_term["coverage_state"] == "insufficient_evidence"
            and swing_term["coverage_state"] == "insufficient_evidence"
            and position_term["coverage_state"] != "insufficient_evidence"
        ):
            primary_lane_id = "position_term"
        elif near_term["coverage_state"] == "insufficient_evidence" and swing_term["coverage_state"] != "insufficient_evidence":
            primary_lane_id = "swing_term"
        elif near_term["coverage_state"] == "insufficient_evidence" and position_term["coverage_state"] != "insufficient_evidence":
            primary_lane_id = "position_term"
        else:
            lane_strength = {
                "near_term": abs(float(near_term.get("score_normalized", 0.0) or 0.0)),
                "swing_term": abs(float(swing_term.get("score_normalized", 0.0) or 0.0)),
                "position_term": abs(float(position_term.get("score_normalized", 0.0) or 0.0)),
            }
            primary_lane_id = max(lane_strength, key=lane_strength.get)
        secondary_lane_ids = [lane_id for lane_id in ("near_term", "swing_term", "position_term") if lane_id != primary_lane_id]
        primary_lane = lane_diagnostics[primary_lane_id]

        rows.append(
            {
                "model_id": "sector_news_v1",
                "model_scope": "sector",
                "scope_id": sector,
                "horizon_primary": primary_lane["canonical_horizon"],
                "horizon_secondary": [lane_diagnostics[lane_id]["canonical_horizon"] for lane_id in secondary_lane_ids],
                "lane_primary": primary_lane_id,
                "lane_secondary": secondary_lane_ids,
                "signal_direction": primary_lane.get("signal_direction", "neutral"),
                "signal_strength": primary_lane.get("signal_strength", 0.0),
                "confidence_score": primary_lane["confidence_score"],
                "confidence_label": primary_lane["confidence_label"],
                "coverage_state": primary_lane["coverage_state"],
                "score_raw": primary_lane.get("score_raw", 0.0),
                "score_normalized": primary_lane.get("score_normalized", 0.0),
                "why_code": primary_lane["why_code"],
                "lane_diagnostics": lane_diagnostics,
                "diagnostics": {
                    "quality_inputs": {
                        "near_term_article_count": near_term["article_count"],
                        "swing_term_article_count": swing_term["article_count"],
                        "position_term_article_count": position_term["article_count"],
                    },
                },
                "evidence": evidence,
                "as_of_utc": now_utc.isoformat(),
                "data_freshness_ok": bool(primary_lane["lane_data_freshness_ok"]),
            }
        )

    return rows
