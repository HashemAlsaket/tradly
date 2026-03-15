from __future__ import annotations

from dataclasses import asdict, dataclass
from math import tanh
from typing import Literal, Sequence


CoverageState = Literal["sufficient_evidence", "thin_evidence", "insufficient_evidence"]
ConfidenceLabel = Literal["low", "medium", "high"]
LatencyClass = Literal["realtime", "delayed_tolerable", "delayed_material", "stale"]
Horizon = Literal["intraday", "1to3d", "1to2w", "2to6w"]

DELAYED_DATA_LATENCY_MINUTES = 15
QUALITY_AUDIT_MIN_ROWS = 5
STALE_FRESHNESS_CAP = 60
STALE_CONFIDENCE_CAP = 49

SIGNAL_STRENGTH_CONFIDENCE_CAPS: tuple[tuple[float, int], ...] = (
    (0.05, 54),
    (0.10, 60),
    (0.20, 60),
)

FRESHNESS_CAP_BY_HORIZON: dict[Horizon, int] = {
    "intraday": 25,
    "1to3d": 70,
    "1to2w": 85,
    "2to6w": 90,
}


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def normalize_score(*, score_raw: float, raw_scale: float, precision: int = 4) -> float:
    if raw_scale <= 0:
        raise ValueError("raw_scale_must_be_positive")
    normalized = 100.0 * tanh(score_raw / raw_scale)
    return round(clamp(normalized, -100.0, 100.0), precision)


def confidence_label(confidence_score: int) -> ConfidenceLabel:
    if confidence_score >= 70:
        return "high"
    if confidence_score >= 40:
        return "medium"
    return "low"


def latency_minutes_for_status(data_status: str | None) -> int:
    normalized = str(data_status or "").strip().upper()
    if normalized == "REALTIME":
        return 0
    if normalized == "DELAYED":
        return DELAYED_DATA_LATENCY_MINUTES
    return 0


def classify_latency(
    *,
    data_status: str | None,
    recency_ok: bool,
    horizon: Horizon,
) -> LatencyClass:
    normalized = str(data_status or "").strip().upper()
    if not recency_ok:
        return "stale"
    if normalized == "REALTIME":
        return "realtime"
    if normalized == "DELAYED":
        return "delayed_material" if horizon in {"intraday", "1to3d"} else "delayed_tolerable"
    return "stale"


@dataclass(frozen=True)
class LatencyAssessment:
    data_status: str
    market_data_latency_minutes: int
    latency_class: LatencyClass
    recency_ok: bool
    freshness_cap: int | None
    confidence_cap: int | None
    forced_coverage_state: CoverageState | None
    why_code: tuple[str, ...]

    def to_dict(self) -> dict:
        return asdict(self)


def assess_latency(
    *,
    data_status: str | None,
    recency_ok: bool,
    horizon: Horizon,
) -> LatencyAssessment:
    normalized_status = str(data_status or "").strip().upper() or "UNKNOWN"
    latency_class = classify_latency(data_status=normalized_status, recency_ok=recency_ok, horizon=horizon)
    latency_minutes = latency_minutes_for_status(normalized_status)
    why_code: list[str] = []
    freshness_cap: int | None = None
    confidence_cap: int | None = None
    forced_coverage_state: CoverageState | None = None

    if latency_class == "stale":
        freshness_cap = STALE_FRESHNESS_CAP
        confidence_cap = STALE_CONFIDENCE_CAP
        forced_coverage_state = "thin_evidence"
        why_code.append("market_data_stale")
    elif latency_class == "realtime":
        freshness_cap = None
    elif normalized_status == "DELAYED":
        freshness_cap = FRESHNESS_CAP_BY_HORIZON[horizon]
        why_code.append("market_data_delayed_intraday" if horizon == "intraday" else "market_data_delayed_15m")
        if horizon == "intraday":
            confidence_cap = 25
            forced_coverage_state = "insufficient_evidence"

    return LatencyAssessment(
        data_status=normalized_status,
        market_data_latency_minutes=latency_minutes,
        latency_class=latency_class,
        recency_ok=recency_ok,
        freshness_cap=freshness_cap,
        confidence_cap=confidence_cap,
        forced_coverage_state=forced_coverage_state,
        why_code=tuple(why_code),
    )


def apply_freshness_cap(*, freshness_score: int, assessment: LatencyAssessment) -> int:
    capped = int(clamp(freshness_score, 0, 100))
    if assessment.freshness_cap is not None:
        capped = min(capped, assessment.freshness_cap)
    return capped


@dataclass(frozen=True)
class ConfidenceInputs:
    evidence_density_score: int
    feature_agreement_score: int
    freshness_score: int
    stability_score: int
    coverage_score: int
    coverage_state: CoverageState
    signal_strength: float
    informative_feature_count: int
    independent_informative_feature_count: int


def compute_confidence(
    inputs: ConfidenceInputs,
    *,
    assessment: LatencyAssessment | None = None,
) -> int:
    base_score = round(
        0.25 * inputs.evidence_density_score
        + 0.25 * inputs.feature_agreement_score
        + 0.20 * inputs.freshness_score
        + 0.20 * inputs.stability_score
        + 0.10 * inputs.coverage_score
    )
    confidence_score = int(clamp(base_score, 0, 100))

    if inputs.coverage_state == "thin_evidence":
        confidence_score = min(confidence_score, 49)
    elif inputs.coverage_state == "insufficient_evidence":
        confidence_score = min(confidence_score, 25)

    for max_signal_strength, cap in SIGNAL_STRENGTH_CONFIDENCE_CAPS:
        if inputs.signal_strength < max_signal_strength:
            confidence_score = min(confidence_score, cap)
            break

    if inputs.informative_feature_count <= 1:
        confidence_score = min(confidence_score, 65)
    elif inputs.independent_informative_feature_count < 3:
        confidence_score = min(confidence_score, 85)

    if assessment is not None and assessment.confidence_cap is not None:
        confidence_score = min(confidence_score, assessment.confidence_cap)

    return int(clamp(confidence_score, 0, 100))


@dataclass(frozen=True)
class ArtifactAuditResult:
    status: Literal["pass", "fail"]
    failure_reasons: tuple[str, ...]
    summary: dict[str, int | float]

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "failure_reasons": list(self.failure_reasons),
            "summary": self.summary,
        }


def _float_value(row: dict, key: str) -> float | None:
    value = row.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _confidence_value(row: dict) -> int | None:
    value = row.get("confidence_score")
    if isinstance(value, (int, float)):
        return int(round(value))
    return None


def _status_values(row: dict) -> set[str]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    raw_status = evidence.get("data_status", row.get("data_status", ""))
    values: list[str] = []
    if isinstance(raw_status, str):
        values = [raw_status]
    elif isinstance(raw_status, (list, tuple, set)):
        values = [str(item) for item in raw_status]
    return {value.strip().upper() for value in values if str(value).strip()}


def audit_model_artifact(
    rows: Sequence[dict],
    *,
    allow_extreme_why_codes: Sequence[str] = ("extreme_event", "gap_extreme", "event_shock"),
) -> ArtifactAuditResult:
    row_count = len(rows)
    failures: list[str] = []

    clipped_extreme_count = 0
    confidence_90_plus_count = 0
    same_confidence_count = 0
    weak_score_high_confidence_count = 0
    delayed_missing_latency_count = 0
    delayed_intraday_sufficient_count = 0
    insufficient_non_neutral_count = 0

    confidence_counts: dict[int, int] = {}

    for row in rows:
        score = _float_value(row, "score_normalized")
        confidence = _confidence_value(row)
        why_code = row.get("why_code") if isinstance(row.get("why_code"), list) else []
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        data_statuses = _status_values(row)
        latency_class = str(evidence.get("latency_class", row.get("latency_class", ""))).strip()
        horizon_primary = str(row.get("horizon_primary", "")).strip()
        coverage_state = str(row.get("coverage_state", "")).strip()
        signal_direction = str(row.get("signal_direction", "")).strip()

        if score is not None and abs(score) >= 95:
            if not any(code in allow_extreme_why_codes for code in why_code):
                clipped_extreme_count += 1

        if confidence is not None:
            if confidence >= 90:
                confidence_90_plus_count += 1
            confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1

        if score is not None and confidence is not None:
            if abs(score) < 5 and confidence >= 55:
                weak_score_high_confidence_count += 1
            elif abs(score) < 10 and confidence >= 70:
                weak_score_high_confidence_count += 1

        if "DELAYED" in data_statuses and (
            "market_data_latency_minutes" not in evidence or "latency_class" not in evidence
        ):
            delayed_missing_latency_count += 1

        if latency_class == "delayed_material" and horizon_primary == "intraday" and coverage_state == "sufficient_evidence":
            delayed_intraday_sufficient_count += 1

        if coverage_state == "insufficient_evidence" and signal_direction not in {"", "neutral"}:
            insufficient_non_neutral_count += 1

    if confidence_counts:
        same_confidence_count = max(confidence_counts.values())

    if row_count >= QUALITY_AUDIT_MIN_ROWS:
        if clipped_extreme_count / row_count > 0.15:
            failures.append("score_saturation_excessive")
        if confidence_90_plus_count / row_count > 0.40:
            failures.append("confidence_cluster_high")
        if same_confidence_count / row_count > 0.50:
            failures.append("confidence_clustering_excessive")

    if delayed_missing_latency_count > 0:
        failures.append("delayed_data_missing_latency_metadata")
    if delayed_intraday_sufficient_count > 0:
        failures.append("delayed_intraday_claims_sufficient_evidence")
    if insufficient_non_neutral_count > 0:
        failures.append("insufficient_evidence_non_neutral")
    if weak_score_high_confidence_count > 0:
        failures.append("score_confidence_consistency_violation")

    status: Literal["pass", "fail"] = "fail" if failures else "pass"
    return ArtifactAuditResult(
        status=status,
        failure_reasons=tuple(failures),
        summary={
            "row_count": row_count,
            "clipped_extreme_count": clipped_extreme_count,
            "confidence_90_plus_count": confidence_90_plus_count,
            "same_confidence_count": same_confidence_count,
            "weak_score_high_confidence_count": weak_score_high_confidence_count,
            "delayed_missing_latency_count": delayed_missing_latency_count,
            "delayed_intraday_sufficient_count": delayed_intraday_sufficient_count,
            "insufficient_non_neutral_count": insufficient_non_neutral_count,
        },
    )
