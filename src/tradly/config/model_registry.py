from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


ModelScope = Literal["market", "sector", "symbol"]
DirectionalRole = Literal["directional", "supporting"]
CoverageState = Literal["sufficient_evidence", "thin_evidence", "insufficient_evidence"]


@dataclass(frozen=True)
class ModelRegistryEntry:
    model_id: str
    scope: ModelScope
    required_inputs: tuple[str, ...]
    supported_horizons: tuple[str, ...]
    output_schema_version: int
    directional_role: DirectionalRole
    ensemble_inclusion: bool
    confidence_inclusion: bool
    base_weight_default: float
    minimum_coverage_state: CoverageState

    def to_dict(self) -> dict:
        return asdict(self)


def _entry(
    *,
    model_id: str,
    scope: ModelScope,
    required_inputs: tuple[str, ...],
    supported_horizons: tuple[str, ...],
    output_schema_version: int = 1,
    directional_role: DirectionalRole,
    ensemble_inclusion: bool,
    confidence_inclusion: bool,
    base_weight_default: float,
    minimum_coverage_state: CoverageState,
) -> ModelRegistryEntry:
    return ModelRegistryEntry(
        model_id=model_id,
        scope=scope,
        required_inputs=required_inputs,
        supported_horizons=supported_horizons,
        output_schema_version=output_schema_version,
        directional_role=directional_role,
        ensemble_inclusion=ensemble_inclusion,
        confidence_inclusion=confidence_inclusion,
        base_weight_default=base_weight_default,
        minimum_coverage_state=minimum_coverage_state,
    )


MODEL_REGISTRY: dict[str, ModelRegistryEntry] = {
    "market_regime_v1": _entry(
        model_id="market_regime_v1",
        scope="market",
        required_inputs=("market_bars", "macro_points", "news_interpretations"),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=True,
        confidence_inclusion=True,
        base_weight_default=0.15,
        minimum_coverage_state="thin_evidence",
    ),
    "sector_movement_v1": _entry(
        model_id="sector_movement_v1",
        scope="sector",
        required_inputs=("market_bars", "universe_registry", "universe_runtime_scopes"),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=True,
        confidence_inclusion=True,
        base_weight_default=0.20,
        minimum_coverage_state="thin_evidence",
    ),
    "sector_news_v1": _entry(
        model_id="sector_news_v1",
        scope="sector",
        required_inputs=("news_events", "news_symbols", "news_interpretations", "universe_registry"),
        supported_horizons=("intraday", "1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=True,
        confidence_inclusion=True,
        base_weight_default=0.15,
        minimum_coverage_state="thin_evidence",
    ),
    "symbol_movement_v1": _entry(
        model_id="symbol_movement_v1",
        scope="symbol",
        required_inputs=("market_bars", "market_regime_v1", "sector_movement_v1", "universe_runtime_scopes"),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=True,
        confidence_inclusion=True,
        base_weight_default=0.30,
        minimum_coverage_state="thin_evidence",
    ),
    "symbol_news_v1": _entry(
        model_id="symbol_news_v1",
        scope="symbol",
        required_inputs=("news_events", "news_symbols", "news_interpretations", "universe_runtime_scopes"),
        supported_horizons=("intraday", "1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=True,
        confidence_inclusion=True,
        base_weight_default=0.20,
        minimum_coverage_state="thin_evidence",
    ),
    "event_risk_v1": _entry(
        model_id="event_risk_v1",
        scope="symbol",
        required_inputs=(
            "earnings_watchlist",
            "market_regime_v1",
            "recommendation_v1",
            "market_snapshots",
            "news_events",
            "news_symbols",
            "news_interpretations",
            "universe_registry",
        ),
        supported_horizons=("intraday", "1to3d", "1to2w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.10,
        minimum_coverage_state="thin_evidence",
    ),
    "macro_news_v1": _entry(
        model_id="macro_news_v1",
        scope="market",
        required_inputs=("news_interpretations", "market_bars", "macro_points"),
        supported_horizons=("intraday", "1to3d", "1to2w"),
        directional_role="directional",
        ensemble_inclusion=False,
        confidence_inclusion=True,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "range_expectation_v1": _entry(
        model_id="range_expectation_v1",
        scope="symbol",
        required_inputs=("market_bars",),
        supported_horizons=("intraday", "1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "ensemble_v1": _entry(
        model_id="ensemble_v1",
        scope="symbol",
        required_inputs=(
            "market_regime_v1",
            "sector_movement_v1",
            "symbol_movement_v1",
            "symbol_news_v1",
            "sector_news_v1",
            "range_expectation_v1",
        ),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="directional",
        ensemble_inclusion=False,
        confidence_inclusion=True,
        base_weight_default=1.0,
        minimum_coverage_state="thin_evidence",
    ),
    "recommendation_v1": _entry(
        model_id="recommendation_v1",
        scope="symbol",
        required_inputs=("ensemble_v1",),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "recommendation_review_v1": _entry(
        model_id="recommendation_review_v1",
        scope="symbol",
        required_inputs=("recommendation_v1", "symbol_news_v1", "event_risk_v1", "universe_registry"),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "portfolio_policy_v1": _entry(
        model_id="portfolio_policy_v1",
        scope="symbol",
        required_inputs=(
            "market_regime_v1",
            "recommendation_v1",
            "recommendation_review_v1",
            "event_risk_v1",
            "freshness_snapshot",
            "portfolio_snapshot_v1",
            "universe_registry",
        ),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "recommendation_scorecard_v1": _entry(
        model_id="recommendation_scorecard_v1",
        scope="symbol",
        required_inputs=("recommendation_v1", "market_bars"),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "recommendation_scorecard_history_v1": _entry(
        model_id="recommendation_scorecard_history_v1",
        scope="symbol",
        required_inputs=("recommendation_scorecard_v1",),
        supported_horizons=("1to3d", "1to2w", "2to6w"),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
    "intraday_execution_vwap_v1": _entry(
        model_id="intraday_execution_vwap_v1",
        scope="symbol",
        required_inputs=("intraday_bars", "intraday_volume"),
        supported_horizons=("intraday",),
        directional_role="supporting",
        ensemble_inclusion=False,
        confidence_inclusion=False,
        base_weight_default=0.0,
        minimum_coverage_state="thin_evidence",
    ),
}


def list_model_registry() -> list[ModelRegistryEntry]:
    return [MODEL_REGISTRY[key] for key in sorted(MODEL_REGISTRY)]


def get_model_registry_entry(model_id: str) -> ModelRegistryEntry:
    try:
        return MODEL_REGISTRY[model_id]
    except KeyError as exc:
        raise KeyError(f"unknown_model_id:{model_id}") from exc


def get_model_registry_payload() -> dict[str, dict]:
    return {model_id: entry.to_dict() for model_id, entry in sorted(MODEL_REGISTRY.items())}
