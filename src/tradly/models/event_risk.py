from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


ALLOWED_EVENT_TYPE = {"earnings", "unknown"}
ALLOWED_EVENT_PHASE = {"inactive", "pre_event", "post_event", "digestion_window"}
ALLOWED_RESULT_TONE = {"positive", "negative", "mixed", "unclear"}
ALLOWED_REACTION_STATE = {
    "clean_positive_confirmation",
    "beat_but_rejected",
    "miss_and_breakdown",
    "mixed_uncertain",
    "macro_overwhelmed",
    "awaiting_reaction",
    "no_event_active",
}
ALLOWED_REACTION_SEVERITY = {"low", "medium", "high"}
ALLOWED_ACTION_BIAS = {"upgrade", "downgrade", "hold"}


@dataclass(frozen=True)
class EventRiskRow:
    model_id: str
    scope_id: str
    event_active: bool
    event_type: str
    event_phase: str
    event_timestamp_local: str | None
    event_source: str
    articles_considered: int
    reported_result_tone: str
    guidance_tone: str
    dominant_positive: str
    dominant_negative: str
    dominant_market_concern: str
    reaction_state: str
    reaction_severity: str
    price_reaction_pct: float | None
    move_vs_qqq_pct: float | None
    move_vs_sector_pct: float | None
    market_session_state: str
    market_regime: str
    macro_state: str
    confidence_adjustment: int
    action_bias: str
    hard_cap_buy_to_watch: bool
    reason_codes: list[str]
    summary_note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clamp_tone(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in ALLOWED_RESULT_TONE else "unclear"


def _clamp_reaction_state(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in ALLOWED_REACTION_STATE else "mixed_uncertain"


def _clamp_reaction_severity(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in ALLOWED_REACTION_SEVERITY else "medium"


def _clamp_action_bias(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in ALLOWED_ACTION_BIAS else "hold"


def build_event_risk_row(
    *,
    symbol: str,
    event_active: bool,
    event_type: str,
    event_phase: str,
    event_timestamp_local: str | None,
    event_source: str,
    articles_considered: int,
    event_semantics: dict[str, Any],
    reaction_interpretation: dict[str, Any],
    price_context: dict[str, Any],
) -> dict[str, Any]:
    if not event_active:
        return EventRiskRow(
            model_id="event_risk_v1",
            scope_id=symbol,
            event_active=False,
            event_type="earnings" if event_type in ALLOWED_EVENT_TYPE else "unknown",
            event_phase="inactive",
            event_timestamp_local=event_timestamp_local,
            event_source=event_source,
            articles_considered=articles_considered,
            reported_result_tone="unclear",
            guidance_tone="unclear",
            dominant_positive="",
            dominant_negative="",
            dominant_market_concern="",
            reaction_state="no_event_active",
            reaction_severity="low",
            price_reaction_pct=price_context.get("price_reaction_pct"),
            move_vs_qqq_pct=price_context.get("move_vs_qqq_pct"),
            move_vs_sector_pct=price_context.get("move_vs_sector_pct"),
            market_session_state=str(price_context.get("market_session_state", "")).strip(),
            market_regime=str(price_context.get("market_regime", "")).strip(),
            macro_state=str(price_context.get("macro_state", "")).strip(),
            confidence_adjustment=0,
            action_bias="hold",
            hard_cap_buy_to_watch=False,
            reason_codes=["no_event_active"],
            summary_note="No active event window for this symbol.",
        ).to_dict()

    confidence_adjustment = int(reaction_interpretation.get("confidence_adjustment", 0) or 0)
    return EventRiskRow(
        model_id="event_risk_v1",
        scope_id=symbol,
        event_active=True,
        event_type=event_type if event_type in ALLOWED_EVENT_TYPE else "unknown",
        event_phase=event_phase if event_phase in ALLOWED_EVENT_PHASE else "inactive",
        event_timestamp_local=event_timestamp_local,
        event_source=event_source,
        articles_considered=articles_considered,
        reported_result_tone=_clamp_tone(event_semantics.get("reported_result_tone")),
        guidance_tone=_clamp_tone(event_semantics.get("guidance_tone")),
        dominant_positive=str(event_semantics.get("dominant_positive", "")).strip(),
        dominant_negative=str(event_semantics.get("dominant_negative", "")).strip(),
        dominant_market_concern=str(event_semantics.get("dominant_market_concern", "")).strip(),
        reaction_state=_clamp_reaction_state(reaction_interpretation.get("reaction_state")),
        reaction_severity=_clamp_reaction_severity(reaction_interpretation.get("reaction_severity")),
        price_reaction_pct=price_context.get("price_reaction_pct"),
        move_vs_qqq_pct=price_context.get("move_vs_qqq_pct"),
        move_vs_sector_pct=price_context.get("move_vs_sector_pct"),
        market_session_state=str(price_context.get("market_session_state", "")).strip(),
        market_regime=str(price_context.get("market_regime", "")).strip(),
        macro_state=str(price_context.get("macro_state", "")).strip(),
        confidence_adjustment=confidence_adjustment,
        action_bias=_clamp_action_bias(reaction_interpretation.get("action_bias")),
        hard_cap_buy_to_watch=bool(reaction_interpretation.get("hard_cap_buy_to_watch", False)),
        reason_codes=[
            str(code).strip()
            for code in reaction_interpretation.get("reason_codes", [])
            if str(code).strip()
        ],
        summary_note=str(reaction_interpretation.get("summary_note", "")).strip()
        or str(event_semantics.get("summary_note", "")).strip(),
    ).to_dict()
