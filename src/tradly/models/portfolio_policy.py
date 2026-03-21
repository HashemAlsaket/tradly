from __future__ import annotations

from dataclasses import dataclass
from typing import Any


MEANINGFUL_WEIGHT_DELTA = 0.005
STRICT_POLICIES = {"market_hours_strict"}
LONG_ACTIONS = {"Buy", "Watch Buy"}
NON_LONG_ACTIONS = {"Sell/Trim", "Watch Trim", "Hold", "Hold/Watch", "Defer", "Defer Buy", "Defer Trim", "Blocked", "Unknown"}
ACTIVE_ORDER_STATUSES = {"new", "open", "working", "accepted", "pending", "queued", "submitted"}

MODE_CONSTRAINTS: dict[str, dict[str, float | int]] = {
    "risk_off": {
        "gross_long_cap": 0.35,
        "single_name_cap": 0.08,
        "theme_cap": 0.18,
        "tactical_cap": 0.12,
        "max_active_positions": 5,
    },
    "neutral": {
        "gross_long_cap": 0.50,
        "single_name_cap": 0.08,
        "theme_cap": 0.18,
        "tactical_cap": 0.20,
        "max_active_positions": 8,
    },
    "risk_on": {
        "gross_long_cap": 0.75,
        "single_name_cap": 0.10,
        "theme_cap": 0.25,
        "tactical_cap": 0.25,
        "max_active_positions": 12,
    },
}

HORIZON_MAP = {
    "1to3d": "tactical_1_3d",
    "1to2w": "swing_1_2w",
    "2to6w": "position_2_6w",
}

BUCKET_MULTIPLIER = {
    "Buy": 1.0,
    "Watch Buy": 0.35,
}

MARKET_MULTIPLIER = {
    "risk_off": 0.5,
    "neutral": 0.7,
    "risk_on": 1.2,
}

TIER_MULTIPLIER = {
    "tier_1_best": 1.0,
    "tier_2_conditional": 0.65,
    "tier_3_probe": 0.40,
    "tier_blocked": 0.0,
}

TIER_CLAMPS = {
    "tier_1_best": {"starter": 0.03, "max": 0.08},
    "tier_2_conditional": {"starter": 0.015, "max": 0.04},
    "tier_3_probe": {"starter": 0.01, "max": 0.025},
    "tier_blocked": {"starter": 0.0, "max": 0.0},
}


@dataclass(frozen=True)
class SnapshotValidation:
    valid: bool
    failure_reasons: list[str]
    position_count: int
    open_order_count: int
    unmanaged_symbols: list[str]


@dataclass(frozen=True)
class OpenOrderSummary:
    active_by_symbol: dict[str, list[dict[str, Any]]]
    reserved_buy_notional: float


def validate_portfolio_snapshot(snapshot: dict[str, Any], *, active_universe: set[str]) -> SnapshotValidation:
    failure_reasons: list[str] = []
    unmanaged_symbols: list[str] = []
    required_top = {"as_of_utc", "base_currency", "cash_available", "net_liquidation_value", "positions", "open_orders"}
    missing_top = sorted(key for key in required_top if key not in snapshot)
    if missing_top:
        failure_reasons.append(f"missing_top_level:{','.join(missing_top)}")

    nav = float(snapshot.get("net_liquidation_value", 0) or 0)
    if nav <= 0:
        failure_reasons.append("invalid_net_liquidation_value")

    positions = snapshot.get("positions", [])
    if not isinstance(positions, list):
        failure_reasons.append("positions_not_list")
        positions = []
    open_orders = snapshot.get("open_orders", [])
    if not isinstance(open_orders, list):
        failure_reasons.append("open_orders_not_list")
        open_orders = []

    for position in positions:
        if not isinstance(position, dict):
            failure_reasons.append("position_not_object")
            continue
        missing_fields = sorted(
            field
            for field in ("symbol", "shares", "market_value", "cost_basis", "unrealized_pnl")
            if field not in position
        )
        if missing_fields:
            failure_reasons.append(f"position_missing_fields:{','.join(missing_fields)}")
        symbol = str(position.get("symbol", "")).strip().upper()
        if symbol and symbol not in active_universe:
            unmanaged_symbols.append(symbol)

    for order in open_orders:
        if not isinstance(order, dict):
            failure_reasons.append("open_order_not_object")
            continue
        missing_fields = sorted(
            field for field in ("symbol", "side", "shares", "status") if field not in order
        )
        if missing_fields:
            failure_reasons.append(f"open_order_missing_fields:{','.join(missing_fields)}")

    return SnapshotValidation(
        valid=not failure_reasons,
        failure_reasons=failure_reasons,
        position_count=len(positions),
        open_order_count=len(open_orders),
        unmanaged_symbols=sorted(set(unmanaged_symbols)),
    )


def _score_multiplier(score: float) -> float:
    if score >= 80:
        return 1.2
    if score >= 70:
        return 1.0
    if score >= 60:
        return 0.8
    if score >= 50:
        return 0.6
    return 0.0


def _confidence_multiplier(confidence: int) -> float:
    if confidence >= 80:
        return 1.1
    if confidence >= 70:
        return 1.0
    if confidence >= 55:
        return 0.85
    return 0.6


def _normalize_score(score_normalized: float) -> float:
    return abs(float(score_normalized or 0.0))


def _theme_from_symbol_meta(symbol_meta: dict[str, Any], symbol: str) -> str:
    roles = {str(role).strip().lower() for role in symbol_meta.get("roles", []) if str(role).strip()}
    sector = str(symbol_meta.get("sector", "")).strip()
    industry = str(symbol_meta.get("industry", "")).strip()
    asset_type = str(symbol_meta.get("asset_type", "")).strip().lower()

    if "semis" in roles:
        return "semis_ai_beta"
    if sector == "Healthcare":
        return "healthcare"
    if sector == "Financial Services":
        return "financials_rates"
    if sector == "Energy":
        return "energy"
    if sector in {"Utilities", "Consumer Defensive"}:
        return "defensives"
    if asset_type == "etf" and (
        "index" in industry.lower() or symbol in {"SPY", "QQQ", "IWM", "DIA", "VTI"}
    ):
        return "broad_index_etfs"
    if sector:
        return f"sector_{sector.lower().replace(' ', '_')}"
    if industry:
        return f"industry_{industry.lower().replace(' ', '_').replace('-', '_')}"
    return f"symbol_{symbol.lower()}"


def _portfolio_mode(
    *,
    market_row: dict[str, Any],
    freshness_snapshot: dict[str, Any],
) -> tuple[str, list[str]]:
    freshness = freshness_snapshot.get("freshness", {}) if isinstance(freshness_snapshot, dict) else {}
    metrics = freshness.get("metrics", {}) if isinstance(freshness, dict) else {}
    checks = freshness.get("checks", []) if isinstance(freshness, dict) else []
    reasons: list[str] = []
    freshness_policy = str(freshness.get("freshness_policy", "")).strip()
    short_ready = bool(metrics.get("short_horizon_execution_ready", False))
    medium_ready = bool(metrics.get("medium_horizon_thesis_usable", False))
    direction = str(market_row.get("signal_direction", "")).strip().lower()
    confidence = int(market_row.get("confidence_score", 0) or 0)
    evidence = market_row.get("evidence", {}) if isinstance(market_row.get("evidence"), dict) else {}
    macro_hostility = evidence.get("macro_hostility", {}) if isinstance(evidence.get("macro_hostility"), dict) else {}
    macro_state = str(macro_hostility.get("macro_state", "")).strip().lower()

    strict_blocker = False
    if freshness_policy in STRICT_POLICIES:
        for check in checks:
            if not isinstance(check, dict):
                continue
            if str(check.get("status", "")).strip().upper() == "FAIL":
                strict_blocker = True
                reasons.append(f"strict_blocker:{check.get('name', 'unknown')}")
        if not short_ready:
            strict_blocker = True
            reasons.append("short_horizon_not_ready")

    if strict_blocker or (macro_state == "macro_unstable" and not short_ready):
        reasons.append("macro_unstable_short_not_ready")
        return "risk_off", sorted(set(reasons))
    if direction == "bearish" and confidence >= 75 and not short_ready:
        reasons.append("strong_bearish_regime_weak_short_horizon")
        return "risk_off", sorted(set(reasons))
    if macro_state not in {"macro_unstable", "risk_off", "unknown"} and short_ready and medium_ready:
        reasons.append("supportive_macro_and_freshness")
        return "risk_on", sorted(set(reasons))
    reasons.append("mixed_or_macro_unstable")
    return "neutral", sorted(set(reasons))


def _tier_for_symbol(
    *,
    recommendation_row: dict[str, Any],
    review_row: dict[str, Any] | None,
    event_risk_row: dict[str, Any] | None,
    freshness_snapshot: dict[str, Any],
) -> tuple[str, list[str]]:
    action = str(recommendation_row.get("recommended_action", "")).strip()
    recommendation_class = str(recommendation_row.get("recommendation_class", "")).strip().lower()
    confidence = int(recommendation_row.get("confidence_score", 0) or 0)
    execution_ready = bool(recommendation_row.get("execution_ready", True))
    source_state = str(recommendation_row.get("source_state", "")).strip().lower()
    review_disposition = str((review_row or {}).get("review_disposition", "")).strip().lower()
    event_active = bool((event_risk_row or {}).get("event_active", False))
    event_action_bias = str((event_risk_row or {}).get("action_bias", "")).strip().lower()
    event_reaction_severity = str((event_risk_row or {}).get("reaction_severity", "")).strip().lower()
    event_hard_cap = bool((event_risk_row or {}).get("hard_cap_buy_to_watch", False))
    reasons: list[str] = []

    freshness = freshness_snapshot.get("freshness", {}) if isinstance(freshness_snapshot, dict) else {}
    freshness_policy = str(freshness.get("freshness_policy", "")).strip()

    if action not in LONG_ACTIONS:
        reasons.append("non_long_bucket")
        return "tier_blocked", reasons
    if event_active and event_action_bias == "downgrade" and (event_hard_cap or event_reaction_severity == "high"):
        reasons.append("event_reaction_damage")
        return "tier_blocked", reasons
    if source_state == "blocked" or not execution_ready and freshness_policy in STRICT_POLICIES:
        reasons.append("strict_session_execution_blocker")
        return "tier_blocked", reasons
    if action == "Watch Buy":
        reasons.append("watch_buy_probe")
        return "tier_3_probe", reasons
    if confidence <= 55:
        reasons.append("confidence_too_low_for_tier_1")
        return "tier_2_conditional", reasons
    if event_active and event_action_bias == "downgrade" and event_reaction_severity == "medium":
        reasons.append("event_reaction_caution")
        return "tier_2_conditional", reasons

    strong_long = recommendation_class in {"aligned_long", "mixed_strong_long"}
    if (
        action == "Buy"
        and strong_long
        and confidence >= 70
        and review_disposition == "promote"
    ):
        reasons.append("promoted_strong_long")
        return "tier_1_best", reasons

    reasons.append("conditional_long")
    return "tier_2_conditional", reasons


def _theme_penalty(*, current_theme_weight: float, theme_cap: float) -> float:
    if theme_cap <= 0:
        return 0.0
    utilization = current_theme_weight / theme_cap
    if utilization > 1.0:
        return 0.0
    if utilization > 0.8:
        return 0.5
    if utilization >= 0.5:
        return 0.8
    return 1.0


def _starter_target_for_tier(tier: str) -> float:
    return float(TIER_CLAMPS.get(tier, TIER_CLAMPS["tier_blocked"])["starter"])


def _max_target_for_tier(tier: str) -> float:
    return float(TIER_CLAMPS.get(tier, TIER_CLAMPS["tier_blocked"])["max"])


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _position_by_symbol(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    positions = snapshot.get("positions", []) if isinstance(snapshot, dict) else []
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(positions, list):
        return out
    for position in positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol", "")).strip().upper()
        if symbol:
            out[symbol] = position
    return out


def _estimate_order_notional(order: dict[str, Any], position: dict[str, Any] | None) -> float:
    shares = max(_coerce_float(order.get("shares", 0.0)), 0.0)
    limit_price = _coerce_float(order.get("limit_price", 0.0))
    if shares <= 0:
        return 0.0
    if limit_price > 0:
        return shares * limit_price
    if isinstance(position, dict):
        held_shares = _coerce_float(position.get("shares", 0.0))
        held_value = _coerce_float(position.get("market_value", 0.0))
        if held_shares > 0 and held_value > 0:
            return shares * (held_value / held_shares)
    return 0.0


def _summarize_open_orders(snapshot: dict[str, Any], *, positions_by_symbol: dict[str, dict[str, Any]]) -> OpenOrderSummary:
    active_by_symbol: dict[str, list[dict[str, Any]]] = {}
    reserved_buy_notional = 0.0
    open_orders = snapshot.get("open_orders", []) if isinstance(snapshot, dict) else []
    if not isinstance(open_orders, list):
        return OpenOrderSummary(active_by_symbol=active_by_symbol, reserved_buy_notional=reserved_buy_notional)
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        symbol = str(order.get("symbol", "")).strip().upper()
        status = str(order.get("status", "")).strip().lower()
        if not symbol or status not in ACTIVE_ORDER_STATUSES:
            continue
        active_by_symbol.setdefault(symbol, []).append(order)
        side = str(order.get("side", "")).strip().lower()
        if side in {"buy", "add"}:
            reserved_buy_notional += _estimate_order_notional(order, positions_by_symbol.get(symbol))
    return OpenOrderSummary(
        active_by_symbol=active_by_symbol,
        reserved_buy_notional=reserved_buy_notional,
    )


def build_portfolio_policy(
    *,
    market_regime_payload: dict[str, Any],
    recommendation_payload: dict[str, Any],
    review_payload: dict[str, Any],
    event_risk_payload: dict[str, Any],
    freshness_snapshot: dict[str, Any],
    portfolio_snapshot: dict[str, Any],
    universe_registry: dict[str, Any],
    now_utc,
) -> dict[str, Any]:
    recommendation_rows = recommendation_payload.get("rows", []) if isinstance(recommendation_payload, dict) else []
    review_rows = review_payload.get("rows", []) if isinstance(review_payload, dict) else []
    event_risk_rows = event_risk_payload.get("rows", []) if isinstance(event_risk_payload, dict) else []
    market_rows = market_regime_payload.get("rows", []) if isinstance(market_regime_payload, dict) else []

    if not isinstance(recommendation_rows, list):
        recommendation_rows = []
    if not isinstance(review_rows, list):
        review_rows = []
    if not isinstance(event_risk_rows, list):
        event_risk_rows = []
    if not isinstance(market_rows, list):
        market_rows = []

    symbol_meta_lookup: dict[str, dict[str, Any]] = {}
    symbols_section = universe_registry.get("symbols", []) if isinstance(universe_registry, dict) else []
    if isinstance(symbols_section, list):
        for item in symbols_section:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            if symbol:
                symbol_meta_lookup[symbol] = item

    active_universe = {
        symbol
        for symbol, meta in symbol_meta_lookup.items()
        if bool(meta.get("active", False)) and bool(meta.get("model", False))
    }
    snapshot_validation = validate_portfolio_snapshot(portfolio_snapshot, active_universe=active_universe)
    if not snapshot_validation.valid:
        return {
            "input_audit": {
                "status": "fail",
                "failure_reasons": snapshot_validation.failure_reasons,
                "snapshot_position_count": snapshot_validation.position_count,
                "snapshot_open_order_count": snapshot_validation.open_order_count,
            },
            "quality_audit": {"status": "fail", "failure_reasons": ["invalid_portfolio_snapshot"]},
            "rows": [],
        }

    review_by_symbol = {
        str(row.get("scope_id", "")).strip().upper(): row
        for row in review_rows
        if isinstance(row, dict) and str(row.get("scope_id", "")).strip()
    }
    recommendation_by_symbol = {
        str(row.get("scope_id", row.get("symbol", ""))).strip().upper(): row
        for row in recommendation_rows
        if isinstance(row, dict) and str(row.get("scope_id", row.get("symbol", ""))).strip()
    }
    event_risk_by_symbol = {
        str(row.get("scope_id", "")).strip().upper(): row
        for row in event_risk_rows
        if isinstance(row, dict) and str(row.get("scope_id", "")).strip()
    }
    position_by_symbol = _position_by_symbol(portfolio_snapshot)
    open_order_summary = _summarize_open_orders(
        portfolio_snapshot,
        positions_by_symbol=position_by_symbol,
    )

    market_row = market_rows[0] if market_rows and isinstance(market_rows[0], dict) else {}
    portfolio_mode, mode_reasons = _portfolio_mode(market_row=market_row, freshness_snapshot=freshness_snapshot)
    constraints = dict(MODE_CONSTRAINTS[portfolio_mode])
    nav = _coerce_float(portfolio_snapshot.get("net_liquidation_value", 0.0))
    cash_available = _coerce_float(portfolio_snapshot.get("cash_available", 0.0))
    reserved_buy_notional = min(open_order_summary.reserved_buy_notional, cash_available)
    effective_cash_available = max(cash_available - reserved_buy_notional, 0.0)
    reserved_buy_weight = (reserved_buy_notional / nav) if nav > 0 else 0.0
    effective_gross_cap = max(float(constraints["gross_long_cap"]) - reserved_buy_weight, 0.0)
    base_weight = effective_gross_cap / float(constraints["max_active_positions"]) if float(constraints["max_active_positions"]) > 0 else 0.0

    current_theme_weights: dict[str, float] = {}
    current_horizon_weights: dict[str, float] = {}
    current_gross_long = 0.0
    for symbol, position in position_by_symbol.items():
        weight = max(0.0, _coerce_float(position.get("market_value")) / nav) if nav > 0 else 0.0
        meta = symbol_meta_lookup.get(symbol, {})
        theme = _theme_from_symbol_meta(meta, symbol)
        current_theme_weights[theme] = current_theme_weights.get(theme, 0.0) + weight
        recommended_horizon = (
            str(position.get("horizon_override", "")).strip()
            or HORIZON_MAP.get(
                str((recommendation_by_symbol.get(symbol, {}) or {}).get("recommended_horizon", "")).strip(),
                "unmanaged",
            )
        )
        horizon = recommended_horizon or "unmanaged"
        current_horizon_weights[horizon] = current_horizon_weights.get(horizon, 0.0) + weight
        current_gross_long += max(weight, 0.0)

    provisional_rows: list[dict[str, Any]] = []
    raw_total = 0.0
    for recommendation_row in recommendation_rows:
        if not isinstance(recommendation_row, dict):
            continue
        symbol = str(recommendation_row.get("scope_id", recommendation_row.get("symbol", ""))).strip().upper()
        if not symbol:
            continue
        review_row = review_by_symbol.get(symbol)
        event_risk_row = event_risk_by_symbol.get(symbol)
        meta = symbol_meta_lookup.get(symbol, {})
        theme = _theme_from_symbol_meta(meta, symbol)
        horizon_bucket = HORIZON_MAP.get(str(recommendation_row.get("recommended_horizon", "")).strip(), "unknown")
        current_position = position_by_symbol.get(symbol, {})
        current_weight = max(0.0, _coerce_float(current_position.get("market_value")) / nav) if nav > 0 else 0.0
        current_theme_weight = current_theme_weights.get(theme, 0.0)
        tier, tier_reason_codes = _tier_for_symbol(
            recommendation_row=recommendation_row,
            review_row=review_row,
            event_risk_row=event_risk_row,
            freshness_snapshot=freshness_snapshot,
        )
        score = _normalize_score(_coerce_float(recommendation_row.get("score_normalized")))
        confidence = int(recommendation_row.get("confidence_score", 0) or 0)
        action = str(recommendation_row.get("recommended_action", "")).strip()
        bucket_mult = BUCKET_MULTIPLIER.get(action, 0.0)
        theme_penalty = _theme_penalty(
            current_theme_weight=current_theme_weight,
            theme_cap=float(constraints["theme_cap"]),
        )
        raw_target = (
            base_weight
            * bucket_mult
            * _score_multiplier(score)
            * _confidence_multiplier(confidence)
            * MARKET_MULTIPLIER[portfolio_mode]
            * TIER_MULTIPLIER[tier]
            * theme_penalty
            * 1.0
        )
        tier_clamped = min(raw_target, _max_target_for_tier(tier))
        if current_weight <= 0 and tier_clamped > 0:
            tier_clamped = min(tier_clamped, _starter_target_for_tier(tier))
        raw_total += max(tier_clamped, 0.0)
        provisional_rows.append(
            {
                "symbol": symbol,
                "recommendation_row": recommendation_row,
                "review_row": review_row or {},
                "event_risk_row": event_risk_row or {},
                "current_position": current_position,
                "current_weight": current_weight,
                "theme_id": theme,
                "horizon_bucket": horizon_bucket,
                "idea_tier": tier,
                "tier_reason_codes": tier_reason_codes,
                "theme_penalty": theme_penalty,
                "raw_target_weight": max(tier_clamped, 0.0),
            }
        )

    gross_cap = effective_gross_cap
    scale = min(1.0, (gross_cap / raw_total)) if raw_total > 0 else 1.0
    working_theme_weights = dict(current_theme_weights)
    working_horizon_weights = dict(current_horizon_weights)
    rows: list[dict[str, Any]] = []
    active_target_count = sum(
        1
        for item in provisional_rows
        if float(item.get("current_weight", 0.0) or 0.0) > 0
        and str((item.get("recommendation_row") or {}).get("recommended_action", "")).strip() in LONG_ACTIONS
    )
    max_active_positions = int(constraints["max_active_positions"])
    sorted_rows = sorted(provisional_rows, key=lambda row: row["raw_target_weight"], reverse=True)
    for item in sorted_rows:
        symbol = str(item["symbol"])
        target_weight = item["raw_target_weight"] * scale
        tier = str(item["idea_tier"])
        target_weight = min(target_weight, _max_target_for_tier(tier))
        target_weight = min(target_weight, float(constraints["single_name_cap"]))

        theme = str(item["theme_id"])
        horizon_bucket = str(item["horizon_bucket"])
        current_weight = float(item["current_weight"])
        current_horizon_bucket = (
            str((item["current_position"] or {}).get("horizon_override", "")).strip()
            or HORIZON_MAP.get(
                str((item["recommendation_row"] or {}).get("recommended_horizon", "")).strip(),
                "unmanaged",
            )
        )

        active_orders = open_order_summary.active_by_symbol.get(symbol, [])
        has_active_buy_order = any(str(order.get("side", "")).strip().lower() in {"buy", "add"} for order in active_orders)
        has_active_sell_order = any(str(order.get("side", "")).strip().lower() in {"sell", "trim", "exit"} for order in active_orders)

        projected_theme = working_theme_weights.get(theme, 0.0) - current_weight + target_weight
        if projected_theme > float(constraints["theme_cap"]):
            remaining = max(float(constraints["theme_cap"]) - (working_theme_weights.get(theme, 0.0) - current_weight), 0.0)
            target_weight = min(target_weight, remaining)
        projected_horizon = working_horizon_weights.get(horizon_bucket, 0.0) - (
            current_weight if current_horizon_bucket == horizon_bucket else 0.0
        ) + target_weight
        if horizon_bucket == "tactical_1_3d" and projected_horizon > float(constraints["tactical_cap"]):
            remaining = max(
                float(constraints["tactical_cap"])
                - (working_horizon_weights.get(horizon_bucket, 0.0) - (current_weight if current_horizon_bucket == horizon_bucket else 0.0)),
                0.0,
            )
            target_weight = min(target_weight, remaining)

        if current_weight <= 0 and target_weight > 0:
            if active_target_count >= max_active_positions:
                target_weight = 0.0
            else:
                active_target_count += 1

        working_theme_weights[theme] = working_theme_weights.get(theme, 0.0) - current_weight + target_weight
        if current_horizon_bucket:
            working_horizon_weights[current_horizon_bucket] = max(
                working_horizon_weights.get(current_horizon_bucket, 0.0) - current_weight,
                0.0,
            )
        working_horizon_weights[horizon_bucket] = working_horizon_weights.get(horizon_bucket, 0.0) + target_weight

        recommendation_row = item["recommendation_row"]
        review_row = item["review_row"]
        event_risk_row = item["event_risk_row"]
        weight_delta = target_weight - current_weight
        current_position = item["current_position"] if isinstance(item["current_position"], dict) else {}
        is_held = current_weight > 0
        policy_blockers: list[str] = []
        recommendation_action = str(recommendation_row.get("recommended_action", "")).strip()
        unmanaged = symbol in snapshot_validation.unmanaged_symbols
        if unmanaged:
            policy_blockers.append("unmanaged_holding")
        if item["theme_penalty"] == 0.0 and recommendation_action in {"Buy", "Watch Buy"}:
            policy_blockers.append("theme_cap_exhausted")
        if item["idea_tier"] == "tier_blocked":
            policy_blockers.append("tier_blocked")
        if bool(event_risk_row.get("event_active", False)) and str(event_risk_row.get("action_bias", "")).strip().lower() == "downgrade":
            policy_blockers.append("event_reaction_damage")
        if current_weight <= 0 and item["raw_target_weight"] > 0 and target_weight <= 0:
            policy_blockers.append("max_active_positions_reached")
        if has_active_buy_order and recommendation_action in {"Buy", "Watch Buy"}:
            policy_blockers.append("active_buy_order_exists")
        if has_active_sell_order and recommendation_action not in {"Buy", "Watch Buy"}:
            policy_blockers.append("active_sell_order_exists")

        action_recommendation = "do_not_trade"
        if unmanaged:
            action_recommendation = "do_not_trade"
        elif has_active_buy_order and recommendation_action in {"Buy", "Watch Buy"}:
            action_recommendation = "do_not_trade"
        elif has_active_sell_order and recommendation_action not in {"Buy", "Watch Buy"}:
            action_recommendation = "do_not_trade"
        elif is_held and recommendation_action not in {"Buy", "Watch Buy"}:
            action_recommendation = "exit"
            target_weight = 0.0
            weight_delta = -current_weight
        elif is_held and current_weight - target_weight >= MEANINGFUL_WEIGHT_DELTA:
            action_recommendation = "trim"
        elif current_weight == 0 and target_weight >= MEANINGFUL_WEIGHT_DELTA and not policy_blockers:
            action_recommendation = "buy"
        elif is_held and weight_delta >= MEANINGFUL_WEIGHT_DELTA and recommendation_action == "Buy" and not policy_blockers:
            action_recommendation = "add"
        elif is_held and abs(weight_delta) < MEANINGFUL_WEIGHT_DELTA and recommendation_action in {"Buy", "Watch Buy"}:
            action_recommendation = "hold"
        elif is_held and target_weight <= 0:
            action_recommendation = "exit"

        freshness = freshness_snapshot.get("freshness", {}) if isinstance(freshness_snapshot, dict) else {}
        session_state = str(freshness.get("market_session_state", "")).strip().lower()
        execution_state = "cash_session_actionable"
        if session_state != "market_hours":
            if action_recommendation in {"buy", "add", "trim"}:
                execution_state = "deferred_to_next_cash_session"
            elif action_recommendation == "exit":
                execution_state = "urgent_exit"
            else:
                execution_state = "informational"

        delta_notional = weight_delta * nav
        policy_reason_codes = sorted(
            set(
                list(item["tier_reason_codes"])
                + [str(review_row.get("review_reason_code", "")).strip()]
                + [str(recommendation_row.get("primary_reason_code", "")).strip()]
                + [str(event_risk_row.get("reaction_state", "")).strip()]
                )
            - {""}
        )
        rows.append(
            {
                "symbol": symbol,
                "recommended_action": recommendation_action,
                "recommended_horizon": recommendation_row.get("recommended_horizon"),
                "review_disposition": review_row.get("review_disposition"),
                "review_bucket": review_row.get("review_bucket"),
                "event_active": bool(event_risk_row.get("event_active", False)),
                "event_reaction_state": event_risk_row.get("reaction_state"),
                "event_reaction_severity": event_risk_row.get("reaction_severity"),
                "recommendation_class": recommendation_row.get("recommendation_class"),
                "confidence_score": recommendation_row.get("confidence_score"),
                "score_normalized": recommendation_row.get("score_normalized"),
                "portfolio_mode": portfolio_mode,
                "current_weight": round(current_weight, 6),
                "target_weight": round(max(target_weight, 0.0), 6),
                "weight_delta": round((max(target_weight, 0.0) - current_weight), 6),
                "current_notional": round(current_weight * nav, 2),
                "target_notional": round(max(target_weight, 0.0) * nav, 2),
                "delta_notional": round((max(target_weight, 0.0) - current_weight) * nav, 2),
                "action_recommendation": action_recommendation,
                "action_size_recommendation": (
                    f"{action_recommendation} {abs((max(target_weight, 0.0) - current_weight))*100:.1f}% (~${abs(delta_notional):,.0f})"
                    if action_recommendation in {"buy", "add", "trim", "exit"}
                    else "hold near target" if action_recommendation == "hold" else "no meaningful change"
                ),
                "action_execution_state": execution_state,
                "deferred_to_next_cash_session": execution_state == "deferred_to_next_cash_session",
                "idea_tier": tier,
                "theme_id": theme,
                "horizon_bucket": horizon_bucket,
                "policy_reason_codes": policy_reason_codes,
                "policy_blockers": sorted(set(policy_blockers)),
                "starter_allowed": action_recommendation == "buy" and execution_state != "informational",
                "add_allowed": action_recommendation == "add" and execution_state != "informational",
                "trim_required": action_recommendation == "trim",
                "exit_required": action_recommendation == "exit",
                "managed_symbol": not unmanaged,
                "current_shares": _coerce_float(current_position.get("shares", 0.0)),
                "active_open_order_count": len(active_orders),
                "why_code": recommendation_row.get("why_code", []),
            }
        )

    for symbol in sorted(snapshot_validation.unmanaged_symbols):
        position = position_by_symbol.get(symbol, {})
        current_weight = max(0.0, _coerce_float(position.get("market_value")) / nav) if nav > 0 else 0.0
        rows.append(
            {
                "symbol": symbol,
                "recommended_action": "Hold/Watch",
                "recommended_horizon": position.get("horizon_override") or "UNSET",
                "review_disposition": "blocked",
                "review_bucket": "blocked",
                "recommendation_class": "unmanaged_holding",
                "confidence_score": 0,
                "score_normalized": 0.0,
                "portfolio_mode": portfolio_mode,
                "current_weight": round(current_weight, 6),
                "target_weight": 0.0,
                "weight_delta": round(-current_weight, 6),
                "current_notional": round(current_weight * nav, 2),
                "target_notional": 0.0,
                "delta_notional": round(-current_weight * nav, 2),
                "action_recommendation": "do_not_trade",
                "action_size_recommendation": "unmanaged holding",
                "action_execution_state": "informational",
                "deferred_to_next_cash_session": False,
                "idea_tier": "tier_blocked",
                "theme_id": _theme_from_symbol_meta(symbol_meta_lookup.get(symbol, {}), symbol),
                "horizon_bucket": str(position.get("horizon_override", "")).strip() or "unmanaged",
                "policy_reason_codes": ["unmanaged_symbol"],
                "policy_blockers": ["unmanaged_holding"],
                "starter_allowed": False,
                "add_allowed": False,
                "trim_required": False,
                "exit_required": False,
                "managed_symbol": False,
                "current_shares": _coerce_float(position.get("shares", 0.0)),
                "why_code": [],
            }
        )

    target_theme_weights = dict(working_theme_weights)
    target_horizon_weights = dict(working_horizon_weights)
    target_gross_long = sum(max(float(weight), 0.0) for weight in target_theme_weights.values())
    theme_violations = {theme: weight for theme, weight in target_theme_weights.items() if weight > float(constraints["theme_cap"])}
    horizon_violations = {
        bucket: weight
        for bucket, weight in target_horizon_weights.items()
        if bucket == "tactical_1_3d" and weight > float(constraints["tactical_cap"])
    }
    single_name_violations = [row["symbol"] for row in rows if float(row.get("target_weight", 0.0) or 0.0) > float(constraints["single_name_cap"])]

    quality_failures: list[str] = []
    if len({row["symbol"] for row in rows}) != len(rows):
        quality_failures.append("duplicate_symbol_rows")
    invalid_actions = [
        row["symbol"]
        for row in rows
        if row.get("action_recommendation")
        not in {"buy", "add", "hold", "trim", "exit", "do_not_trade"}
    ]
    if invalid_actions:
        quality_failures.append("invalid_action_recommendations")

    rows = sorted(
        rows,
        key=lambda row: (
            {"buy": 5, "add": 4, "trim": 3, "exit": 3, "hold": 2, "do_not_trade": 1}.get(
                str(row.get("action_recommendation", "")),
                0,
            ),
            float(row.get("target_weight", 0.0) or 0.0),
            int(row.get("confidence_score", 0) or 0),
        ),
        reverse=True,
    )

    return {
        "input_summary": {
            "recommendation_count": len(recommendation_rows),
            "review_count": len(review_rows),
            "event_risk_count": len(event_risk_rows),
            "portfolio_snapshot_as_of_utc": portfolio_snapshot.get("as_of_utc"),
            "universe_symbol_count": len(symbol_meta_lookup),
            "managed_universe_count": len(active_universe),
            "unmanaged_position_count": len(snapshot_validation.unmanaged_symbols),
            "active_open_order_count": sum(len(orders) for orders in open_order_summary.active_by_symbol.values()),
            "reserved_open_order_buy_notional": round(reserved_buy_notional, 2),
        },
        "input_audit": {
            "status": "ready",
            "snapshot_position_count": snapshot_validation.position_count,
            "snapshot_open_order_count": snapshot_validation.open_order_count,
            "unmanaged_symbols": snapshot_validation.unmanaged_symbols,
        },
        "quality_audit": {
            "status": "pass" if not quality_failures else "fail",
            "failure_reasons": quality_failures,
        },
        "portfolio_mode": portfolio_mode,
        "portfolio_mode_reason_codes": mode_reasons,
        "target_gross_long_exposure": round(target_gross_long, 6),
        "current_gross_long_exposure": round(current_gross_long, 6),
        "available_cash": round(effective_cash_available, 2),
        "portfolio_constraints": constraints,
        "theme_exposure": {
            "current": {key: round(value, 6) for key, value in sorted(current_theme_weights.items())},
            "target": {key: round(value, 6) for key, value in sorted(target_theme_weights.items())},
        },
        "horizon_exposure": {
            "current": {key: round(value, 6) for key, value in sorted(current_horizon_weights.items())},
            "target": {key: round(value, 6) for key, value in sorted(target_horizon_weights.items())},
        },
        "policy_violation_counts": {
            "single_name": len(single_name_violations),
            "theme": len(theme_violations),
            "horizon": len(horizon_violations),
            "blocked_rows": sum(1 for row in rows if row.get("policy_blockers")),
        },
        "rows": rows,
    }
