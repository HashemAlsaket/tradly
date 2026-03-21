from __future__ import annotations


def freshness_policy_for_session(market_session: str) -> str:
    if market_session == "overnight":
        return "after_hours_relaxed"
    if market_session == "pre_market":
        return "premarket_tradable"
    if market_session == "market_hours":
        return "market_hours_strict"
    if market_session == "after_hours":
        return "after_hours_relaxed"
    return "closed_calendar_relaxed"


def freshness_mode_for_policy(policy: str) -> str:
    if policy == "market_hours_strict":
        return "market_hours"
    if policy == "closed_calendar_relaxed":
        return "closed_calendar"
    return "offhours"


def policy_requires_intraday_strict(policy: str) -> bool:
    return policy in {"market_hours_strict"}


def policy_uses_intraday(policy: str) -> bool:
    return policy in {"premarket_tradable", "market_hours_strict", "after_hours_relaxed"}


def policy_relaxes_intraday(policy: str) -> bool:
    return policy in {"premarket_tradable", "after_hours_relaxed"}
