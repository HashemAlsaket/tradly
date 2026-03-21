from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


SECTOR_ETF_BY_SECTOR = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financial Services": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Consumer Defensive": "XLP",
    "Consumer Cyclical": "XLY",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Basic Materials": "XLB",
}


@dataclass(frozen=True)
class EventPriceContext:
    symbol: str
    current_price: float | None
    prev_close: float | None
    price_reaction_pct: float | None
    qqq_change_pct: float | None
    sector_change_pct: float | None
    move_vs_qqq_pct: float | None
    move_vs_sector_pct: float | None
    market_session_state: str
    market_regime: str
    macro_state: str
    current_action: str
    current_confidence: int
    reaction_window_ready: bool
    sector_proxy_symbol: str | None


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def build_event_price_context(
    *,
    symbol: str,
    snapshot_by_symbol: dict[str, dict[str, Any]],
    symbol_metadata: dict[str, dict[str, Any]],
    market_row: dict[str, Any],
    recommendation_row: dict[str, Any] | None,
) -> EventPriceContext:
    symbol_snapshot = snapshot_by_symbol.get(symbol, {})
    sector = str(symbol_metadata.get(symbol, {}).get("sector", "")).strip()
    sector_proxy_symbol = SECTOR_ETF_BY_SECTOR.get(sector)
    qqq_snapshot = snapshot_by_symbol.get("QQQ", {})
    sector_snapshot = snapshot_by_symbol.get(sector_proxy_symbol, {}) if sector_proxy_symbol else {}
    market_evidence = market_row.get("evidence", {}) if isinstance(market_row.get("evidence"), dict) else {}
    macro_hostility = market_evidence.get("macro_hostility", {}) if isinstance(market_evidence.get("macro_hostility"), dict) else {}
    market_session_state = str(market_row.get("lane_diagnostics", {}).get("near_term", {}).get("market_session_state", "")).strip() or str(market_row.get("evidence", {}).get("market_session_state", "")).strip()
    current_action = str((recommendation_row or {}).get("recommended_action", "")).strip()
    current_confidence = int((recommendation_row or {}).get("confidence_score", 0) or 0)

    price_reaction_pct = _as_float(symbol_snapshot.get("change_pct"))
    qqq_change_pct = _as_float(qqq_snapshot.get("change_pct"))
    sector_change_pct = _as_float(sector_snapshot.get("change_pct"))
    move_vs_qqq_pct = price_reaction_pct - qqq_change_pct if price_reaction_pct is not None and qqq_change_pct is not None else None
    move_vs_sector_pct = price_reaction_pct - sector_change_pct if price_reaction_pct is not None and sector_change_pct is not None else None

    return EventPriceContext(
        symbol=symbol,
        current_price=_as_float(symbol_snapshot.get("last_trade_price")) or _as_float(symbol_snapshot.get("session_close")),
        prev_close=_as_float(symbol_snapshot.get("prev_close")),
        price_reaction_pct=price_reaction_pct,
        qqq_change_pct=qqq_change_pct,
        sector_change_pct=sector_change_pct,
        move_vs_qqq_pct=move_vs_qqq_pct,
        move_vs_sector_pct=move_vs_sector_pct,
        market_session_state=market_session_state,
        market_regime=str(market_row.get("signal_direction", "")).strip().lower(),
        macro_state=str(macro_hostility.get("macro_state", "")).strip().lower(),
        current_action=current_action,
        current_confidence=current_confidence,
        reaction_window_ready=price_reaction_pct is not None,
        sector_proxy_symbol=sector_proxy_symbol,
    )
