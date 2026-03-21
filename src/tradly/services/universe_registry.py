from __future__ import annotations

import json
from pathlib import Path


ONBOARDING_STAGE_FLAGS = {
    "registry_only": {"active": False, "market_data": False, "model": False, "direct_news": False},
    "market_data_only": {"active": False, "market_data": True, "model": False, "direct_news": False},
    "modeled": {"active": True, "market_data": True, "model": True, "direct_news": False},
    "modeled_with_direct_news": {"active": True, "market_data": True, "model": True, "direct_news": True},
    "portfolio_eligible": {"active": True, "market_data": True, "model": True, "direct_news": True},
}


def normalize_registry_row(row: dict) -> dict:
    normalized = dict(row)
    stage = str(normalized.get("onboarding_stage", "")).strip()
    if stage:
        flags = ONBOARDING_STAGE_FLAGS.get(stage)
        if flags is None:
            raise RuntimeError(f"invalid_onboarding_stage:{stage}")
        normalized.update(flags)
    return normalized


def load_normalized_registry(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("universe registry must be an object")
    symbols = payload.get("symbols", [])
    if not isinstance(symbols, list):
        raise RuntimeError("universe registry must contain a symbols list")
    normalized = dict(payload)
    normalized["symbols"] = [normalize_registry_row(row) for row in symbols if isinstance(row, dict)]
    return normalized
