from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.ensemble import build_ensemble_rows
from tradly.paths import get_repo_root
from tradly.services.artifact_alignment import assess_artifact_alignment
from tradly.services.time_context import get_time_context


MAX_UPSTREAM_AGE = timedelta(hours=6)


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows_by_scope(payload: dict) -> dict[str, dict]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("scope_id", "")): row
        for row in rows
        if isinstance(row, dict) and str(row.get("scope_id", "")).strip()
    }


def _input_status(rows: list[dict]) -> str:
    if not rows:
        return "thin_evidence"
    insufficient_count = sum(1 for row in rows if str(row.get("coverage_state", "")) == "insufficient_evidence")
    thin_count = sum(1 for row in rows if str(row.get("coverage_state", "")) == "thin_evidence")
    upstream_thin_count = sum(1 for row in rows if "upstream_lane_thin" in row.get("why_code", []))
    if insufficient_count > 0:
        return "thin_evidence"
    if thin_count >= max(3, len(rows) // 5):
        return "thin_evidence"
    if upstream_thin_count >= max(5, len(rows) // 3):
        return "thin_evidence"
    return "ready"


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    scope_manifest_path = repo_root / "data" / "manual" / "universe_runtime_scopes.json"
    if not scope_manifest_path.exists():
        print(f"scope manifest missing: {scope_manifest_path}")
        return 1

    time_ctx = get_time_context()
    scope_manifest = json.loads(scope_manifest_path.read_text(encoding="utf-8"))
    model_symbols = scope_manifest["scopes"]["model_symbols"]
    if not isinstance(model_symbols, list) or not model_symbols:
        print("ensemble_v1_failed:model_symbols_missing")
        return 2

    registry_payload = json.loads((repo_root / "data" / "manual" / "universe_registry.json").read_text(encoding="utf-8"))
    registry_symbols = registry_payload.get("symbols", []) if isinstance(registry_payload, dict) else []
    symbol_metadata = {
        str(item.get("symbol", "")).strip().upper(): {
            "asset_type": str(item.get("asset_type", "")).strip(),
            "sector": str(item.get("sector", "")).strip(),
        }
        for item in registry_symbols
        if isinstance(item, dict) and str(item.get("symbol", "")).strip()
    }

    required_artifacts = {
        "market_regime_v1": _load_latest_json(runs_dir, "*/market_regime_v1.json"),
        "sector_movement_v1": _load_latest_json(runs_dir, "*/sector_movement_v1.json"),
        "symbol_movement_v1": _load_latest_json(runs_dir, "*/symbol_movement_v1.json"),
        "symbol_news_v1": _load_latest_json(runs_dir, "*/symbol_news_v1.json"),
        "sector_news_v1": _load_latest_json(runs_dir, "*/sector_news_v1.json"),
        "range_expectation_v1": _load_latest_json(runs_dir, "*/range_expectation_v1.json"),
    }
    missing = [name for name, payload in required_artifacts.items() if not payload]
    if missing:
        print("ensemble_v1_failed:missing_upstream_artifacts")
        for name in missing:
            print(f"error={name}_missing")
        return 3

    alignments = {
        name: assess_artifact_alignment(
            artifact_name=name,
            payload=payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        )
        for name, payload in required_artifacts.items()
    }
    stale_reasons: list[str] = []
    for alignment in alignments.values():
        stale_reasons.extend(alignment.reason_codes)
    if stale_reasons:
        print("ensemble_v1_failed:stale_upstream_artifacts")
        for reason in stale_reasons:
            print(f"error={reason}")
        return 4

    market_rows = required_artifacts["market_regime_v1"].get("rows")
    if not isinstance(market_rows, list) or not market_rows:
        print("ensemble_v1_failed:market_regime_rows_missing")
        return 5

    rows = build_ensemble_rows(
        market_row=market_rows[0],
        sector_rows_by_scope=_rows_by_scope(required_artifacts["sector_movement_v1"]),
        symbol_movement_rows_by_scope=_rows_by_scope(required_artifacts["symbol_movement_v1"]),
        symbol_news_rows_by_scope=_rows_by_scope(required_artifacts["symbol_news_v1"]),
        sector_news_rows_by_scope=_rows_by_scope(required_artifacts["sector_news_v1"]),
        range_rows_by_scope=_rows_by_scope(required_artifacts["range_expectation_v1"]),
        symbol_metadata=symbol_metadata,
        model_symbols=sorted(str(symbol).strip().upper() for symbol in model_symbols),
        now_utc=time_ctx.now_utc,
    )
    quality_audit = audit_model_artifact(rows).to_dict()
    input_status = _input_status(rows)
    upstream_thin_count = sum(1 for row in rows if "upstream_lane_thin" in row.get("why_code", []))
    coverage_counts: dict[str, int] = {}
    for row in rows:
        coverage_state = str(row.get("coverage_state", "unknown"))
        coverage_counts[coverage_state] = coverage_counts.get(coverage_state, 0) + 1

    registry_entry = get_model_registry_entry("ensemble_v1")
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "ensemble_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "model_symbol_count": len(model_symbols),
            "upstream_models": sorted(required_artifacts),
        },
        "input_audit": {
            "status": input_status,
            "aligned_artifacts": {
                name: {
                    "run_timestamp_utc": alignment.run_timestamp_utc,
                    "age_sec": alignment.age_sec,
                    "valid": alignment.valid,
                }
                for name, alignment in alignments.items()
            },
            "upstream_thin_row_count": upstream_thin_count,
            "coverage_counts": coverage_counts,
        },
        "quality_audit": quality_audit,
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"ensemble_rows={len(rows)}")
    if rows:
        confidences = [int(row.get("confidence_score", 0)) for row in rows]
        print(f"confidence_range=({min(confidences)},{max(confidences)})")
    print(f"quality_audit_status={quality_audit['status']}")
    if quality_audit["failure_reasons"]:
        print(f"quality_audit_failures={','.join(quality_audit['failure_reasons'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
