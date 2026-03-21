from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.portfolio_policy import build_portfolio_policy
from tradly.paths import get_repo_root
from tradly.services.artifact_alignment import assess_artifact_alignment
from tradly.services.time_context import get_time_context
from tradly.services.universe_registry import load_normalized_registry


MAX_UPSTREAM_AGE = timedelta(hours=6)


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    return _load_json_file(candidates[-1])


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    manual_dir = repo_root / "data" / "manual"
    time_ctx = get_time_context()

    market_payload = _load_latest_json(runs_dir, "*/market_regime_v1.json")
    recommendation_payload = _load_latest_json(runs_dir, "*/recommendation_v1.json")
    review_payload = _load_latest_json(runs_dir, "*/recommendation_review_v1.json")
    event_risk_payload = _load_latest_json(runs_dir, "*/event_risk_v1.json")
    freshness_snapshot = _load_json_file(repo_root / "data" / "journal" / "freshness_snapshot.json")
    portfolio_snapshot = _load_json_file(manual_dir / "portfolio_snapshot_v1.json")
    try:
        universe_registry = load_normalized_registry(manual_dir / "universe_registry.json")
    except Exception:
        universe_registry = {}

    if not market_payload:
        print("portfolio_policy_v1_failed:market_regime_missing")
        return 1
    if not recommendation_payload:
        print("portfolio_policy_v1_failed:recommendation_missing")
        return 2
    if not review_payload:
        print("portfolio_policy_v1_failed:recommendation_review_missing")
        return 3
    if not event_risk_payload:
        print("portfolio_policy_v1_failed:event_risk_missing")
        return 4
    if not freshness_snapshot:
        print("portfolio_policy_v1_failed:freshness_snapshot_missing")
        return 5
    if not portfolio_snapshot:
        print("portfolio_policy_v1_failed:portfolio_snapshot_missing")
        return 6
    if not universe_registry:
        print("portfolio_policy_v1_failed:universe_registry_missing")
        return 7

    alignments = {
        "market_regime_v1": assess_artifact_alignment(
            artifact_name="market_regime_v1",
            payload=market_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
        "recommendation_v1": assess_artifact_alignment(
            artifact_name="recommendation_v1",
            payload=recommendation_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
        "recommendation_review_v1": assess_artifact_alignment(
            artifact_name="recommendation_review_v1",
            payload=review_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
        "event_risk_v1": assess_artifact_alignment(
            artifact_name="event_risk_v1",
            payload=event_risk_payload,
            now_utc=time_ctx.now_utc,
            max_age=MAX_UPSTREAM_AGE,
        ),
    }
    stale = [name for name, alignment in alignments.items() if not alignment.valid]
    if stale:
        print(f"portfolio_policy_v1_failed:stale_upstream:{','.join(sorted(stale))}")
        return 8

    model_payload = build_portfolio_policy(
        market_regime_payload=market_payload,
        recommendation_payload=recommendation_payload,
        review_payload=review_payload,
        event_risk_payload=event_risk_payload,
        freshness_snapshot=freshness_snapshot,
        portfolio_snapshot=portfolio_snapshot,
        universe_registry=universe_registry,
        now_utc=time_ctx.now_utc,
    )

    input_audit = model_payload.get("input_audit", {}) if isinstance(model_payload, dict) else {}
    if str(input_audit.get("status", "")).strip().lower() == "fail":
        print("portfolio_policy_v1_failed:invalid_portfolio_snapshot")
        for reason in input_audit.get("failure_reasons", []):
            print(f"error={reason}")
        return 9

    registry_entry = get_model_registry_entry("portfolio_policy_v1")
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "portfolio_policy_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            **(model_payload.get("input_summary", {}) if isinstance(model_payload, dict) else {}),
            "upstream_models": ["market_regime_v1", "recommendation_v1", "recommendation_review_v1", "event_risk_v1"],
        },
        "input_audit": {
            **(input_audit if isinstance(input_audit, dict) else {}),
            "aligned_artifacts": {
                name: {
                    "run_timestamp_utc": alignment.run_timestamp_utc,
                    "age_sec": alignment.age_sec,
                    "valid": alignment.valid,
                }
                for name, alignment in alignments.items()
            },
        },
        "quality_audit": model_payload.get("quality_audit", {}),
        "portfolio_mode": model_payload.get("portfolio_mode"),
        "portfolio_mode_reason_codes": model_payload.get("portfolio_mode_reason_codes", []),
        "target_gross_long_exposure": model_payload.get("target_gross_long_exposure"),
        "current_gross_long_exposure": model_payload.get("current_gross_long_exposure"),
        "available_cash": model_payload.get("available_cash"),
        "portfolio_constraints": model_payload.get("portfolio_constraints", {}),
        "theme_exposure": model_payload.get("theme_exposure", {}),
        "horizon_exposure": model_payload.get("horizon_exposure", {}),
        "policy_violation_counts": model_payload.get("policy_violation_counts", {}),
        "rows": model_payload.get("rows", []),
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"portfolio_mode={payload['portfolio_mode']}")
    print(f"portfolio_rows={len(payload['rows'])}")
    print(f"quality_audit_status={(payload.get('quality_audit', {}) or {}).get('status', 'missing')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
