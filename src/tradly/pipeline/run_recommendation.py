from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.recommendation import build_recommendation_rows
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


def _quality_audit(rows: list[dict]) -> dict[str, object]:
    seen: set[str] = set()
    duplicate_scopes = 0
    invalid_actions = 0
    for row in rows:
        scope_id = str(row.get("scope_id", "")).strip()
        if scope_id in seen:
            duplicate_scopes += 1
        elif scope_id:
            seen.add(scope_id)
        if str(row.get("recommended_action", "")).strip() not in {
            "Buy",
            "Sell/Trim",
            "Watch Buy",
            "Watch Trim",
            "Hold",
            "Hold/Watch",
            "Defer",
            "Blocked",
            "Unknown",
        }:
            invalid_actions += 1
    failures: list[str] = []
    if duplicate_scopes:
        failures.append("duplicate_scope_ids")
    if invalid_actions:
        failures.append("invalid_recommended_actions")
    return {
        "status": "pass" if not failures else "fail",
        "failure_reasons": failures,
        "summary": {
            "row_count": len(rows),
            "duplicate_scope_count": duplicate_scopes,
            "invalid_action_count": invalid_actions,
        },
    }


def _input_status(ensemble_payload: dict, rows: list[dict]) -> str:
    if not rows:
        return "thin_evidence"
    ensemble_quality = str((ensemble_payload.get("quality_audit", {}) or {}).get("status", "")).strip().lower()
    if ensemble_quality == "fail":
        return "thin_evidence"
    ensemble_input_status = str((ensemble_payload.get("input_audit", {}) or {}).get("status", "")).strip().lower()
    if ensemble_input_status and ensemble_input_status != "ready":
        return ensemble_input_status
    return "ready"


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    time_ctx = get_time_context()

    ensemble_payload = _load_latest_json(runs_dir, "*/ensemble_v1.json")
    if not ensemble_payload:
        print("recommendation_v1_failed:ensemble_missing")
        return 1

    alignment = assess_artifact_alignment(
        artifact_name="ensemble_v1",
        payload=ensemble_payload,
        now_utc=time_ctx.now_utc,
        max_age=MAX_UPSTREAM_AGE,
    )
    if not alignment.valid:
        print("recommendation_v1_failed:stale_ensemble_artifact")
        for reason in alignment.reason_codes:
            print(f"error={reason}")
        return 2

    ensemble_rows = ensemble_payload.get("rows", [])
    if not isinstance(ensemble_rows, list) or not ensemble_rows:
        print("recommendation_v1_failed:ensemble_rows_missing")
        return 3

    rows = build_recommendation_rows(ensemble_rows=ensemble_rows, now_utc=time_ctx.now_utc)
    quality_audit = _quality_audit(rows)
    counts: dict[str, int] = {}
    class_counts: dict[str, int] = {}
    for row in rows:
        action = str(row.get("recommended_action", "Unknown"))
        counts[action] = counts.get(action, 0) + 1
        klass = str(row.get("recommendation_class", "unknown"))
        class_counts[klass] = class_counts.get(klass, 0) + 1

    registry_entry = get_model_registry_entry("recommendation_v1")
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "recommendation_v1.json"

    input_status = _input_status(ensemble_payload, rows)
    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "upstream_model": "ensemble_v1",
            "symbol_count": len(rows),
        },
        "input_audit": {
            "status": input_status,
            "aligned_artifacts": {
                "ensemble_v1": {
                    "run_timestamp_utc": alignment.run_timestamp_utc,
                    "age_sec": alignment.age_sec,
                    "valid": alignment.valid,
                }
            },
            "upstream_input_status": str((ensemble_payload.get("input_audit", {}) or {}).get("status", "")),
            "upstream_quality_status": str((ensemble_payload.get("quality_audit", {}) or {}).get("status", "")),
        },
        "quality_audit": quality_audit,
        "recommendation_counts": counts,
        "recommendation_class_counts": class_counts,
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"recommendation_rows={len(rows)}")
    print(f"quality_audit_status={quality_audit['status']}")
    print(f"recommendation_counts={counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
