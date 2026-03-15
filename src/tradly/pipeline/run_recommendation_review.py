from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from tradly.config import get_model_registry_entry
from tradly.models.recommendation_review import build_review_rows
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


def _input_status(recommendation_payload: dict, rows: list[dict]) -> str:
    if not rows:
        return "thin_evidence"
    upstream_input = str((recommendation_payload.get("input_audit", {}) or {}).get("status", "")).strip().lower()
    if upstream_input and upstream_input != "ready":
        return upstream_input
    upstream_quality = str((recommendation_payload.get("quality_audit", {}) or {}).get("status", "")).strip().lower()
    if upstream_quality == "fail":
        return "thin_evidence"
    return "ready"


def _quality_audit(rows: list[dict]) -> dict[str, object]:
    invalid_dispositions = 0
    invalid_buckets = 0
    valid_dispositions = {"promote", "review_required", "watch", "defer", "blocked"}
    valid_buckets = {"top_longs", "top_shorts", "top_ideas", "contrarian_review", "manual_review", "watchlist", "deferred", "blocked"}
    for row in rows:
        if str(row.get("review_disposition", "")).strip() not in valid_dispositions:
            invalid_dispositions += 1
        if str(row.get("review_bucket", "")).strip() not in valid_buckets:
            invalid_buckets += 1
    failures: list[str] = []
    if invalid_dispositions:
        failures.append("invalid_review_dispositions")
    if invalid_buckets:
        failures.append("invalid_review_buckets")
    return {
        "status": "pass" if not failures else "fail",
        "failure_reasons": failures,
        "summary": {
            "row_count": len(rows),
            "invalid_review_disposition_count": invalid_dispositions,
            "invalid_review_bucket_count": invalid_buckets,
        },
    }


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    time_ctx = get_time_context()

    recommendation_payload = _load_latest_json(runs_dir, "*/recommendation_v1.json")
    if not recommendation_payload:
        print("recommendation_review_v1_failed:recommendation_missing")
        return 1

    alignment = assess_artifact_alignment(
        artifact_name="recommendation_v1",
        payload=recommendation_payload,
        now_utc=time_ctx.now_utc,
        max_age=MAX_UPSTREAM_AGE,
    )
    if not alignment.valid:
        print("recommendation_review_v1_failed:stale_recommendation_artifact")
        return 2

    recommendation_rows = recommendation_payload.get("rows", [])
    if not isinstance(recommendation_rows, list) or not recommendation_rows:
        print("recommendation_review_v1_failed:recommendation_rows_missing")
        return 3

    rows = build_review_rows(recommendation_rows=recommendation_rows, now_utc=time_ctx.now_utc)
    quality_audit = _quality_audit(rows)
    counts: dict[str, int] = {}
    bucket_counts: dict[str, int] = {}
    for row in rows:
        disposition = str(row.get("review_disposition", "unknown"))
        counts[disposition] = counts.get(disposition, 0) + 1
        bucket = str(row.get("review_bucket", "unknown"))
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    registry_entry = get_model_registry_entry("recommendation_review_v1")
    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "recommendation_review_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "upstream_model": "recommendation_v1",
            "recommendation_count": len(recommendation_rows),
        },
        "input_audit": {
            "status": _input_status(recommendation_payload, rows),
            "upstream_input_status": str((recommendation_payload.get("input_audit", {}) or {}).get("status", "")),
            "upstream_quality_status": str((recommendation_payload.get("quality_audit", {}) or {}).get("status", "")),
        },
        "quality_audit": quality_audit,
        "review_disposition_counts": counts,
        "review_bucket_counts": bucket_counts,
        "rows": rows,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"review_rows={len(rows)}")
    print(f"review_disposition_counts={counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
