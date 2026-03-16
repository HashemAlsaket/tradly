from __future__ import annotations

import json
from pathlib import Path

from tradly.paths import get_repo_root
from tradly.services.time_context import get_time_context


def _load_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_history_compatible(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("cohort_model_id", "")).strip() != "recommendation_v1":
        return False
    if not str(payload.get("cohort_run_timestamp_utc", "")).strip():
        return False
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return False
    for row in rows:
        if not isinstance(row, dict):
            return False
        if "review_disposition" not in row or "review_bucket" not in row:
            return False
    return True


def _aggregate_run_summaries(payloads: list[tuple[Path, dict]]) -> dict[str, object]:
    totals = {
        "run_count": len(payloads),
        "total_recommendations": 0,
        "pending_count": 0,
        "scored_count": 0,
        "not_scored_count": 0,
        "correct_count": 0,
        "incorrect_count": 0,
        "flat_count": 0,
    }
    directional_returns: list[float] = []
    run_summaries: list[dict[str, object]] = []
    by_review_bucket: dict[str, dict[str, int]] = {}
    by_review_disposition: dict[str, dict[str, int]] = {}
    for path, payload in payloads:
        summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
        if not isinstance(summary, dict):
            summary = {}
        for key in totals:
            if key == "run_count":
                continue
            totals[key] += int(summary.get(key, 0) or 0)
        rows = payload.get("rows", [])
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                value = row.get("directional_return_pct")
                if value is not None:
                    directional_returns.append(float(value))
                for field_name, target in (
                    ("review_bucket", by_review_bucket),
                    ("review_disposition", by_review_disposition),
                ):
                    key = str(row.get(field_name, "unknown")).strip() or "unknown"
                    bucket_summary = target.setdefault(
                        key,
                        {
                            "total_recommendations": 0,
                            "pending_count": 0,
                            "scored_count": 0,
                            "not_scored_count": 0,
                            "correct_count": 0,
                            "incorrect_count": 0,
                            "flat_count": 0,
                        },
                    )
                    bucket_summary["total_recommendations"] += 1
                    status = str(row.get("evaluation_status", "")).strip()
                    if status == "pending":
                        bucket_summary["pending_count"] += 1
                    elif status == "scored":
                        bucket_summary["scored_count"] += 1
                    elif status == "not_scored":
                        bucket_summary["not_scored_count"] += 1
                    outcome = str(row.get("outcome_label", "")).strip()
                    if outcome == "correct":
                        bucket_summary["correct_count"] += 1
                    elif outcome == "incorrect":
                        bucket_summary["incorrect_count"] += 1
                    elif outcome == "flat":
                        bucket_summary["flat_count"] += 1
        run_summaries.append(
            {
                "path": str(path),
                "run_timestamp_utc": payload.get("run_timestamp_utc"),
                "summary": summary,
                "input_audit": payload.get("input_audit", {}),
            }
        )
    totals["average_directional_return_pct"] = round(sum(directional_returns) / len(directional_returns), 4) if directional_returns else None
    totals["hit_rate"] = round(totals["correct_count"] / totals["scored_count"], 4) if totals["scored_count"] else None
    return {
        "summary": totals,
        "runs": run_summaries,
        "by_review_bucket": dict(sorted(by_review_bucket.items())),
        "by_review_disposition": dict(sorted(by_review_disposition.items())),
    }


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    time_ctx = get_time_context()

    scorecard_paths = sorted(runs_dir.glob("*/recommendation_scorecard_v1__*.json"))
    if not scorecard_paths:
        scorecard_paths = sorted(runs_dir.glob("*/recommendation_scorecard_v1.json"))
    if not scorecard_paths:
        print("recommendation_scorecard_history_v1_failed:scorecard_missing")
        return 1

    payloads = [(path, _load_json(path)) for path in scorecard_paths]
    payloads = [(path, payload) for path, payload in payloads if payload]
    if not payloads:
        print("recommendation_scorecard_history_v1_failed:scorecard_unreadable")
        return 2
    compatible_payloads: dict[str, tuple[Path, dict]] = {}
    for path, payload in payloads:
        if not _is_history_compatible(payload):
            continue
        cohort_run_timestamp_utc = str(payload.get("cohort_run_timestamp_utc", "")).strip()
        existing = compatible_payloads.get(cohort_run_timestamp_utc)
        if existing is None or str(existing[1].get("run_timestamp_utc", "")) < str(payload.get("run_timestamp_utc", "")):
            compatible_payloads[cohort_run_timestamp_utc] = (path, payload)
    payloads = [compatible_payloads[key] for key in sorted(compatible_payloads)]
    if not payloads:
        print("recommendation_scorecard_history_v1_failed:no_compatible_scorecards")
        return 3

    aggregated = _aggregate_run_summaries(payloads)
    summary = aggregated["summary"]
    input_status = "ready" if int(summary.get("scored_count", 0) or 0) > 0 else "thin_evidence"

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "recommendation_scorecard_history_v1.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": "recommendation_scorecard_history_v1",
        "output_schema_version": 1,
        "input_summary": {
            "scorecard_run_count": len(payloads),
            "latest_scorecard_path": str(payloads[-1][0]),
        },
        "input_audit": {
            "status": input_status,
            "scored_count": int(summary.get("scored_count", 0) or 0),
            "run_count": len(payloads),
        },
        "summary": summary,
        "by_review_bucket": aggregated["by_review_bucket"],
        "by_review_disposition": aggregated["by_review_disposition"],
        "runs": aggregated["runs"],
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"history_summary={summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
