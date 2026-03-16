from __future__ import annotations

import json
from datetime import timezone
from pathlib import Path

import duckdb

from tradly.analytics.recommendation_scorecard import build_scorecard_rows, summarize_groups, summarize_scorecard
from tradly.paths import get_repo_root
from tradly.services.time_context import get_time_context


def _load_latest_json(runs_dir: Path, pattern: str) -> dict:
    candidates = sorted(runs_dir.glob(pattern))
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_bars_by_symbol(db_path: Path, symbols: list[str]) -> dict[str, list[dict]]:
    if not symbols:
        return {}
    placeholders = ", ".join(["?"] * len(symbols))
    query = f"""
        SELECT symbol, ts_utc, close
        FROM market_bars
        WHERE timeframe = '1d'
          AND symbol IN ({placeholders})
        ORDER BY symbol, ts_utc
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(query, symbols).fetchall()
    finally:
        con.close()

    bars_by_symbol: dict[str, list[dict]] = {}
    for symbol, ts_utc, close in rows:
        normalized_ts = ts_utc.replace(tzinfo=timezone.utc) if ts_utc.tzinfo is None else ts_utc.astimezone(timezone.utc)
        bars_by_symbol.setdefault(str(symbol), []).append(
            {
                "ts_utc": normalized_ts,
                "close": float(close) if close is not None else 0.0,
            }
        )
    return bars_by_symbol


def _input_status(recommendation_payload: dict, summary: dict) -> str:
    upstream_quality = str((recommendation_payload.get("quality_audit", {}) or {}).get("status", "")).strip().lower()
    if upstream_quality == "fail":
        return "thin_evidence"
    upstream_input = str((recommendation_payload.get("input_audit", {}) or {}).get("status", "")).strip().lower()
    if upstream_input and upstream_input != "ready":
        return upstream_input
    if int(summary.get("scored_count", 0) or 0) == 0:
        return "thin_evidence"
    return "ready"


def main() -> int:
    repo_root = get_repo_root()
    runs_dir = repo_root / "data" / "runs"
    time_ctx = get_time_context()

    review_payload = _load_latest_json(runs_dir, "*/recommendation_review_v1.json")
    if not review_payload:
        print("recommendation_scorecard_v1_failed:recommendation_review_missing")
        return 1

    recommendation_rows = review_payload.get("rows", [])
    if not isinstance(recommendation_rows, list) or not recommendation_rows:
        print("recommendation_scorecard_v1_failed:recommendation_review_rows_missing")
        return 2
    cohort_run_timestamp_utc = str(
        (review_payload.get("input_summary", {}) or {}).get("recommendation_run_timestamp_utc", "")
    ).strip()
    if not cohort_run_timestamp_utc:
        print("recommendation_scorecard_v1_failed:recommendation_cohort_timestamp_missing")
        return 3

    symbols = sorted(
        {
            str(row.get("scope_id", "")).strip()
            for row in recommendation_rows
            if str(row.get("scope_id", "")).strip()
        }
    )
    bars_by_symbol = _load_bars_by_symbol(repo_root / "data" / "tradly.duckdb", symbols)
    scorecard_rows = build_scorecard_rows(recommendation_rows=recommendation_rows, bars_by_symbol=bars_by_symbol)
    summary = summarize_scorecard(scorecard_rows)

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "recommendation_scorecard_v1.json"
    cohort_ts_compact = cohort_run_timestamp_utc.replace("-", "").replace(":", "").replace("+00:00", "Z").replace("T", "T")
    archive_path = out_dir / f"recommendation_scorecard_v1__cohort_{cohort_ts_compact}.json"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": "recommendation_scorecard_v1",
        "output_schema_version": 1,
        "cohort_model_id": "recommendation_v1",
        "cohort_run_timestamp_utc": cohort_run_timestamp_utc,
        "review_run_timestamp_utc": str(review_payload.get("run_timestamp_utc", "")),
        "input_summary": {
            "upstream_model": "recommendation_review_v1",
            "recommendation_count": len(recommendation_rows),
            "scored_symbol_count": len(bars_by_symbol),
        },
        "input_audit": {
            "status": _input_status(review_payload, summary),
            "upstream_input_status": str((review_payload.get("input_audit", {}) or {}).get("status", "")),
            "upstream_quality_status": str((review_payload.get("quality_audit", {}) or {}).get("status", "")),
            "pending_only": int(summary.get("scored_count", 0) or 0) == 0,
            "scored_count": int(summary.get("scored_count", 0) or 0),
        },
        "evaluation_policy": {
            "entry_rule": "next_available_1d_close_after_recommendation_timestamp",
            "exit_rule": "close_after_horizon_trading_days",
            "horizon_trading_days": {
                "1to3d": 3,
                "1to2w": 10,
                "2to6w": 30,
            },
        },
        "summary": summary,
        "by_action": summarize_groups(scorecard_rows, field="recommended_action"),
        "by_horizon": summarize_groups(scorecard_rows, field="recommended_horizon"),
        "by_regime_alignment": summarize_groups(scorecard_rows, field="regime_alignment"),
        "by_review_disposition": summarize_groups(scorecard_rows, field="review_disposition"),
        "by_review_bucket": summarize_groups(scorecard_rows, field="review_bucket"),
        "rows": scorecard_rows,
    }

    serialized = json.dumps(payload, indent=2)
    out_path.write_text(serialized, encoding="utf-8")
    archive_path.write_text(serialized, encoding="utf-8")
    print(f"output={out_path}")
    print(f"archive_output={archive_path}")
    print(f"scorecard_rows={len(scorecard_rows)}")
    print(f"summary={payload['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
