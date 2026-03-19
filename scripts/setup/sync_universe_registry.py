from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

from tradly.services.universe_registry import normalize_registry_row


BUCKET_ORDER = [
    "core_semis",
    "healthcare_core",
    "us_macro",
    "asia_semis",
    "asia_macro",
    "sector_context",
    "event_reserve",
]

DEFAULT_BUCKET_CAPS = {
    "healthcare_core": 300,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_registry(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise RuntimeError("universe registry must be an object with a symbols list")
    return payload["symbols"]


def _write_universe_csv(path: Path, symbols: list[dict]) -> None:
    fieldnames = ["symbol", "asset_type", "sector", "industry", "halal_flag", "active"]
    rows = [
        {
            "symbol": row["symbol"],
            "asset_type": row["asset_type"],
            "sector": row["sector"],
            "industry": row["industry"],
            "halal_flag": row["halal_flag"],
            "active": "true" if bool(row.get("active")) else "false",
        }
        for row in symbols
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_bucket_map(symbols: list[dict]) -> dict[str, list[str]]:
    bucket_map: dict[str, set[str]] = defaultdict(set)
    for row in symbols:
        if not bool(row.get("direct_news")):
            continue
        symbol = str(row["symbol"]).strip().upper()
        for bucket in row.get("news_buckets", []):
            if bucket:
                bucket_map[str(bucket)].add(symbol)
    ordered: dict[str, list[str]] = {}
    for bucket in BUCKET_ORDER:
        ordered[bucket] = sorted(bucket_map.get(bucket, set()))
    for bucket in sorted(bucket_map):
        if bucket not in ordered:
            ordered[bucket] = sorted(bucket_map[bucket])
    return ordered


def _build_scope_manifest(symbols: list[dict], bucket_map: dict[str, list[str]]) -> dict:
    def _sorted_where(field: str) -> list[str]:
        return sorted(str(row["symbol"]).strip().upper() for row in symbols if bool(row.get(field)))

    all_symbols = sorted(str(row["symbol"]).strip().upper() for row in symbols)
    by_sector: dict[str, list[str]] = defaultdict(list)
    by_role: dict[str, list[str]] = defaultdict(list)
    for row in symbols:
        symbol = str(row["symbol"]).strip().upper()
        by_sector[str(row["sector"])].append(symbol)
        for role in row.get("roles", []):
            by_role[str(role)].append(symbol)

    return {
        "version": 1,
        "summary": {
            "all_symbols": len(all_symbols),
            "active_symbols": sum(1 for row in symbols if bool(row.get("active"))),
            "market_data_symbols": sum(1 for row in symbols if bool(row.get("market_data"))),
            "model_symbols": sum(1 for row in symbols if bool(row.get("model"))),
            "direct_news_symbols": sum(1 for row in symbols if bool(row.get("direct_news"))),
            "portfolio_eligible_symbols": sum(1 for row in symbols if bool(row.get("portfolio_eligible"))),
        },
        "scopes": {
            "all_symbols": all_symbols,
            "active_symbols": _sorted_where("active"),
            "market_data_symbols": _sorted_where("market_data"),
            "model_symbols": _sorted_where("model"),
            "direct_news_symbols": _sorted_where("direct_news"),
            "portfolio_eligible_symbols": _sorted_where("portfolio_eligible"),
        },
        "groupings": {
            "by_sector": {sector: sorted(values) for sector, values in sorted(by_sector.items())},
            "by_role": {role: sorted(values) for role, values in sorted(by_role.items())},
            "by_onboarding_stage": {
                stage: sorted(
                    str(row["symbol"]).strip().upper()
                    for row in symbols
                    if str(row.get("onboarding_stage", "")).strip() == stage
                )
                for stage in sorted({str(row.get("onboarding_stage", "")).strip() for row in symbols if str(row.get("onboarding_stage", "")).strip()})
            },
            "news_buckets": bucket_map,
        },
    }


def _write_watchlists(path: Path, template_path: Path, symbols: list[dict]) -> None:
    template = json.loads(template_path.read_text(encoding="utf-8"))
    if not isinstance(template, dict):
        raise RuntimeError("watchlist template must be a JSON object")

    buckets = _build_bucket_map(symbols)
    bucket_caps = template.get("bucket_daily_caps", {})
    ordered_caps: dict[str, int] = {}
    for bucket in BUCKET_ORDER:
        if bucket in bucket_caps:
            ordered_caps[bucket] = int(bucket_caps[bucket])
    for bucket in buckets:
        if bucket not in ordered_caps:
            ordered_caps[bucket] = int(bucket_caps.get(bucket, DEFAULT_BUCKET_CAPS.get(bucket, 0)))

    payload = {
        "lookback_days": int(template.get("lookback_days", 90)),
        "limit_per_request": int(template.get("limit_per_request", 3)),
        "pulls_per_bucket_per_run": int(template.get("pulls_per_bucket_per_run", 1)),
        "daily_request_budget": int(template.get("daily_request_budget", 100)),
        "bucket_daily_caps": ordered_caps,
        "max_pages_per_bucket": int(template.get("max_pages_per_bucket", 300)),
        "buckets": buckets,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_scope_manifest(path: Path, symbols: list[dict], bucket_map: dict[str, list[str]]) -> None:
    payload = _build_scope_manifest(symbols, bucket_map)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    repo_root = _repo_root()
    registry_path = repo_root / "data" / "manual" / "universe_registry.json"
    csv_path = repo_root / "db" / "seeds" / "universe_v1.csv"
    watchlist_path = repo_root / "data" / "manual" / "news_seed_watchlists.json"
    scope_manifest_path = repo_root / "data" / "manual" / "universe_runtime_scopes.json"

    if not registry_path.exists():
        print(f"registry_missing={registry_path}")
        return 1
    if not watchlist_path.exists():
        print(f"watchlist_missing={watchlist_path}")
        return 2

    symbols = sorted((normalize_registry_row(row) for row in _load_registry(registry_path)), key=lambda row: row["symbol"])
    _write_universe_csv(csv_path, symbols)
    bucket_map = _build_bucket_map(symbols)
    _write_watchlists(watchlist_path, watchlist_path, symbols)
    _write_scope_manifest(scope_manifest_path, symbols, bucket_map)

    active_count = sum(1 for row in symbols if bool(row.get("active")))
    market_data_count = sum(1 for row in symbols if bool(row.get("market_data")))
    model_count = sum(1 for row in symbols if bool(row.get("model")))
    direct_news_count = sum(1 for row in symbols if bool(row.get("direct_news")))

    print(f"registry_symbols={len(symbols)}")
    print(f"csv_rows_written={len(symbols)}")
    print(f"active_rows={active_count}")
    print(f"market_data_rows={market_data_count}")
    print(f"model_rows={model_count}")
    print(f"direct_news_symbols={direct_news_count}")
    print(f"bucket_counts={[(bucket, len(values)) for bucket, values in bucket_map.items()]}")
    print(f"scope_manifest={scope_manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
