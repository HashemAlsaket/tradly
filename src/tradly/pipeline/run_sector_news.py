from __future__ import annotations

import json
from collections import defaultdict

from tradly.config import get_model_registry_entry
from tradly.models.calibration import audit_model_artifact
from tradly.models.sector_news import SectorNewsItem, build_sector_news_rows
from tradly.paths import get_repo_root
from tradly.services.db_time import from_db_utc
from tradly.services.time_context import get_time_context


def main() -> int:
    repo_root = get_repo_root()
    db_path = repo_root / "data" / "tradly.duckdb"
    scope_manifest_path = repo_root / "data" / "manual" / "universe_runtime_scopes.json"
    if not db_path.exists():
        print(f"db file not found: {db_path}")
        return 1
    if not scope_manifest_path.exists():
        print(f"scope manifest missing: {scope_manifest_path}")
        return 2

    try:
        import duckdb
    except ImportError:
        print("duckdb is not installed. Install it with: pip install duckdb")
        return 3

    registry_entry = get_model_registry_entry("sector_news_v1")
    time_ctx = get_time_context()
    scope_payload = json.loads(scope_manifest_path.read_text(encoding="utf-8"))
    sector_members = {
        sector: members
        for sector, members in scope_payload["groupings"]["by_sector"].items()
        if sector not in {"ETF", "Macro"}
    }
    model_symbols = sorted(str(symbol).strip().upper() for symbol in scope_payload["scopes"]["model_symbols"])

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        interpretation_rows = conn.execute(
            f"""
            WITH latest_interpretation AS (
              SELECT *,
                     ROW_NUMBER() OVER (
                       PARTITION BY provider, provider_news_id
                       ORDER BY interpreted_at_utc DESC, prompt_version DESC
                     ) AS rn
              FROM news_interpretations
            )
            SELECT
              li.provider,
              li.provider_news_id,
              ne.published_at_utc,
              li.interpreted_at_utc,
              li.bucket,
              li.impact_scope,
              li.impact_direction,
              li.impact_horizon,
              li.confidence_label,
              li.relevance_symbols_json,
              li.thesis_tags_json,
              li.market_impact_note,
              li.prompt_version
            FROM latest_interpretation li
            JOIN news_events ne
              ON ne.provider = li.provider
             AND ne.provider_news_id = li.provider_news_id
            WHERE li.rn = 1
              AND li.bucket IN ('sector', 'symbol')
              AND li.interpreted_at_utc >= ?
            ORDER BY li.interpreted_at_utc DESC
            """,
            [time_ctx.now_utc - __import__("datetime").timedelta(days=7)],
        ).fetchall()
    finally:
        conn.close()

    sector_lookup = {symbol: sector for sector, members in sector_members.items() for symbol in members}
    interpretations_by_sector: dict[str, list[SectorNewsItem]] = defaultdict(list)
    prompt_versions: set[str] = set()
    for (
        provider,
        provider_news_id,
        published_at_utc,
        interpreted_at_utc,
        bucket,
        impact_scope,
        impact_direction,
        impact_horizon,
        confidence_label,
        relevance_symbols_json,
        thesis_tags_json,
        market_impact_note,
        prompt_version,
    ) in interpretation_rows:
        prompt_versions.add(str(prompt_version))
        try:
            relevance_symbols = tuple(str(item).strip().upper() for item in json.loads(relevance_symbols_json or "[]"))
        except json.JSONDecodeError:
            relevance_symbols = ()
        try:
            thesis_tags = tuple(str(item).strip() for item in json.loads(thesis_tags_json or "[]"))
        except json.JSONDecodeError:
            thesis_tags = ()

        item = SectorNewsItem(
            provider=str(provider),
            provider_news_id=str(provider_news_id),
            published_at_utc=from_db_utc(published_at_utc),
            interpreted_at_utc=from_db_utc(interpreted_at_utc),
            bucket=str(bucket),
            impact_scope=str(impact_scope),
            impact_direction=str(impact_direction),
            impact_horizon=str(impact_horizon),
            confidence_label=str(confidence_label),
            relevance_symbols=relevance_symbols,
            thesis_tags=thesis_tags,
            market_impact_note=str(market_impact_note),
        )

        assigned_sectors: set[str] = set()
        if item.bucket == "sector":
            for sector, scope in {
                "Technology": "technology",
                "Healthcare": "healthcare",
                "Financial Services": "financial_services",
                "Industrials": "industrials",
                "Consumer Defensive": "consumer_defensive",
                "Communication Services": "communication_services",
                "Consumer Cyclical": "consumer_cyclical",
                "Basic Materials": "basic_materials",
                "Real Estate": "real_estate",
                "Utilities": "utilities",
                "Energy": "energy",
            }.items():
                if item.impact_scope == scope or (item.impact_scope == "semis" and sector == "Technology"):
                    assigned_sectors.add(sector)

        if item.bucket == "symbol":
            for symbol in relevance_symbols:
                sector = sector_lookup.get(symbol)
                if sector:
                    assigned_sectors.add(sector)

        for sector in assigned_sectors:
            interpretations_by_sector[sector].append(item)

    rows = build_sector_news_rows(
        sector_members=sector_members,
        interpretations_by_sector=interpretations_by_sector,
        now_utc=time_ctx.now_utc,
    )
    audited_rows = [row for row in rows if row.get("coverage_state") != "insufficient_evidence"] or rows
    quality_audit = audit_model_artifact(audited_rows).to_dict()

    coverage_counts: dict[str, int] = {}
    blocked_sectors: list[str] = []
    for row in rows:
        coverage_state = str(row.get("coverage_state", "unknown"))
        coverage_counts[coverage_state] = coverage_counts.get(coverage_state, 0) + 1
        if coverage_state == "insufficient_evidence":
            blocked_sectors.append(str(row.get("scope_id", "")))

    run_date = time_ctx.now_utc.strftime("%Y-%m-%d")
    out_dir = repo_root / "data" / "runs" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sector_news_v1.json"

    interpreted_sector_count = len(interpretations_by_sector)
    covered_row_count = sum(1 for row in rows if row.get("coverage_state") != "insufficient_evidence")
    input_status = "ready" if covered_row_count >= max(7, len(sector_members) - 2) else "thin_evidence"

    payload = {
        "run_timestamp_utc": time_ctx.now_utc.isoformat(),
        "run_timestamp_local": time_ctx.now_local.isoformat(),
        "local_timezone": time_ctx.local_timezone,
        "model_id": registry_entry.model_id,
        "output_schema_version": registry_entry.output_schema_version,
        "registry": registry_entry.to_dict(),
        "input_summary": {
            "sector_count": len(sector_members),
            "interpreted_sector_count": interpreted_sector_count,
            "interpretation_count": len(interpretation_rows),
            "prompt_versions_seen": sorted(prompt_versions),
        },
        "input_audit": {
            "status": input_status,
            "recent_interpreted_sectors": sorted(interpretations_by_sector),
            "missing_recent_interpreted_sectors": sorted(sector for sector in sector_members if sector not in interpretations_by_sector),
            "lookback_days": 7,
            "model_symbol_count": len(model_symbols),
        },
        "row_audit": {
            "coverage_counts": coverage_counts,
            "blocked_sectors": blocked_sectors,
        },
        "quality_audit_scope": "covered_rows_only" if len(audited_rows) != len(rows) else "all_rows",
        "quality_audit": quality_audit,
        "rows": rows,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"output={out_path}")
    print(f"sector_news_rows={len(rows)}")
    print(f"interpreted_sector_count={interpreted_sector_count}")
    print(f"quality_audit_status={quality_audit['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
