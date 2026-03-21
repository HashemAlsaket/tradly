from __future__ import annotations

import json


BASE_RULES = [
    "Use `symbol_specific` when the article is primarily about one or more named symbols.",
    "Use a canonical sector scope when the impact is mainly sector-level.",
    "Use `relevance_symbols` only for symbols that are explicitly present in the provided `symbols` list for that article.",
    "Do not add unrelated symbols to `relevance_symbols` just because they are large-cap peers, AI leaders, or thematically adjacent.",
    "If the article is sector-level or macro-level and no single provided symbol is clearly the main subject, return an empty `relevance_symbols` list.",
    "When the article is clearly about one provided symbol, prefer only that symbol in `relevance_symbols` rather than carrying over other tickers.",
    "Canonical sector ids only: `technology`, `healthcare`, `financial_services`, `industrials`, `consumer_defensive`, `communication_services`, `consumer_cyclical`, `basic_materials`, `real_estate`, `utilities`, `energy`.",
    "Do not output human-friendly aliases like `financials`, `consumer discretionary`, `consumer staples`, `materials`, `communication services`, or `real estate`.",
    "Use exact scope ids from the allowed list only. Prefer underscores, not spaces.",
    "Examples: `broad_market`, `risk_sentiment`, `symbol_specific`, `financial_services`.",
    "Use `multiple` only when the article clearly affects several distinct scopes and no single scope dominates.",
    "Never output placeholder scopes like `unclear`, `unknown`, or `n/a`.",
    "If scope is uncertain, choose the closest canonical scope instead.",
    "Use `bullish` or `bearish` for direct directional pressure on a sector or symbol.",
    "Use `risk_on` or `risk_off` for broader market tone or cross-asset posture.",
    "Use `2to6w` when the article's impact is more durable than a normal swing horizon.",
    "Treat `market_session_state`, `day_name`, `is_weekend`, `is_market_holiday`, and `last_cash_session_date` as important context.",
    "Weekend or holiday timing does not mean the data is stale; it means the cash market is closed.",
    "On weekends or market holidays, avoid overusing very short-horizon calls unless the article is clearly about the next trading session.",
    "Medium and position horizons may still be appropriate when the thesis is durable.",
]

SECTOR_MODULE_RULES = {
    "technology": [
        "For technology articles, classify the impact using broad-technology-aware thesis tags when applicable.",
        "Prefer concise tags such as `cloud_spend`, `enterprise_it_demand`, `software_margin_durability`, `ai_platform_monetization`, `networking_refresh`, `hardware_upgrade_cycle`.",
        "Use `semis` when the article is clearly semiconductor-specific. Use `technology` for broad software, infrastructure, enterprise IT, networking, or hardware platform articles.",
        "When a broad technology article is mainly about cloud platforms, enterprise software, networking infrastructure, consumer hardware, or AI application software, keep the impact scope as `technology` or `symbol_specific` rather than forcing it into broad market buckets.",
        "Do not tag semiconductor names in `relevance_symbols` unless they are explicitly listed in the article's provided `symbols`.",
    ],
    "healthcare": [
        "For healthcare articles, classify the impact using healthcare-aware thesis tags when applicable.",
        "Prefer concise tags such as `trial_readout`, `drug_approval`, `regulatory`, `pricing_reimbursement`, `utilization_cost_pressure`, `devices_tools_demand`, `patent_litigation`, `defensive_earnings_resilience`.",
        "When a healthcare article is mainly about large-cap pharma or managed care, keep the impact scope as `healthcare` or `symbol_specific` rather than forcing it into broad market or macro buckets.",
    ],
    "industrials": [
        "For industrials articles, classify the impact using industrials-aware thesis tags when applicable.",
        "Prefer concise tags such as `backlog_orders`, `capex_manufacturing_demand`, `aerospace_defense`, `freight_logistics`, `margin_input_cost`, `macro_pmi_sensitivity`.",
        "When an industrials article is mainly about capital equipment, aerospace and defense, or logistics, keep the impact scope as `industrials` or `symbol_specific` rather than forcing it into broad market buckets.",
    ],
    "consumer_defensive": [
        "For consumer defensive articles, classify the impact using consumer-defensive-aware thesis tags when applicable.",
        "Prefer concise tags such as `pricing_power`, `consumer_staples_demand`, `membership_traffic`, `margin_input_cost`, `defensive_rotation`, `private_label_mix`.",
        "When a consumer defensive article is mainly about staples demand, discount retail traffic, or household and personal care resilience, keep the impact scope as `consumer_defensive` or `symbol_specific` rather than forcing it into broad market buckets.",
        "Avoid tagging unrelated technology or AI symbols when the article is mainly about consumer staples or discount retail operators.",
    ],
    "communication_services": [
        "For communication services articles, classify the impact using communication-services-aware thesis tags when applicable.",
        "Prefer concise tags such as `digital_ad_demand`, `platform_monetization`, `streaming_engagement`, `subscriber_churn`, `content_pipeline`, `regulatory_platform_risk`, `ad_pricing_mix`.",
        "When a communication services article is mainly about internet platforms, streaming media, entertainment, or cable broadband, keep the impact scope as `communication_services` or `symbol_specific` rather than forcing it into broad market buckets.",
        "Only tag a communication-services symbol when that exact provided symbol is clearly discussed; do not substitute unrelated mega-cap tech or semiconductor names.",
    ],
    "energy": [
        "For energy articles, classify the impact using energy-aware thesis tags when applicable.",
        "Prefer concise tags such as `oil_price_leverage`, `upstream_supply_discipline`, `refining_margin`, `opec_supply`, `energy_services_demand`, `commodity_cost_pass_through`.",
        "When an energy article is mainly about integrated majors, upstream exploration and production, oilfield services, or commodity-linked cash flow leverage, keep the impact scope as `energy` or `symbol_specific` rather than forcing it into broad market or macro buckets.",
        "When the article is about crude, OPEC, or macro oil shock more than a specific company, leave `relevance_symbols` empty instead of forcing a stock tag.",
    ],
}


def _relevant_sector_modules(batch_articles: list[dict]) -> list[str]:
    sectors: set[str] = set()
    for article in batch_articles:
        for hint in article.get("symbol_sector_hints", []):
            sector = str(hint or "").strip().lower().replace(" ", "_")
            if sector in SECTOR_MODULE_RULES:
                sectors.add(sector)
    return sorted(sectors)


def build_news_interpreter_user_prompt(batch_articles: list[dict]) -> str:
    rules = list(BASE_RULES)
    for sector in _relevant_sector_modules(batch_articles):
        rules.extend(SECTOR_MODULE_RULES[sector])
    numbered_rules = "\n".join(f"{idx}. {rule}" for idx, rule in enumerate(rules, start=1))
    return (
        "For each article, output one interpretation object with this exact shape:\n"
        "{\n"
        '  "provider": "marketaux",\n'
        '  "provider_news_id": "id",\n'
        '  "bucket": "macro|sector|symbol|asia|ignore",\n'
        '  "impact_scope": "macro|broad_market|rates|energy|semis|usd|risk_sentiment|technology|healthcare|financial_services|industrials|consumer_defensive|communication_services|consumer_cyclical|basic_materials|real_estate|utilities|symbol_specific|multiple",\n'
        '  "impact_direction": "bullish|bearish|neutral|mixed|unclear|risk_on|risk_off",\n'
        '  "impact_horizon": "intraday|1to3d|1to2w|2to6w",\n'
        '  "relevance_symbols": ["MU"],\n'
        '  "thesis_tags": ["rates"],\n'
        '  "market_impact_note": "short plain English note",\n'
        '  "confidence_label": "low|medium|high",\n'
        '  "based_on_provided_evidence": true,\n'
        '  "calculation_performed": false\n'
        "}\n"
        "Interpretation rules:\n"
        f"{numbered_rules}\n"
        'Return as: {"interpretations":[...]}.\n'
        f"Articles:\n{json.dumps(batch_articles, ensure_ascii=True)}"
    )
