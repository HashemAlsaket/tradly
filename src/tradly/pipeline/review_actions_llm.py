from __future__ import annotations

import http.client
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from tradly.config.model_suite import load_openai_model_suite
from tradly.paths import get_repo_root


ALLOWED_ACTIONS = {"Strong Buy", "Buy", "Watch", "Trim", "Exit", "Abstain"}
ALLOWED_CONFIDENCE = {"low", "medium", "high"}
BUY_ACTIONS = {"Strong Buy", "Buy"}
MIN_CONFIDENCE_SCORE = 0
MAX_CONFIDENCE_SCORE = 100


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _find_latest_actions(runs_dir: Path) -> Path | None:
    if not runs_dir.exists():
        return None
    candidates = sorted(runs_dir.glob("*/model_v0_actions.json"))
    if not candidates:
        return None
    return candidates[-1]


def _call_openai_chat(model: str, api_key: str, prompt_payload: dict) -> dict:
    system_text = (
        "You are a trading decision reviewer.\n"
        "Hard constraints:\n"
        "1) You MUST NOT calculate any new numeric values.\n"
        "2) You may only interpret the provided fields.\n"
        "3) Return only valid JSON.\n"
        "4) For each symbol return exactly one decision.\n"
        "5) Include based_on_provided_evidence=true and calculation_performed=false.\n"
    )
    user_text = (
        "Review the symbol evidence and provide final action calls.\n"
        "Allowed llm_action values: Strong Buy, Buy, Watch, Trim, Exit, Abstain.\n"
        "Allowed confidence_label values: low, medium, high.\n"
        "You MUST also provide confidence_score as an integer from 0 to 100.\n"
        "confidence_score is your judgment score based only on the provided evidence.\n"
        "It is NOT a calculated probability and must not be described as one.\n"
        "Output JSON format:\n"
        "{\n"
        '  "decisions": [\n'
        "    {\n"
        '      "symbol": "MU",\n'
        '      "llm_action": "Buy",\n'
        '      "confidence_score": 74,\n'
        '      "confidence_label": "medium",\n'
        '      "rationale": "short text",\n'
        '      "based_on_provided_evidence": true,\n'
        '      "calculation_performed": false\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        f"Input evidence:\n{json.dumps(prompt_payload, ensure_ascii=True)}"
    )

    request_body = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
    }

    conn = http.client.HTTPSConnection("api.openai.com", timeout=60)
    try:
        conn.request(
            "POST",
            "/v1/chat/completions",
            body=json.dumps(request_body),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        response = conn.getresponse()
        body = response.read().decode("utf-8")
    finally:
        conn.close()

    if response.status >= 400:
        raise RuntimeError(f"openai_http_error status={response.status} body={body[:500]}")

    payload = json.loads(body)
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("openai response missing choices")
    message = choices[0].get("message", {})
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("openai response missing message content")
    return json.loads(content)


def _validate_decisions(actions_payload: dict, llm_payload: dict) -> dict[str, dict]:
    actions = actions_payload.get("actions")
    if not isinstance(actions, list) or not actions:
        raise RuntimeError("actions payload missing actions list")

    action_symbols = [str(row.get("symbol", "")).strip().upper() for row in actions]
    action_set = set(action_symbols)
    action_row_by_symbol = {
        str(row.get("symbol", "")).strip().upper(): row
        for row in actions
        if str(row.get("symbol", "")).strip()
    }
    news_guardrails = actions_payload.get("news_guardrails", {})
    coverage_blocked = bool(news_guardrails.get("coverage_blocked")) if isinstance(news_guardrails, dict) else False

    decisions = llm_payload.get("decisions")
    if not isinstance(decisions, list) or not decisions:
        raise RuntimeError("llm output missing decisions list")

    out: dict[str, dict] = {}
    for row in decisions:
        if not isinstance(row, dict):
            raise RuntimeError("llm decision row is not object")
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol not in action_set:
            raise RuntimeError(f"llm decision symbol invalid: {symbol}")
        llm_action = str(row.get("llm_action", "")).strip()
        if llm_action not in ALLOWED_ACTIONS:
            raise RuntimeError(f"llm_action invalid for {symbol}: {llm_action}")
        confidence_score = row.get("confidence_score")
        if isinstance(confidence_score, bool) or not isinstance(confidence_score, int):
            raise RuntimeError(f"confidence_score invalid type for {symbol}: {confidence_score}")
        if confidence_score < MIN_CONFIDENCE_SCORE or confidence_score > MAX_CONFIDENCE_SCORE:
            raise RuntimeError(
                f"confidence_score out of range for {symbol}: {confidence_score}"
            )
        confidence = str(row.get("confidence_label", "")).strip().lower()
        if confidence not in ALLOWED_CONFIDENCE:
            raise RuntimeError(f"confidence_label invalid for {symbol}: {confidence}")
        rationale = str(row.get("rationale", "")).strip()
        if not rationale:
            raise RuntimeError(f"rationale missing for {symbol}")
        based_on_evidence = row.get("based_on_provided_evidence")
        calculation_performed = row.get("calculation_performed")
        if based_on_evidence is not True:
            raise RuntimeError(f"based_on_provided_evidence must be true for {symbol}")
        if calculation_performed is not False:
            raise RuntimeError(f"calculation_performed must be false for {symbol}")

        model_row = action_row_by_symbol.get(symbol, {})
        if llm_action in BUY_ACTIONS:
            if coverage_blocked:
                raise RuntimeError(f"buy_disallowed_under_coverage_block:{symbol}")
            if bool(model_row.get("investability_blocked")):
                raise RuntimeError(f"buy_disallowed_investability_block:{symbol}")
            if model_row.get("hard_downgrade_reason"):
                raise RuntimeError(
                    f"buy_disallowed_hard_downgrade:{symbol}:{model_row.get('hard_downgrade_reason')}"
                )

        out[symbol] = {
            "llm_action": llm_action,
            "confidence_score": confidence_score,
            "confidence_label": confidence,
            "rationale": rationale,
            "based_on_provided_evidence": True,
            "calculation_performed": False,
        }

    missing = action_set - set(out.keys())
    if missing:
        raise RuntimeError(f"llm decisions missing symbols: {sorted(missing)}")
    extras = set(out.keys()) - action_set
    if extras:
        raise RuntimeError(f"llm decisions had extra symbols: {sorted(extras)}")

    return out


def main() -> int:
    repo_root = get_repo_root()
    _load_dotenv(repo_root / ".env")
    runs_dir = repo_root / "data" / "runs"

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY missing")
        return 1

    model_suite = load_openai_model_suite()
    model = model_suite.llm_model
    if not model:
        print("OPENAI_LLM_MODEL missing")
        return 2

    latest_actions_path = _find_latest_actions(runs_dir)
    if latest_actions_path is None:
        print("no model_v0_actions.json found under data/runs")
        return 3

    actions_payload = json.loads(latest_actions_path.read_text(encoding="utf-8"))
    actions = actions_payload.get("actions")
    if not isinstance(actions, list) or not actions:
        print("actions payload invalid or empty")
        return 4

    prompt_actions: list[dict] = []
    for row in actions:
        prompt_actions.append(
            {
                "symbol": row.get("symbol"),
                "sector": row.get("sector"),
                "halal_flag": row.get("halal_flag"),
                "data_status": row.get("data_status"),
                "model_action": row.get("final_action"),
                "model_score": row.get("final_score"),
                "horizon_type": row.get("horizon_type"),
                "reference_price": row.get("reference_price"),
                "invalidation_price": row.get("invalidation_price"),
                "sell_plan": row.get("sell_plan"),
                "why_now": row.get("why_now"),
                "regime_summary": row.get("regime_summary"),
                "regime_flags": row.get("regime_flags"),
                "news_count_24h": row.get("news_count_24h"),
                "news_sentiment_avg_24h": row.get("news_sentiment_avg_24h"),
                "execution_regime": row.get("execution_regime"),
            }
        )

    prompt_payload = {
        "run_timestamp_utc": actions_payload.get("run_timestamp_utc"),
        "model": actions_payload.get("model"),
        "decision_mode": actions_payload.get("decision_mode"),
        "symbols": prompt_actions,
    }

    try:
        llm_raw = _call_openai_chat(model=model, api_key=api_key, prompt_payload=prompt_payload)
        decisions_by_symbol = _validate_decisions(actions_payload=actions_payload, llm_payload=llm_raw)
    except Exception as exc:
        print(f"llm_review_failed={exc}")
        return 5

    reviewed_actions = []
    for row in actions:
        symbol = str(row.get("symbol", "")).strip().upper()
        review = decisions_by_symbol[symbol]
        merged = dict(row)
        merged["llm_action"] = review["llm_action"]
        merged["llm_decision_confidence_score"] = review["confidence_score"]
        merged["llm_confidence_label"] = review["confidence_label"]
        merged["llm_rationale"] = review["rationale"]
        merged["llm_based_on_provided_evidence"] = True
        merged["llm_calculation_performed"] = False
        reviewed_actions.append(merged)

    reviewed_payload = {
        "review_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "review_model": model,
        "review_mode": "interpretation_only_no_calculations",
        "source_actions_file": str(latest_actions_path),
        "source_run_timestamp_utc": actions_payload.get("run_timestamp_utc"),
        "actions": reviewed_actions,
        "scored_count": actions_payload.get("scored_count"),
    }

    out_path = latest_actions_path.parent / "model_v0_reviewed.json"
    out_path.write_text(json.dumps(reviewed_payload, indent=2), encoding="utf-8")
    print(f"review_output={out_path}")
    print(f"reviewed_symbols={len(reviewed_actions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
