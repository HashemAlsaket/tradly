from __future__ import annotations

from datetime import datetime, timezone
import unittest

from dashboard.app import _portfolio_rows
from tradly.models.portfolio_policy import _theme_from_symbol_meta, build_portfolio_policy, validate_portfolio_snapshot


def _market_payload(*, macro_state: str = "macro_unstable", signal_direction: str = "bearish", confidence: int = 80) -> dict:
    return {
        "rows": [
            {
                "scope_id": "US_BROAD_MARKET",
                "signal_direction": signal_direction,
                "confidence_score": confidence,
                "evidence": {
                    "macro_hostility": {
                        "macro_state": macro_state,
                    }
                },
            }
        ]
    }


def _freshness_snapshot(*, policy: str = "after_hours_relaxed", short_ready: bool = True, medium_ready: bool = True) -> dict:
    session = "after_hours" if policy in {"after_hours_relaxed", "premarket_tradable"} else "market_hours"
    return {
        "freshness": {
            "freshness_policy": policy,
            "market_session_state": session,
            "metrics": {
                "short_horizon_execution_ready": short_ready,
                "medium_horizon_thesis_usable": medium_ready,
            },
            "checks": [],
        }
    }


def _portfolio_snapshot(*, open_orders: list[dict] | None = None) -> dict:
    return {
        "as_of_utc": "2026-03-16T20:55:00+00:00",
        "base_currency": "USD",
        "cash_available": 50000.0,
        "net_liquidation_value": 100000.0,
        "positions": [
            {
                "symbol": "BAC",
                "shares": 100,
                "market_value": 7000.0,
                "cost_basis": 6500.0,
                "unrealized_pnl": 500.0,
            },
            {
                "symbol": "XYZ",
                "shares": 50,
                "market_value": 2500.0,
                "cost_basis": 3000.0,
                "unrealized_pnl": -500.0,
            },
        ],
        "open_orders": open_orders or [],
    }


def _universe_registry() -> dict:
    return {
        "symbols": [
            {
                "symbol": "CVX",
                "active": True,
                "model": True,
                "asset_type": "stock",
                "sector": "Energy",
                "industry": "Oil & Gas Integrated",
                "roles": ["sector_leader"],
            },
            {
                "symbol": "CRM",
                "active": True,
                "model": True,
                "asset_type": "stock",
                "sector": "Technology",
                "industry": "Software - Application",
                "roles": ["sector_leader"],
            },
            {
                "symbol": "NFLX",
                "active": True,
                "model": True,
                "asset_type": "stock",
                "sector": "Communication Services",
                "industry": "Entertainment",
                "roles": ["sector_leader"],
            },
            {
                "symbol": "BAC",
                "active": True,
                "model": True,
                "asset_type": "stock",
                "sector": "Financial Services",
                "industry": "Banks - Diversified",
                "roles": ["sector_leader"],
            },
        ]
    }


def _recommendation_payload() -> dict:
    return {
        "rows": [
            {
                "scope_id": "CVX",
                "symbol": "CVX",
                "recommended_action": "Buy",
                "recommended_horizon": "1to3d",
                "recommendation_class": "aligned_long",
                "confidence_score": 77,
                "score_normalized": 77.0,
                "execution_ready": True,
                "source_state": "actionable",
                "primary_reason_code": "market_context_headwind",
                "why_code": ["market_context_headwind"],
            },
            {
                "scope_id": "CRM",
                "symbol": "CRM",
                "recommended_action": "Buy",
                "recommended_horizon": "2to6w",
                "recommendation_class": "mixed_weak_long",
                "confidence_score": 54,
                "score_normalized": 58.0,
                "execution_ready": True,
                "source_state": "actionable",
                "primary_reason_code": "market_context_headwind",
                "why_code": ["market_context_headwind"],
            },
            {
                "scope_id": "NFLX",
                "symbol": "NFLX",
                "recommended_action": "Watch Buy",
                "recommended_horizon": "1to3d",
                "recommendation_class": "watch_long",
                "confidence_score": 65,
                "score_normalized": 65.0,
                "execution_ready": True,
                "source_state": "research_only",
                "primary_reason_code": "market_context_headwind",
                "why_code": ["market_context_headwind"],
            },
            {
                "scope_id": "BAC",
                "symbol": "BAC",
                "recommended_action": "Sell/Trim",
                "recommended_horizon": "1to3d",
                "recommendation_class": "aligned_short",
                "confidence_score": 85,
                "score_normalized": -75.0,
                "execution_ready": True,
                "source_state": "actionable",
                "primary_reason_code": "market_context_headwind",
                "why_code": ["market_context_headwind"],
            },
        ]
    }


def _review_payload() -> dict:
    return {
        "rows": [
            {
                "scope_id": "CVX",
                "review_disposition": "promote",
                "review_bucket": "top_longs",
                "review_reason_code": "regime_aligned_actionable",
            },
            {
                "scope_id": "CRM",
                "review_disposition": "review_required",
                "review_bucket": "manual_review",
                "review_reason_code": "mixed_setup",
            },
            {
                "scope_id": "NFLX",
                "review_disposition": "watch",
                "review_bucket": "watchlist",
                "review_reason_code": "recommendation_not_actionable",
            },
            {
                "scope_id": "BAC",
                "review_disposition": "promote",
                "review_bucket": "top_shorts",
                "review_reason_code": "regime_aligned_actionable",
            },
        ]
    }


def _event_risk_payload() -> dict:
    return {"rows": []}


class PortfolioPolicyTests(unittest.TestCase):
    def test_healthcare_is_first_class_theme_not_generic_defensive(self) -> None:
        self.assertEqual(
            _theme_from_symbol_meta(
                {
                    "asset_type": "stock",
                    "sector": "Healthcare",
                    "industry": "Drug Manufacturers - General",
                    "roles": ["core_leader", "pharma_defensive"],
                },
                "JNJ",
            ),
            "healthcare",
        )

    def test_snapshot_validation_flags_invalid_nav_and_unmanaged(self) -> None:
        validation = validate_portfolio_snapshot(
            {
                "as_of_utc": "2026-03-16T20:55:00+00:00",
                "base_currency": "USD",
                "cash_available": 0,
                "net_liquidation_value": 0,
                "positions": [
                    {
                        "symbol": "XYZ",
                        "shares": 1,
                        "market_value": 10,
                        "cost_basis": 9,
                        "unrealized_pnl": 1,
                    }
                ],
                "open_orders": [],
            },
            active_universe={"CVX"},
        )
        self.assertFalse(validation.valid)
        self.assertIn("invalid_net_liquidation_value", validation.failure_reasons)
        self.assertEqual(validation.unmanaged_symbols, ["XYZ"])

    def test_build_portfolio_policy_generates_tiers_actions_and_deferred_state(self) -> None:
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(),
            recommendation_payload=_recommendation_payload(),
            review_payload=_review_payload(),
            event_risk_payload=_event_risk_payload(),
            freshness_snapshot=_freshness_snapshot(),
            portfolio_snapshot=_portfolio_snapshot(),
            universe_registry=_universe_registry(),
            now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(payload["portfolio_mode"], "neutral")
        rows_by_symbol = {row["symbol"]: row for row in payload["rows"]}

        self.assertEqual(rows_by_symbol["CVX"]["idea_tier"], "tier_1_best")
        self.assertEqual(rows_by_symbol["CVX"]["action_recommendation"], "buy")
        self.assertTrue(rows_by_symbol["CVX"]["deferred_to_next_cash_session"])

        self.assertEqual(rows_by_symbol["CRM"]["idea_tier"], "tier_2_conditional")
        self.assertIn(rows_by_symbol["CRM"]["action_recommendation"], {"buy", "do_not_trade"})

        self.assertEqual(rows_by_symbol["NFLX"]["idea_tier"], "tier_3_probe")
        self.assertEqual(rows_by_symbol["NFLX"]["action_execution_state"], "informational")

        self.assertEqual(rows_by_symbol["BAC"]["action_recommendation"], "exit")
        self.assertEqual(rows_by_symbol["BAC"]["action_execution_state"], "urgent_exit")

        self.assertFalse(rows_by_symbol["XYZ"]["managed_symbol"])
        self.assertEqual(rows_by_symbol["XYZ"]["action_recommendation"], "do_not_trade")

    def test_strict_blocker_pushes_mode_to_risk_off(self) -> None:
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(),
            recommendation_payload=_recommendation_payload(),
            review_payload=_review_payload(),
            event_risk_payload=_event_risk_payload(),
            freshness_snapshot=_freshness_snapshot(policy="market_hours_strict", short_ready=False, medium_ready=True),
            portfolio_snapshot=_portfolio_snapshot(),
            universe_registry=_universe_registry(),
            now_utc=datetime(2026, 3, 16, 16, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(payload["portfolio_mode"], "risk_off")

    def test_dashboard_portfolio_rows_expose_action_fields(self) -> None:
        rows = _portfolio_rows(
            {
                "rows": [
                    {
                        "symbol": "CVX",
                        "action_recommendation": "buy",
                        "current_weight": 0.0,
                        "target_weight": 0.03,
                        "weight_delta": 0.03,
                        "idea_tier": "tier_1_best",
                        "theme_id": "energy",
                        "horizon_bucket": "tactical_1_3d",
                        "action_execution_state": "deferred_to_next_cash_session",
                        "action_size_recommendation": "buy 3.0% (~$3,000)",
                        "policy_reason_codes": ["promoted_strong_long"],
                        "policy_blockers": [],
                        "confidence_score": 77,
                    }
                ]
            }
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Symbol"], "CVX")
        self.assertEqual(rows[0]["Action"], "buy")
        self.assertEqual(rows[0]["ExecState"], "deferred_to_next_cash_session")

    def test_max_active_positions_is_enforced(self) -> None:
        universe = {
            "symbols": [
                {
                    "symbol": f"S{i}",
                    "active": True,
                    "model": True,
                    "asset_type": "stock",
                    "sector": f"Sector{i}",
                    "industry": "Software",
                    "roles": ["sector_leader"],
                }
                for i in range(20)
            ]
        }
        recommendations = {
            "rows": [
                {
                    "scope_id": f"S{i}",
                    "symbol": f"S{i}",
                    "recommended_action": "Buy",
                    "recommended_horizon": "2to6w",
                    "recommendation_class": "aligned_long",
                    "confidence_score": 80,
                    "score_normalized": 80.0,
                    "execution_ready": True,
                    "source_state": "actionable",
                    "primary_reason_code": "market_context_supportive",
                    "why_code": ["market_context_supportive"],
                }
                for i in range(20)
            ]
        }
        reviews = {
            "rows": [
                {
                    "scope_id": f"S{i}",
                    "review_disposition": "promote",
                    "review_bucket": "top_longs",
                    "review_reason_code": "regime_aligned_actionable",
                }
                for i in range(20)
            ]
        }
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(macro_state="risk_on_confirmed", signal_direction="bullish", confidence=82),
            recommendation_payload=recommendations,
            review_payload=reviews,
            event_risk_payload=_event_risk_payload(),
            freshness_snapshot=_freshness_snapshot(policy="market_hours_strict", short_ready=True, medium_ready=True),
            portfolio_snapshot={
                "as_of_utc": "2026-03-16T20:55:00+00:00",
                "base_currency": "USD",
                "cash_available": 100000.0,
                "net_liquidation_value": 100000.0,
                "positions": [],
                "open_orders": [],
            },
            universe_registry=universe,
            now_utc=datetime(2026, 3, 16, 16, 0, tzinfo=timezone.utc),
        )
        active_targets = [row for row in payload["rows"] if row["target_weight"] > 0]
        self.assertEqual(payload["portfolio_mode"], "risk_on")
        self.assertLessEqual(len(active_targets), payload["portfolio_constraints"]["max_active_positions"])

    def test_unmanaged_holdings_stay_in_target_exposure(self) -> None:
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(),
            recommendation_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "symbol": "CVX",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to3d",
                        "recommendation_class": "aligned_long",
                        "confidence_score": 80,
                        "score_normalized": 80.0,
                        "execution_ready": True,
                        "source_state": "actionable",
                        "primary_reason_code": "market_context_headwind",
                        "why_code": ["market_context_headwind"],
                    }
                ]
            },
            review_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "review_disposition": "promote",
                        "review_bucket": "top_longs",
                        "review_reason_code": "regime_aligned_actionable",
                    }
                ]
            },
            event_risk_payload=_event_risk_payload(),
            freshness_snapshot=_freshness_snapshot(),
            portfolio_snapshot={
                "as_of_utc": "2026-03-16T20:55:00+00:00",
                "base_currency": "USD",
                "cash_available": 50000.0,
                "net_liquidation_value": 100000.0,
                "positions": [
                    {
                        "symbol": "ZZZ",
                        "shares": 10,
                        "market_value": 15000.0,
                        "cost_basis": 12000.0,
                        "unrealized_pnl": 3000.0,
                    }
                ],
                "open_orders": [],
            },
            universe_registry={"symbols": [{"symbol": "CVX", "active": True, "model": True, "asset_type": "stock", "sector": "Energy", "industry": "Oil & Gas", "roles": ["sector_leader"]}]},
            now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
        )
        self.assertIn("symbol_zzz", payload["theme_exposure"]["target"])
        self.assertGreaterEqual(payload["target_gross_long_exposure"], 0.15)

    def test_open_buy_order_reserves_cash_and_blocks_duplicate_buy(self) -> None:
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(),
            recommendation_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "symbol": "CVX",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to3d",
                        "recommendation_class": "aligned_long",
                        "confidence_score": 80,
                        "score_normalized": 80.0,
                        "execution_ready": True,
                        "source_state": "actionable",
                        "primary_reason_code": "market_context_headwind",
                        "why_code": ["market_context_headwind"],
                    }
                ]
            },
            review_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "review_disposition": "promote",
                        "review_bucket": "top_longs",
                        "review_reason_code": "regime_aligned_actionable",
                    }
                ]
            },
            event_risk_payload=_event_risk_payload(),
            freshness_snapshot=_freshness_snapshot(),
            portfolio_snapshot=_portfolio_snapshot(
                open_orders=[
                    {
                        "symbol": "CVX",
                        "side": "buy",
                        "shares": 10,
                        "limit_price": 100.0,
                        "status": "open",
                    }
                ]
            ),
            universe_registry=_universe_registry(),
            now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
        )
        row = next(row for row in payload["rows"] if row["symbol"] == "CVX")
        self.assertEqual(row["action_recommendation"], "do_not_trade")
        self.assertIn("active_buy_order_exists", row["policy_blockers"])
        self.assertEqual(payload["available_cash"], 49000.0)

    def test_event_risk_blocks_new_buy_and_adds_reason_codes(self) -> None:
        payload = build_portfolio_policy(
            market_regime_payload=_market_payload(signal_direction="bullish", confidence=78),
            recommendation_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "symbol": "CVX",
                        "recommended_action": "Buy",
                        "recommended_horizon": "1to3d",
                        "recommendation_class": "aligned_long",
                        "confidence_score": 81,
                        "score_normalized": 81.0,
                        "execution_ready": True,
                        "source_state": "actionable",
                        "primary_reason_code": "market_context_supportive",
                        "why_code": ["market_context_supportive"],
                    }
                ]
            },
            review_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "review_disposition": "promote",
                        "review_bucket": "top_longs",
                        "review_reason_code": "event_buy_capped_to_watch",
                    }
                ]
            },
            event_risk_payload={
                "rows": [
                    {
                        "scope_id": "CVX",
                        "event_active": True,
                        "reaction_state": "beat_but_rejected",
                        "reaction_severity": "high",
                        "action_bias": "downgrade",
                        "hard_cap_buy_to_watch": True,
                    }
                ]
            },
            freshness_snapshot=_freshness_snapshot(),
            portfolio_snapshot={
                "as_of_utc": "2026-03-16T20:55:00+00:00",
                "base_currency": "USD",
                "cash_available": 100000.0,
                "net_liquidation_value": 100000.0,
                "positions": [],
                "open_orders": [],
            },
            universe_registry=_universe_registry(),
            now_utc=datetime(2026, 3, 16, 21, 0, tzinfo=timezone.utc),
        )
        row = next(row for row in payload["rows"] if row["symbol"] == "CVX")
        self.assertEqual(row["idea_tier"], "tier_blocked")
        self.assertEqual(row["action_recommendation"], "do_not_trade")
        self.assertIn("event_reaction_damage", row["policy_blockers"])
        self.assertIn("beat_but_rejected", row["policy_reason_codes"])


if __name__ == "__main__":
    unittest.main()
