from __future__ import annotations

import unittest

from tradly.pipeline.review_actions_llm import _validate_decisions


class ReviewActionsLLMTests(unittest.TestCase):
    def test_validate_decisions_accepts_confidence_score(self) -> None:
        actions_payload = {
            "actions": [
                {
                    "symbol": "MU",
                    "investability_blocked": False,
                    "hard_downgrade_reason": None,
                }
            ],
            "news_guardrails": {"coverage_blocked": False},
        }
        llm_payload = {
            "decisions": [
                {
                    "symbol": "MU",
                    "llm_action": "Buy",
                    "confidence_score": 73,
                    "confidence_label": "high",
                    "rationale": "Evidence supports a buy.",
                    "based_on_provided_evidence": True,
                    "calculation_performed": False,
                }
            ]
        }

        out = _validate_decisions(actions_payload=actions_payload, llm_payload=llm_payload)
        self.assertEqual(out["MU"]["confidence_score"], 73)

    def test_validate_decisions_rejects_missing_confidence_score(self) -> None:
        actions_payload = {
            "actions": [
                {
                    "symbol": "MU",
                    "investability_blocked": False,
                    "hard_downgrade_reason": None,
                }
            ],
            "news_guardrails": {"coverage_blocked": False},
        }
        llm_payload = {
            "decisions": [
                {
                    "symbol": "MU",
                    "llm_action": "Buy",
                    "confidence_label": "high",
                    "rationale": "Evidence supports a buy.",
                    "based_on_provided_evidence": True,
                    "calculation_performed": False,
                }
            ]
        }

        with self.assertRaises(RuntimeError):
            _validate_decisions(actions_payload=actions_payload, llm_payload=llm_payload)


if __name__ == "__main__":
    unittest.main()
