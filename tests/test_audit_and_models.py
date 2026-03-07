from __future__ import annotations

import os
import unittest

from tradly.agents.audit import aggregate_action_safe
from tradly.config.model_suite import load_openai_model_suite
from tradly.schemas.run_manifest import AuditStatus


class AuditAndModelsTests(unittest.TestCase):
    def test_aggregate_action_safe_false_when_any_audit_fails(self) -> None:
        status = AuditStatus(
            data_audit="pass",
            calculation_audit="fail",
            decision_audit="pass",
        )

        self.assertFalse(aggregate_action_safe(status))

    def test_load_openai_model_suite_defaults(self) -> None:
        old_values = {
            "OPENAI_LLM_MODEL": os.environ.pop("OPENAI_LLM_MODEL", None),
            "OPENAI_VLM_MODEL": os.environ.pop("OPENAI_VLM_MODEL", None),
            "OPENAI_STT_MODEL": os.environ.pop("OPENAI_STT_MODEL", None),
        }
        try:
            suite = load_openai_model_suite()
            self.assertTrue(bool(suite.llm_model))
            self.assertTrue(bool(suite.vlm_model))
            self.assertTrue(bool(suite.stt_model))
        finally:
            for key, value in old_values.items():
                if value is not None:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
