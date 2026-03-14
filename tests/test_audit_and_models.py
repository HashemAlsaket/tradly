from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone

from tradly.agents.audit import aggregate_action_safe
from tradly.config.model_suite import load_openai_model_suite
from tradly.schemas.run_manifest import AuditStatus, RunManifest


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

    def test_audit_status_rejects_invalid_decision_values(self) -> None:
        with self.assertRaises(ValueError):
            AuditStatus(
                data_audit="pass",
                calculation_audit="unknown",  # type: ignore[arg-type]
                decision_audit="fail",
            )

    def test_run_manifest_rejects_invalid_run_type(self) -> None:
        status = AuditStatus(
            data_audit="pass",
            calculation_audit="pass",
            decision_audit="pass",
        )
        with self.assertRaises(ValueError):
            RunManifest(
                run_id="run-1",
                run_type="hourly",  # type: ignore[arg-type]
                started_at=datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc),
                completed_at=datetime(2026, 3, 6, 15, 1, tzinfo=timezone.utc),
                broker_state_freshness_seconds=10,
                input_snapshots=["snap-1"],
                audit_status=status,
                action_safe=True,
            )


if __name__ == "__main__":
    unittest.main()
