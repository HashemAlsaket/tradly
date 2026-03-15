from __future__ import annotations

import unittest

from dashboard.app import _compute_system_state


class DashboardStateTests(unittest.TestCase):
    def _base_kwargs(self) -> dict:
        return {
            "freshness_snapshot": {
                "overall_status": "PASS",
                "written_at_utc": "2026-03-14T21:30:00+00:00",
            },
            "market_payload": {
                "run_timestamp_utc": "2026-03-14T21:04:37+00:00",
                "quality_audit": {"status": "pass"},
                "rows": [{"scope_id": "US_BROAD_MARKET"}],
            },
            "sector_payload": {
                "run_timestamp_utc": "2026-03-14T21:21:45+00:00",
                "quality_audit": {"status": "pass"},
                "rows": [{"scope_id": "Technology"}],
            },
            "symbol_payload": {
                "run_timestamp_utc": "2026-03-14T21:25:00+00:00",
                "quality_audit": {"status": "pass"},
                "rows": [{"scope_id": "NVDA"}],
            },
            "symbol_news_payload": {
                "run_timestamp_utc": "2026-03-14T21:26:00+00:00",
                "quality_audit": {"status": "pass"},
                "input_audit": {"status": "ready"},
                "rows": [{"scope_id": "NVDA"}],
            },
            "sector_news_payload": {
                "run_timestamp_utc": "2026-03-14T21:27:00+00:00",
                "quality_audit": {"status": "pass"},
                "input_audit": {"status": "ready"},
                "rows": [{"scope_id": "Technology"}],
            },
            "range_payload": {
                "run_timestamp_utc": "2026-03-14T21:28:00+00:00",
                "quality_audit": {"status": "pass"},
                "rows": [{"scope_id": "NVDA"}],
            },
            "ensemble_payload": {
                "run_timestamp_utc": "2026-03-14T21:29:00+00:00",
                "quality_audit": {"status": "pass"},
                "input_audit": {"status": "ready"},
                "rows": [{"scope_id": "NVDA"}],
            },
        }

    def test_ready_when_freshness_and_specialist_artifacts_are_aligned(self) -> None:
        state, reasons, warnings = _compute_system_state(**self._base_kwargs())

        self.assertEqual(state, "ready")
        self.assertEqual(reasons, [])
        self.assertEqual(warnings, [])

    def test_blocks_when_freshness_snapshot_is_older_than_latest_model_run(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["freshness_snapshot"] = {
            "overall_status": "PASS",
            "written_at_utc": "2026-03-14T02:02:52+00:00",
        }
        state, reasons, warnings = _compute_system_state(**kwargs)

        self.assertEqual(state, "blocked")
        self.assertIn("freshness_snapshot_outdated_for_latest_model_runs", reasons)
        self.assertEqual(warnings, [])

    def test_research_only_when_ensemble_or_news_inputs_are_thin(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["ensemble_payload"]["input_audit"] = {"status": "thin_evidence"}
        state, reasons, warnings = _compute_system_state(**kwargs)

        self.assertEqual(state, "research_only")
        self.assertEqual(reasons, [])
        self.assertIn("ensemble_thin_evidence", warnings)

    def test_blocks_when_symbol_or_sector_news_rows_are_missing(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["symbol_news_payload"]["rows"] = []
        kwargs["sector_news_payload"]["rows"] = []
        state, reasons, warnings = _compute_system_state(**kwargs)

        self.assertEqual(state, "blocked")
        self.assertIn("symbol_news_missing", reasons)
        self.assertIn("sector_news_missing", reasons)
        self.assertEqual(warnings, [])


if __name__ == "__main__":
    unittest.main()
