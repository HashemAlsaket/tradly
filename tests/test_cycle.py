from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from tradly.pipeline import cycle


class CycleStepOrderTests(unittest.TestCase):
    def test_refresh_market_calendar_context_runs_before_models(self) -> None:
        step_names = [name for name, _ in cycle.STEPS]

        self.assertIn("refresh_market_calendar_context", step_names)
        self.assertIn("interpret_news_llm", step_names)
        self.assertIn("run_market_regime", step_names)
        self.assertIn("run_recommendation", step_names)
        self.assertIn("run_recommendation_scorecard", step_names)
        self.assertIn("run_recommendation_scorecard_history", step_names)
        self.assertIn("run_recommendation_review", step_names)
        self.assertIn("run_event_risk", step_names)
        self.assertIn("run_ensemble", step_names)

        self.assertLess(
            step_names.index("interpret_news_llm"),
            step_names.index("refresh_market_calendar_context"),
        )
        self.assertLess(
            step_names.index("refresh_market_calendar_context"),
            step_names.index("run_market_regime"),
        )
        self.assertLess(
            step_names.index("run_ensemble"),
            step_names.index("run_recommendation"),
        )
        self.assertLess(
            step_names.index("run_recommendation"),
            step_names.index("run_event_risk"),
        )
        self.assertLess(
            step_names.index("run_event_risk"),
            step_names.index("run_recommendation_scorecard"),
        )
        self.assertLess(
            step_names.index("run_recommendation_scorecard"),
            step_names.index("run_recommendation_scorecard_history"),
        )
        self.assertLess(
            step_names.index("run_recommendation_scorecard_history"),
            step_names.index("run_recommendation_review"),
        )

    def test_cycle_runs_preflight_by_default(self) -> None:
        class FakeResult:
            def __init__(self, returncode=0, stdout='{"actions":["seed_macro_fred"],"final_lags":[{"source":"macro_points","status":"warning"}]}', stderr=""):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        with patch.object(cycle, "get_repo_root", return_value=Path("/tmp/tradly")), \
            patch.object(cycle.subprocess, "run", return_value=FakeResult()) as run_proc, \
            patch.object(cycle, "_run_step", return_value=0) as run_step, \
            patch.object(cycle, "run_and_write_runtime_freshness_snapshot", return_value=(0, "", "", {})) as freshness_mock, \
            patch.dict(cycle.os.environ, {}, clear=True):
            rc = cycle.main()

        self.assertEqual(rc, 0)
        run_proc.assert_called_once()
        self.assertGreaterEqual(run_step.call_count, 1)
        first_call = run_step.call_args_list[0]
        self.assertEqual(first_call.args[0], cycle.STEPS[0][0])
        freshness_mock.assert_called_once()
        kwargs = freshness_mock.call_args.kwargs
        self.assertEqual(kwargs["preflight_actions"], ["seed_macro_fred"])
        self.assertEqual(kwargs["preflight_lags"], [{"source": "macro_points", "status": "warning"}])

    def test_cycle_skips_preflight_when_env_requests_it(self) -> None:
        with patch.object(cycle, "get_repo_root", return_value=Path("/tmp/tradly")), \
            patch.object(cycle, "_run_step", return_value=0) as run_step, \
            patch.object(cycle, "run_and_write_runtime_freshness_snapshot", return_value=(0, "", "", {})), \
            patch.dict(cycle.os.environ, {cycle.SKIP_PREFLIGHT_ENV: "1"}, clear=True):
            rc = cycle.main()

        self.assertEqual(rc, 0)
        self.assertGreaterEqual(run_step.call_count, 1)
        first_call = run_step.call_args_list[0]
        self.assertEqual(first_call.args[0], cycle.STEPS[0][0])



if __name__ == "__main__":
    unittest.main()
