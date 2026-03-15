from __future__ import annotations

import unittest

from tradly.pipeline.cycle import STEPS


class CycleStepOrderTests(unittest.TestCase):
    def test_refresh_market_calendar_context_runs_before_models(self) -> None:
        step_names = [name for name, _ in STEPS]

        self.assertIn("refresh_market_calendar_context", step_names)
        self.assertIn("interpret_news_llm", step_names)
        self.assertIn("run_market_regime", step_names)

        self.assertLess(
            step_names.index("interpret_news_llm"),
            step_names.index("refresh_market_calendar_context"),
        )
        self.assertLess(
            step_names.index("refresh_market_calendar_context"),
            step_names.index("run_market_regime"),
        )


if __name__ == "__main__":
    unittest.main()
