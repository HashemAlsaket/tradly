from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tradly.services.freshness_gate import evaluate_broker_state_freshness


class FreshnessGateTests(unittest.TestCase):
    def test_freshness_gate_passes_inside_sla(self) -> None:
        now = datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc)
        as_of = now - timedelta(seconds=90)

        result = evaluate_broker_state_freshness(as_of, now=now)

        self.assertTrue(result.is_fresh)
        self.assertEqual(result.reason, "broker_state_fresh")

    def test_freshness_gate_blocks_outside_sla(self) -> None:
        now = datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc)
        as_of = now - timedelta(seconds=121)

        result = evaluate_broker_state_freshness(as_of, now=now)

        self.assertFalse(result.is_fresh)
        self.assertEqual(result.reason, "broker_state_stale_blocking_recommendations")

    def test_freshness_gate_requires_timezone_aware_inputs(self) -> None:
        now = datetime(2026, 3, 6, 15, 0)
        as_of = datetime(2026, 3, 6, 14, 58)

        with self.assertRaises(ValueError):
            evaluate_broker_state_freshness(as_of, now=now)


if __name__ == "__main__":
    unittest.main()
