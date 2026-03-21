from __future__ import annotations

import unittest

from tradly.services.event_price_context import build_event_price_context


class EventPriceContextTests(unittest.TestCase):
    def test_build_event_price_context_computes_relative_moves(self) -> None:
        context = build_event_price_context(
            symbol="MU",
            snapshot_by_symbol={
                "MU": {"last_trade_price": 100.0, "prev_close": 105.0, "change_pct": -4.7619, "session_close": 100.0},
                "QQQ": {"change_pct": -1.2},
                "XLK": {"change_pct": -2.0},
            },
            symbol_metadata={"MU": {"sector": "Technology"}},
            market_row={
                "signal_direction": "bearish",
                "lane_diagnostics": {"near_term": {"market_session_state": "after_hours"}},
                "evidence": {"macro_hostility": {"macro_state": "macro_unstable"}},
            },
            recommendation_row={"recommended_action": "Buy", "confidence_score": 81},
        )

        self.assertAlmostEqual(context.price_reaction_pct or 0.0, -4.7619, places=3)
        self.assertAlmostEqual(context.move_vs_qqq_pct or 0.0, -3.5619, places=3)
        self.assertAlmostEqual(context.move_vs_sector_pct or 0.0, -2.7619, places=3)
        self.assertEqual(context.market_session_state, "after_hours")
        self.assertEqual(context.sector_proxy_symbol, "XLK")
        self.assertEqual(context.current_action, "Buy")
        self.assertEqual(context.current_confidence, 81)


if __name__ == "__main__":
    unittest.main()
