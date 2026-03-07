from __future__ import annotations

from dataclasses import dataclass

from tradly.schemas.common import CanonicalRecord


@dataclass(frozen=True)
class BrokerAccountSnapshot(CanonicalRecord):
    account_id: str
    equity: float
    cash: float
    buying_power: float
    day_pnl: float
    total_pnl: float
    open_orders_count: int
    portfolio_drawdown_pct: float

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.account_id:
            raise ValueError("account_id must not be empty")
        if self.open_orders_count < 0:
            raise ValueError("open_orders_count must be >= 0")


@dataclass(frozen=True)
class BrokerPositionSnapshot(CanonicalRecord):
    account_id: str
    symbol: str
    quantity: float
    avg_cost: float
    market_price: float
    market_value: float
    weight_pct: float
    unrealized_pnl: float
    realized_pnl: float
    sector: str
    is_borderline_universe: bool = False

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.account_id:
            raise ValueError("account_id must not be empty")
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not self.sector:
            raise ValueError("sector must not be empty")
