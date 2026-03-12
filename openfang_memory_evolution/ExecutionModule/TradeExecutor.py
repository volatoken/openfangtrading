from __future__ import annotations

from dataclasses import dataclass
import random

from .APIHandler import APIHandler


@dataclass
class ExecutionResult:
    status: str
    filled_price: float
    pnl: float


class TradeExecutor:
    def __init__(self, api_handler: APIHandler, seed: int = 7) -> None:
        self.api_handler = api_handler
        self._rng = random.Random(seed)

    def execute(
        self,
        symbol: str,
        action: str,
        market_context: dict[str, float | str],
        quantity: float = 0.001,
    ) -> ExecutionResult:
        price = float(market_context.get("latest_price", 0.0))
        if action == "HOLD":
            return ExecutionResult(status="SKIPPED", filled_price=price, pnl=0.0)

        response = self.api_handler.place_order(symbol=symbol, side=action, quantity=quantity, price=price)
        regime = str(market_context.get("market_regime", "sideway"))
        noise = self._rng.uniform(-0.01, 0.01)
        directional_edge = 0.0

        if action == "BUY":
            directional_edge = 0.006 if regime == "bull" else (-0.003 if regime == "bear" else 0.001)
        elif action == "SELL":
            directional_edge = 0.006 if regime == "bear" else (-0.003 if regime == "bull" else 0.001)

        ret = directional_edge + noise
        pnl = price * quantity * ret
        return ExecutionResult(status=response.status, filled_price=response.filled_price, pnl=pnl)
