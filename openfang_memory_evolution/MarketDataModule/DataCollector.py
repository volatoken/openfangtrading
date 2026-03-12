from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import random


@dataclass
class MarketSnapshot:
    symbol: str
    timestamp: datetime
    prices: list[float]
    volumes: list[float]
    market_regime: str


class DataCollector:
    """Collect market snapshots. Replace synthetic generator with exchange APIs in production."""

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)

    def collect(self, symbol: str, lookback: int = 60) -> MarketSnapshot:
        base_price = 30000 if symbol.upper().startswith("BTC") else 2000
        drift = self._rng.uniform(-50, 80)
        prices: list[float] = []
        volumes: list[float] = []
        current = base_price + self._rng.uniform(-1000, 1000)

        for _ in range(lookback):
            shock = self._rng.uniform(-120, 120)
            current = max(50.0, current + drift / lookback + shock)
            prices.append(round(current, 2))
            volumes.append(round(self._rng.uniform(20, 400), 2))

        if prices[-1] > prices[0] * 1.03:
            regime = "bull"
        elif prices[-1] < prices[0] * 0.97:
            regime = "bear"
        else:
            regime = "sideway"

        return MarketSnapshot(
            symbol=symbol.upper(),
            timestamp=datetime.now(tz=timezone.utc),
            prices=prices,
            volumes=volumes,
            market_regime=regime,
        )
