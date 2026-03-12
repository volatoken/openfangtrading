from __future__ import annotations

from dataclasses import dataclass

from .DataCollector import MarketSnapshot


@dataclass
class ProcessedMarketData:
    symbol: str
    prices: list[float]
    volumes: list[float]
    returns: list[float]
    market_regime: str


class DataProcessor:
    def process(self, snapshot: MarketSnapshot) -> ProcessedMarketData:
        returns: list[float] = [0.0]
        for i in range(1, len(snapshot.prices)):
            prev = snapshot.prices[i - 1]
            curr = snapshot.prices[i]
            returns.append((curr - prev) / prev if prev else 0.0)

        return ProcessedMarketData(
            symbol=snapshot.symbol,
            prices=snapshot.prices,
            volumes=snapshot.volumes,
            returns=returns,
            market_regime=snapshot.market_regime,
        )
