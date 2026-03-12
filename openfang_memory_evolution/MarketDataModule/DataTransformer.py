from __future__ import annotations

from dataclasses import dataclass

from .DataProcessor import ProcessedMarketData


@dataclass
class TransformedMarketContext:
    symbol: str
    market_regime: str
    latest_price: float
    avg_volume: float
    volatility: float
    mean_return: float


class DataTransformer:
    def transform(self, processed: ProcessedMarketData) -> TransformedMarketContext:
        latest_price = processed.prices[-1]
        avg_volume = sum(processed.volumes) / max(1, len(processed.volumes))
        mean_return = sum(processed.returns) / max(1, len(processed.returns))
        variance = sum((r - mean_return) ** 2 for r in processed.returns) / max(
            1, len(processed.returns)
        )
        volatility = variance**0.5

        return TransformedMarketContext(
            symbol=processed.symbol,
            market_regime=processed.market_regime,
            latest_price=latest_price,
            avg_volume=avg_volume,
            volatility=volatility,
            mean_return=mean_return,
        )
