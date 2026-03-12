from __future__ import annotations

from dataclasses import dataclass

from openfang_memory_evolution.MarketDataModule.DataProcessor import ProcessedMarketData


@dataclass
class IndicatorSet:
    rsi: float
    macd: float
    signal: float
    histogram: float


class IndicatorCalculator:
    def calculate(self, processed: ProcessedMarketData) -> IndicatorSet:
        rsi = self._rsi(processed.prices)
        macd, signal = self._macd(processed.prices)
        return IndicatorSet(
            rsi=rsi,
            macd=macd,
            signal=signal,
            histogram=macd - signal,
        )

    def _rsi(self, prices: list[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0

        gains = 0.0
        losses = 0.0
        for i in range(-period, 0):
            diff = prices[i] - prices[i - 1]
            if diff >= 0:
                gains += diff
            else:
                losses -= diff

        avg_gain = gains / period
        avg_loss = losses / period if losses else 1e-9
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _ema(self, values: list[float], period: int) -> float:
        if not values:
            return 0.0
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema

    def _macd(self, prices: list[float]) -> tuple[float, float]:
        if len(prices) < 26:
            return 0.0, 0.0

        ema12 = self._ema(prices, 12)
        ema26 = self._ema(prices, 26)
        macd = ema12 - ema26

        macd_line_history: list[float] = []
        for i in range(26, len(prices) + 1):
            w = prices[:i]
            macd_line_history.append(self._ema(w, 12) - self._ema(w, 26))

        signal = self._ema(macd_line_history, 9) if macd_line_history else 0.0
        return macd, signal
