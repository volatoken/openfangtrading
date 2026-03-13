from __future__ import annotations

from dataclasses import dataclass
import math

from openfang_memory_evolution.MarketDataModule.DataCollector import OptionBubble
from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


@dataclass
class OptionFlowSignal:
    dominant_timeframe: str
    dominant_expiry: str
    anomaly_timeframe: str
    anomaly_score: float
    timeframe_premium: dict[str, float]


class OptionFlowAnalyzer:
    def __init__(self, sqlite_handler: SQLiteMemoryHandler) -> None:
        self.sqlite_handler = sqlite_handler

    def analyze(self, symbol: str, bubbles: list[OptionBubble]) -> OptionFlowSignal:
        timeframe_premium: dict[str, float] = {"0DTE": 0.0, "weekly": 0.0, "monthly": 0.0}
        expiry_premium: dict[tuple[str, str], float] = {}

        for bubble in bubbles:
            timeframe_premium[bubble.timeframe] = (
                timeframe_premium.get(bubble.timeframe, 0.0) + bubble.premium_usd
            )
            key = (bubble.timeframe, bubble.expiry)
            expiry_premium[key] = expiry_premium.get(key, 0.0) + bubble.premium_usd

        dominant_key = max(expiry_premium.items(), key=lambda x: x[1])[0] if expiry_premium else ("weekly", "")
        dominant_timeframe, dominant_expiry = dominant_key

        anomaly_timeframe = dominant_timeframe
        anomaly_score = 1.0
        best_score = -1.0
        for timeframe, current_total in timeframe_premium.items():
            history = self.sqlite_handler.fetch_recent_flow_totals(
                symbol=symbol,
                timeframe=timeframe,
                limit=40,
            )
            score = self._anomaly_score(current_total, history)
            if score > best_score:
                best_score = score
                anomaly_timeframe = timeframe
                anomaly_score = score

        return OptionFlowSignal(
            dominant_timeframe=dominant_timeframe,
            dominant_expiry=dominant_expiry,
            anomaly_timeframe=anomaly_timeframe,
            anomaly_score=round(anomaly_score, 3),
            timeframe_premium={k: round(v, 2) for k, v in timeframe_premium.items()},
        )

    def _anomaly_score(self, current_total: float, history: list[float]) -> float:
        if len(history) < 5:
            return 1.0
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance) if variance > 0 else 1.0
        z = (current_total - mean) / std
        return max(0.1, 1.0 + z)
