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
    max_pain_by_timeframe: dict[str, float]
    dominant_max_pain: float
    mp_consensus: float
    mp_distance_pct: float
    mp_reversion_direction: str
    mp_divergence_score: float


class OptionFlowAnalyzer:
    def __init__(self, sqlite_handler: SQLiteMemoryHandler) -> None:
        self.sqlite_handler = sqlite_handler

    def analyze(self, symbol: str, spot_price: float, bubbles: list[OptionBubble]) -> OptionFlowSignal:
        timeframe_premium: dict[str, float] = {"0DTE": 0.0, "weekly": 0.0, "monthly": 0.0}
        expiry_premium: dict[tuple[str, str], float] = {}
        strike_contracts: dict[str, dict[float, float]] = {"0DTE": {}, "weekly": {}, "monthly": {}}

        for bubble in bubbles:
            timeframe_premium[bubble.timeframe] = (
                timeframe_premium.get(bubble.timeframe, 0.0) + bubble.premium_usd
            )
            key = (bubble.timeframe, bubble.expiry)
            expiry_premium[key] = expiry_premium.get(key, 0.0) + bubble.premium_usd
            tf_strikes = strike_contracts.setdefault(bubble.timeframe, {})
            tf_strikes[bubble.strike] = tf_strikes.get(bubble.strike, 0.0) + bubble.contracts

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

        max_pain_by_timeframe = self._estimate_max_pain_by_timeframe(strike_contracts)
        dominant_max_pain = float(max_pain_by_timeframe.get(dominant_timeframe, spot_price))
        mp_values = [v for v in max_pain_by_timeframe.values() if v > 0]
        mp_consensus = sum(mp_values) / len(mp_values) if mp_values else spot_price
        mp_distance_pct = abs(spot_price - dominant_max_pain) / max(spot_price, 1.0)
        mp_direction = "none"
        if spot_price > dominant_max_pain:
            mp_direction = "sell"
        elif spot_price < dominant_max_pain:
            mp_direction = "buy"

        divergence_score = self._mp_divergence_score(
            spot_price=spot_price,
            dominant_mp=dominant_max_pain,
            max_pain_values=mp_values,
        )

        return OptionFlowSignal(
            dominant_timeframe=dominant_timeframe,
            dominant_expiry=dominant_expiry,
            anomaly_timeframe=anomaly_timeframe,
            anomaly_score=round(anomaly_score, 3),
            timeframe_premium={k: round(v, 2) for k, v in timeframe_premium.items()},
            max_pain_by_timeframe={k: round(v, 2) for k, v in max_pain_by_timeframe.items()},
            dominant_max_pain=round(dominant_max_pain, 2),
            mp_consensus=round(mp_consensus, 2),
            mp_distance_pct=round(mp_distance_pct, 5),
            mp_reversion_direction=mp_direction,
            mp_divergence_score=round(divergence_score, 3),
        )

    def _anomaly_score(self, current_total: float, history: list[float]) -> float:
        if len(history) < 5:
            return 1.0
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance) if variance > 0 else 1.0
        z = (current_total - mean) / std
        return max(0.1, 1.0 + z)

    def _estimate_max_pain_by_timeframe(
        self,
        strike_contracts: dict[str, dict[float, float]],
    ) -> dict[str, float]:
        result: dict[str, float] = {}
        for timeframe, strikes in strike_contracts.items():
            if not strikes:
                continue
            mp_strike = max(strikes.items(), key=lambda x: x[1])[0]
            result[timeframe] = float(mp_strike)
        return result

    def _mp_divergence_score(
        self,
        spot_price: float,
        dominant_mp: float,
        max_pain_values: list[float],
    ) -> float:
        if spot_price <= 0:
            return 0.0

        distance_pct = abs(spot_price - dominant_mp) / spot_price
        # 1.5% is the baseline trigger for meaningful divergence.
        distance_component = max(0.0, (distance_pct - 0.015) / 0.03)
        distance_component = min(distance_component, 1.0)

        if len(max_pain_values) >= 2:
            mean_mp = sum(max_pain_values) / len(max_pain_values)
            variance = sum((x - mean_mp) ** 2 for x in max_pain_values) / len(max_pain_values)
            dispersion_pct = math.sqrt(variance) / max(spot_price, 1.0)
            agreement_component = max(0.0, 1.0 - (dispersion_pct / 0.02))
        else:
            agreement_component = 0.7

        score = distance_component * (0.6 + 0.4 * agreement_component)
        return min(max(score, 0.0), 1.0)
