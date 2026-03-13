from __future__ import annotations

from dataclasses import dataclass

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import StrategyRecord
from openfang_memory_evolution.LLMModule.LLMManager import LLMManager


@dataclass
class RankedStrategy:
    strategy: StrategyRecord
    similarity: float
    confidence: float
    risk: float
    score: float
    rationale: str


class SemanticRankingEngine:
    def __init__(self, llm_manager: LLMManager) -> None:
        self.llm_manager = llm_manager

    def rank(
        self,
        candidates: list[tuple[StrategyRecord, float]],
        market_context: dict[str, float | str],
    ) -> list[RankedStrategy]:
        ranked: list[RankedStrategy] = []
        for strategy, similarity in candidates:
            llm_eval = self.llm_manager.evaluate_strategy(strategy.strategy_text, market_context)
            total = strategy.wins + strategy.losses
            win_rate = strategy.wins / total if total else 0.5
            mp_bonus = self._mp_divergence_bonus(strategy.strategy_text, market_context)

            score = (
                0.5 * similarity
                + 0.25 * min(strategy.weight / 3.0, 1.0)
                + 0.2 * win_rate
                + 0.1 * llm_eval["confidence"]
                - 0.15 * llm_eval["risk"]
                + mp_bonus
            )

            ranked.append(
                RankedStrategy(
                    strategy=strategy,
                    similarity=similarity,
                    confidence=llm_eval["confidence"],
                    risk=llm_eval["risk"],
                    score=score,
                    rationale=llm_eval["reason"],
                )
            )

        ranked.sort(key=lambda x: x.score, reverse=True)
        return ranked

    def _mp_divergence_bonus(
        self,
        strategy_text: str,
        market_context: dict[str, float | str],
    ) -> float:
        text = strategy_text.lower()
        mp_score = float(market_context.get("mp_divergence_score", 0.0))
        direction = str(market_context.get("mp_reversion_direction", "none")).lower()
        if mp_score < 0.15:
            return 0.0

        base = min(mp_score, 1.0) * 0.18
        is_reversion = "reversion" in text or "mean-reversion" in text
        is_buy = "buy" in text or "long" in text
        is_sell = "sell" in text or "short" in text

        if direction == "buy":
            if is_reversion and is_buy:
                return base
            if is_reversion and is_sell:
                return -base * 0.5
        if direction == "sell":
            if is_reversion and is_sell:
                return base
            if is_reversion and is_buy:
                return -base * 0.5

        if "breakout" in text and mp_score > 0.55:
            return -base * 0.4
        return 0.0
