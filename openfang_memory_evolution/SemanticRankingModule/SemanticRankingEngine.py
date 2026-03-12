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

            score = (
                0.5 * similarity
                + 0.25 * min(strategy.weight / 3.0, 1.0)
                + 0.2 * win_rate
                + 0.1 * llm_eval["confidence"]
                - 0.15 * llm_eval["risk"]
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
