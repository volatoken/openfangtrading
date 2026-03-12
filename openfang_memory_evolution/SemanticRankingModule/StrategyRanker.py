from __future__ import annotations

from openfang_memory_evolution.MemoryModule.MemoryUpdater import MemoryUpdater
from .SemanticRankingEngine import RankedStrategy, SemanticRankingEngine


class StrategyRanker:
    def __init__(
        self,
        memory_updater: MemoryUpdater,
        ranking_engine: SemanticRankingEngine,
        top_k_search: int = 5,
    ) -> None:
        self.memory_updater = memory_updater
        self.ranking_engine = ranking_engine
        self.top_k_search = top_k_search

    def rank(
        self,
        market_vector: list[float],
        market_context: dict[str, float | str],
    ) -> list[RankedStrategy]:
        candidates = self.memory_updater.query_similar_strategies(
            market_vector=market_vector,
            top_k=self.top_k_search,
        )
        if not candidates:
            fallbacks = self.memory_updater.sqlite_handler.fetch_active_strategies(limit=self.top_k_search)
            candidates = [(strategy, 0.2) for strategy in fallbacks]
        return self.ranking_engine.rank(candidates, market_context)
