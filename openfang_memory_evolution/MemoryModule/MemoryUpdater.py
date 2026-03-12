from __future__ import annotations

from typing import Any

from .FAISSVectorIndex import FAISSVectorIndex
from .SQLiteMemoryHandler import SQLiteMemoryHandler, StrategyRecord


class MemoryUpdater:
    def __init__(self, sqlite_handler: SQLiteMemoryHandler, vector_index: FAISSVectorIndex) -> None:
        self.sqlite_handler = sqlite_handler
        self.vector_index = vector_index

    def initialize_index(self) -> None:
        embeddings = self.sqlite_handler.fetch_all_embeddings()
        self.vector_index.build_from_embeddings(embeddings)

    def upsert_strategy(
        self,
        strategy_key: str,
        strategy_text: str,
        embedding: list[float],
        summary: str = "",
    ) -> int:
        strategy_id = self.sqlite_handler.upsert_strategy(
            strategy_key=strategy_key,
            strategy_text=strategy_text,
            embedding=embedding,
            summary=summary,
        )
        self.vector_index.upsert(strategy_id, embedding)
        return strategy_id

    def deactivate_strategy(self, strategy_id: int) -> None:
        self.sqlite_handler.set_strategy_active(strategy_id, active=False)
        self.vector_index.remove(strategy_id)

    def reactivate_strategy(self, strategy_id: int) -> None:
        strategy = self.sqlite_handler.fetch_strategy(strategy_id)
        if not strategy:
            return
        self.sqlite_handler.set_strategy_active(strategy_id, active=True)
        self.vector_index.upsert(strategy_id, strategy.embedding)

    def record_trade(
        self,
        strategy_id: int | None,
        symbol: str,
        side: str,
        pnl: float,
        confidence: float,
        risk: float,
        reasoning: str,
        market_context: dict[str, Any],
    ) -> int:
        return self.sqlite_handler.insert_trade(
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            pnl=pnl,
            confidence=confidence,
            risk=risk,
            reasoning=reasoning,
            market_context=market_context,
        )

    def query_similar_strategies(
        self, market_vector: list[float], top_k: int
    ) -> list[tuple[StrategyRecord, float]]:
        neighbors = self.vector_index.search(market_vector, top_k=top_k)
        output: list[tuple[StrategyRecord, float]] = []
        for strategy_id, score in neighbors:
            strategy = self.sqlite_handler.fetch_strategy(strategy_id)
            if strategy and strategy.active:
                output.append((strategy, score))
        return output
