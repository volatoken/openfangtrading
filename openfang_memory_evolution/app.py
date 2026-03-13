from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any

from openfang_memory_evolution.config.settings import Settings, load_settings
from openfang_memory_evolution.MarketDataModule.DataCollector import DataCollector
from openfang_memory_evolution.MarketDataModule.DataProcessor import DataProcessor
from openfang_memory_evolution.MarketDataModule.DataTransformer import DataTransformer
from openfang_memory_evolution.FeatureExtractionModule.IndicatorCalculator import IndicatorCalculator
from openfang_memory_evolution.FeatureExtractionModule.FeatureNormalizer import FeatureNormalizer
from openfang_memory_evolution.FeatureExtractionModule.FeatureVectorizer import FeatureVectorizer
from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler
from openfang_memory_evolution.MemoryModule.FAISSVectorIndex import FAISSVectorIndex
from openfang_memory_evolution.MemoryModule.MemoryUpdater import MemoryUpdater
from openfang_memory_evolution.MemoryEvolutionModule.FeedbackLoop import FeedbackEvent, FeedbackLoop
from openfang_memory_evolution.MemoryEvolutionModule.MemoryPruning import MemoryPruning
from openfang_memory_evolution.MemoryEvolutionModule.MemorySummaries import MemorySummaries
from openfang_memory_evolution.SemanticRankingModule.SemanticRankingEngine import SemanticRankingEngine
from openfang_memory_evolution.SemanticRankingModule.StrategyRanker import StrategyRanker
from openfang_memory_evolution.LLMModule.LLMManager import LLMManager
from openfang_memory_evolution.LLMModule.LLMReasoning import LLMReasoning
from openfang_memory_evolution.DecisionMakingModule.TradeLogic import TradeLogic
from openfang_memory_evolution.DecisionMakingModule.TradeDecisionMaker import TradeDecisionMaker
from openfang_memory_evolution.ExecutionModule.APIHandler import APIHandler
from openfang_memory_evolution.ExecutionModule.TradeExecutor import TradeExecutor
from openfang_memory_evolution.StrategyModule.StrategyCatalog import get_default_strategy_seeds


class OpenFangEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.data_collector = DataCollector()
        self.data_processor = DataProcessor()
        self.data_transformer = DataTransformer()
        self.indicator_calculator = IndicatorCalculator()
        self.feature_normalizer = FeatureNormalizer()
        self.feature_vectorizer = FeatureVectorizer()

        self.sqlite_handler = SQLiteMemoryHandler(settings.sqlite_path)
        self.vector_index = FAISSVectorIndex(settings.embedding_dim, settings.faiss_store_path)
        self.vector_index.load()
        self.memory_updater = MemoryUpdater(self.sqlite_handler, self.vector_index)
        self.memory_updater.initialize_index()

        self.feedback_loop = FeedbackLoop(
            sqlite_handler=self.sqlite_handler,
            win_weight_boost=settings.win_weight_boost,
            loss_weight_decay=settings.loss_weight_decay,
        )
        self.memory_pruning = MemoryPruning(
            sqlite_handler=self.sqlite_handler,
            memory_updater=self.memory_updater,
            min_trades=settings.min_trades_for_pruning,
            min_win_rate=settings.min_win_rate_for_active,
        )
        self.memory_summaries = MemorySummaries(self.sqlite_handler)

        self.llm_manager = LLMManager()
        self.ranking_engine = SemanticRankingEngine(self.llm_manager)
        self.strategy_ranker = StrategyRanker(
            memory_updater=self.memory_updater,
            ranking_engine=self.ranking_engine,
            top_k_search=settings.top_k_search,
        )
        self.decision_maker = TradeDecisionMaker(TradeLogic(), LLMReasoning(self.llm_manager))
        self.trade_executor = TradeExecutor(APIHandler())
        self._cycle_count = 0

        self._bootstrap_strategies()

    def _text_embedding(self, text: str) -> list[float]:
        vec = [0.0 for _ in range(self.settings.embedding_dim)]
        for i, ch in enumerate(text.encode("utf-8")):
            vec[i % self.settings.embedding_dim] += (ch % 31) / 31.0
        norm = math.sqrt(sum(x * x for x in vec))
        if norm == 0:
            return vec
        return [x / norm for x in vec]

    def _bootstrap_strategies(self) -> None:
        active = self.sqlite_handler.fetch_active_strategies(limit=1)
        if active:
            return

        for seed in get_default_strategy_seeds():
            emb = self._text_embedding(seed.text)
            self.memory_updater.upsert_strategy(
                seed.key,
                seed.text,
                emb,
                summary=seed.summary,
            )
        self.vector_index.save()

    def _build_market_features(self, symbol: str) -> tuple[dict[str, float | str], list[float]]:
        snapshot = self.data_collector.collect(symbol=symbol)
        processed = self.data_processor.process(snapshot)
        transformed = self.data_transformer.transform(processed)
        indicators = self.indicator_calculator.calculate(processed)

        raw_features = {
            "rsi": indicators.rsi,
            "macd": indicators.macd,
            "signal": indicators.signal,
            "histogram": indicators.histogram,
            "latest_price": transformed.latest_price,
            "avg_volume": transformed.avg_volume,
            "volatility": transformed.volatility,
            "mean_return": transformed.mean_return,
        }
        normalized = self.feature_normalizer.normalize(raw_features)
        vector = self.feature_vectorizer.to_vector(normalized)

        market_context: dict[str, float | str] = {
            **raw_features,
            "symbol": transformed.symbol,
            "market_regime": transformed.market_regime,
        }
        return market_context, vector

    def run_cycle(self, symbol: str) -> dict[str, Any]:
        market_context, market_vector = self._build_market_features(symbol)
        ranked = self.strategy_ranker.rank(market_vector, market_context)
        decision = self.decision_maker.decide(symbol, market_context, ranked)
        execution = self.trade_executor.execute(symbol, decision.action, market_context)

        trade_id = self.memory_updater.record_trade(
            strategy_id=decision.strategy_id,
            symbol=symbol,
            side=decision.action,
            pnl=execution.pnl,
            confidence=decision.confidence,
            risk=decision.risk,
            reasoning=decision.reasoning,
            market_context=market_context,
        )

        if decision.strategy_id is not None and decision.action != "HOLD":
            self.feedback_loop.apply(
                FeedbackEvent(
                    strategy_id=decision.strategy_id,
                    pnl=execution.pnl,
                    is_win=execution.pnl > 0,
                )
            )

        self._cycle_count += 1
        maintenance: dict[str, Any] = {}
        if self._cycle_count % 5 == 0:
            maintenance["summaries_updated"] = self.memory_summaries.run()
            maintenance["pruning"] = self.memory_pruning.run()
            self.vector_index.save()

        top = ranked[0] if ranked else None
        return {
            "trade_id": trade_id,
            "action": decision.action,
            "pnl": execution.pnl,
            "strategy_id": decision.strategy_id,
            "strategy_key": top.strategy.strategy_key if top else None,
            "confidence": decision.confidence,
            "risk": decision.risk,
            "market_regime": market_context["market_regime"],
            "rsi": market_context["rsi"],
            "maintenance": maintenance,
        }

    def report(self) -> list[dict[str, Any]]:
        return self.sqlite_handler.fetch_strategy_metrics()

    def close(self) -> None:
        self.vector_index.save()
        self.sqlite_handler.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenFang Memory Evolution Demo")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading symbol")
    parser.add_argument("--cycles", type=int, default=10, help="Number of decision cycles")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    settings = load_settings(root)
    engine = OpenFangEngine(settings)
    try:
        for i in range(args.cycles):
            output = engine.run_cycle(args.symbol)
            print(f"[cycle {i + 1}] {output}")

        print("\nStrategy metrics:")
        for row in engine.report():
            print(row)
    finally:
        engine.close()


if __name__ == "__main__":
    main()
