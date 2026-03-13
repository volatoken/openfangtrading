from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
import time
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
from openfang_memory_evolution.OptionAnalyticsModule.OptionFlowAnalyzer import OptionFlowAnalyzer
from openfang_memory_evolution.StrategyModule.StrategyCatalog import get_default_strategy_seeds
from openfang_memory_evolution.TelegramModule.TelegramSyncService import (
    TelegramSyncConfig,
    TelegramSyncService,
)


class OpenFangEngine:
    def __init__(self, settings: Settings, telegram_source_key: str | None = None) -> None:
        self.settings = settings
        self.telegram_source_key = telegram_source_key
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
        self.option_flow_analyzer = OptionFlowAnalyzer(self.sqlite_handler)
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
        for seed in get_default_strategy_seeds():
            emb = self._text_embedding(seed.text)
            self.memory_updater.upsert_strategy(
                seed.key,
                seed.text,
                emb,
                summary=seed.summary,
            )
        self.vector_index.save()

    def _build_market_features(
        self, symbol: str
    ) -> tuple[dict[str, float | str], list[float], str, list[Any]]:
        snapshot = self.data_collector.collect(symbol=symbol)
        processed = self.data_processor.process(snapshot)
        transformed = self.data_transformer.transform(processed)
        indicators = self.indicator_calculator.calculate(processed)
        option_signal = self.option_flow_analyzer.analyze(
            symbol=symbol,
            spot_price=transformed.latest_price,
            bubbles=snapshot.option_bubbles,
        )

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
            "dominant_timeframe": option_signal.dominant_timeframe,
            "dominant_expiry": option_signal.dominant_expiry,
            "anomaly_timeframe": option_signal.anomaly_timeframe,
            "anomaly_score": option_signal.anomaly_score,
            "premium_0dte": option_signal.timeframe_premium.get("0DTE", 0.0),
            "premium_weekly": option_signal.timeframe_premium.get("weekly", 0.0),
            "premium_monthly": option_signal.timeframe_premium.get("monthly", 0.0),
            "dominant_max_pain": option_signal.dominant_max_pain,
            "max_pain_0dte": option_signal.max_pain_by_timeframe.get("0DTE", option_signal.dominant_max_pain),
            "max_pain_weekly": option_signal.max_pain_by_timeframe.get("weekly", option_signal.dominant_max_pain),
            "max_pain_monthly": option_signal.max_pain_by_timeframe.get("monthly", option_signal.dominant_max_pain),
            "max_pain_consensus": option_signal.mp_consensus,
            "mp_distance_pct": option_signal.mp_distance_pct,
            "mp_reversion_direction": option_signal.mp_reversion_direction,
            "mp_divergence_score": option_signal.mp_divergence_score,
        }

        if self.telegram_source_key:
            tgm = self.sqlite_handler.fetch_latest_telegram_metric(
                source_key=self.telegram_source_key,
                symbol="BTC",
            )
            if tgm:
                market_context["telegram_btc_index"] = tgm.get("btc_index")
                market_context["telegram_top_volume_expiration"] = tgm.get("top_volume_expiration")
                market_context["telegram_top_volume_strike"] = tgm.get("top_volume_strike")
                market_context["telegram_max_pain"] = tgm.get("max_pain")
                market_context["telegram_poc"] = tgm.get("poc")
        return market_context, vector, snapshot.timestamp.isoformat(), snapshot.option_bubbles

    def run_cycle(self, symbol: str) -> dict[str, Any]:
        market_context, market_vector, snapshot_ts, option_bubbles = self._build_market_features(symbol)
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

        self.sqlite_handler.insert_option_bubble_history(
            symbol=symbol,
            snapshot_ts=snapshot_ts,
            bubbles=option_bubbles,
            dominant_timeframe=str(market_context.get("dominant_timeframe", "")),
            dominant_expiry=str(market_context.get("dominant_expiry", "")),
            anomaly_timeframe=str(market_context.get("anomaly_timeframe", "")),
            anomaly_score=float(market_context.get("anomaly_score", 1.0)),
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
            "dominant_timeframe": market_context["dominant_timeframe"],
            "dominant_expiry": market_context["dominant_expiry"],
            "anomaly_timeframe": market_context["anomaly_timeframe"],
            "anomaly_score": market_context["anomaly_score"],
            "dominant_max_pain": market_context["dominant_max_pain"],
            "mp_distance_pct": market_context["mp_distance_pct"],
            "mp_reversion_direction": market_context["mp_reversion_direction"],
            "mp_divergence_score": market_context["mp_divergence_score"],
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
    parser.add_argument("--telegram-sync", action="store_true", help="Sync Telegram data before each cycle")
    parser.add_argument("--telegram-sync-only", action="store_true", help="Run only Telegram sync worker loop")
    parser.add_argument("--telegram-bot-token", default="", help="Telegram bot token")
    parser.add_argument("--telegram-channel-id", default="", help="Telegram channel id filter")
    parser.add_argument(
        "--telegram-channel",
        default="",
        help="Telegram channel username or link (example: AI_otl_Alert or https://t.me/AI_otl_Alert)",
    )
    parser.add_argument("--telegram-source-key", default="telegram_default", help="Source key for Telegram sync state")
    parser.add_argument("--telegram-symbol", default="BTC", help="Symbol label for parsed Telegram metrics")
    parser.add_argument("--telegram-poll-timeout", type=int, default=20, help="Telegram long-poll timeout seconds")
    parser.add_argument("--telegram-poll-seconds", type=int, default=10, help="Sleep seconds between sync loops")
    parser.add_argument(
        "--telegram-sync-iterations",
        type=int,
        default=0,
        help="Sync loop iterations in --telegram-sync-only mode (0 means infinite)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    settings = load_settings(root)
    should_sync_tg = args.telegram_sync or args.telegram_sync_only
    tg_token = args.telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_config: TelegramSyncConfig | None = None
    if should_sync_tg:
        if not tg_token:
            raise SystemExit("Telegram sync requested but TELEGRAM_BOT_TOKEN is missing.")
        tg_config = TelegramSyncConfig(
            bot_token=tg_token,
            source_key=args.telegram_source_key,
            channel_id=args.telegram_channel_id or None,
            channel_username=args.telegram_channel or None,
            symbol=args.telegram_symbol,
            poll_timeout_sec=max(1, int(args.telegram_poll_timeout)),
            poll_interval_sec=max(1, int(args.telegram_poll_seconds)),
        )

    if args.telegram_sync_only:
        tg_handler = SQLiteMemoryHandler(settings.sqlite_path)
        sync_service = TelegramSyncService(config=tg_config, sqlite_handler=tg_handler)
        iterations = int(args.telegram_sync_iterations)
        count = 0
        try:
            while True:
                result = sync_service.sync_once()
                print(f"[telegram-sync-only] {result}")
                count += 1
                if iterations > 0 and count >= iterations:
                    break
                time.sleep(max(1, args.telegram_poll_seconds))
        finally:
            tg_handler.close()
        return

    engine = OpenFangEngine(settings, telegram_source_key=(args.telegram_source_key if should_sync_tg else None))
    sync_service: TelegramSyncService | None = None
    if args.telegram_sync and tg_config:
        sync_service = TelegramSyncService(config=tg_config, sqlite_handler=engine.sqlite_handler)
    try:
        for i in range(args.cycles):
            if sync_service and args.telegram_sync:
                result = sync_service.sync_once()
                print(f"[telegram-sync] {result}")
            output = engine.run_cycle(args.symbol)
            print(f"[cycle {i + 1}] {output}")

        print("\nStrategy metrics:")
        for row in engine.report():
            print(row)
    finally:
        engine.close()


if __name__ == "__main__":
    main()
