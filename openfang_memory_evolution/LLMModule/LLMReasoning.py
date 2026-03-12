from __future__ import annotations

from openfang_memory_evolution.SemanticRankingModule.SemanticRankingEngine import RankedStrategy
from .LLMManager import LLMManager


class LLMReasoning:
    def __init__(self, llm_manager: LLMManager) -> None:
        self.llm_manager = llm_manager

    def build_prompt(
        self,
        symbol: str,
        market_context: dict[str, float | str],
        top_strategy: RankedStrategy | None,
    ) -> str:
        strategy_text = top_strategy.strategy.strategy_text if top_strategy else "No prior strategy"
        return (
            f"Given market condition for {symbol} with "
            f"regime={market_context.get('market_regime')}, "
            f"RSI={float(market_context.get('rsi', 50.0)):.2f}, "
            f"MACD={float(market_context.get('macd', 0.0)):.4f}, "
            f"strategy='{strategy_text}', recommend BUY/SELL/HOLD."
        )

    def explain(self, prompt: str, action: str) -> str:
        return self.llm_manager.reason_trade_decision(prompt, action)
