from __future__ import annotations

from dataclasses import dataclass

from openfang_memory_evolution.LLMModule.LLMReasoning import LLMReasoning
from openfang_memory_evolution.SemanticRankingModule.SemanticRankingEngine import RankedStrategy
from .TradeLogic import TradeLogic


@dataclass
class TradeDecision:
    action: str
    strategy_id: int | None
    confidence: float
    risk: float
    reasoning: str


class TradeDecisionMaker:
    def __init__(self, logic: TradeLogic, llm_reasoning: LLMReasoning) -> None:
        self.logic = logic
        self.llm_reasoning = llm_reasoning

    def decide(
        self,
        symbol: str,
        market_context: dict[str, float | str],
        ranked_strategies: list[RankedStrategy],
    ) -> TradeDecision:
        action = self.logic.choose_action(ranked_strategies, market_context)
        top = ranked_strategies[0] if ranked_strategies else None
        prompt = self.llm_reasoning.build_prompt(symbol, market_context, top)
        reasoning = self.llm_reasoning.explain(prompt, action)
        return TradeDecision(
            action=action,
            strategy_id=top.strategy.id if top else None,
            confidence=top.confidence if top else 0.5,
            risk=top.risk if top else 0.5,
            reasoning=reasoning,
        )
