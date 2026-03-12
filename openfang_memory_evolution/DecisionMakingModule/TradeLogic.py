from __future__ import annotations

from openfang_memory_evolution.SemanticRankingModule.SemanticRankingEngine import RankedStrategy


class TradeLogic:
    def choose_action(
        self,
        ranked: list[RankedStrategy],
        market_context: dict[str, float | str],
    ) -> str:
        if not ranked:
            return "HOLD"

        top = ranked[0]
        rsi = float(market_context.get("rsi", 50.0))
        macd = float(market_context.get("macd", 0.0))
        signal = float(market_context.get("signal", 0.0))

        if top.risk > 0.8 and top.confidence < 0.6:
            return "HOLD"
        if rsi < 30 and macd >= signal:
            return "BUY"
        if rsi > 70 and macd <= signal:
            return "SELL"

        text = top.strategy.strategy_text.lower()
        if "buy" in text or "long" in text:
            return "BUY"
        if "sell" in text or "short" in text:
            return "SELL"
        return "HOLD"
