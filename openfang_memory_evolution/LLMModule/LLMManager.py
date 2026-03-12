from __future__ import annotations


class LLMManager:
    """
    Lightweight LLM facade.
    Replace heuristic internals with real GPT/OpenAI client calls as needed.
    """

    def evaluate_strategy(
        self,
        strategy_text: str,
        market_context: dict[str, float | str],
    ) -> dict[str, float | str]:
        text = strategy_text.lower()
        regime = str(market_context.get("market_regime", "sideway")).lower()
        rsi = float(market_context.get("rsi", 50.0))
        confidence = 0.5
        risk = 0.5

        if "oversold" in text and rsi < 35:
            confidence += 0.2
            risk -= 0.1
        if "overbought" in text and rsi > 65:
            confidence += 0.2
            risk -= 0.1
        if "trend" in text and regime == "bull":
            confidence += 0.15
        if "mean-reversion" in text and regime == "sideway":
            confidence += 0.1
        if "breakout" in text and regime in {"bull", "bear"}:
            confidence += 0.1
            risk += 0.1

        confidence = min(max(confidence, 0.05), 0.99)
        risk = min(max(risk, 0.01), 0.99)
        reason = f"regime={regime}, rsi={rsi:.1f}, text_match={text[:80]}"
        return {"confidence": confidence, "risk": risk, "reason": reason}

    def reason_trade_decision(
        self,
        prompt: str,
        recommended_action: str,
    ) -> str:
        return (
            f"Prompt: {prompt}\n"
            f"Decision: {recommended_action}. "
            f"Reasoning based on evolved memory, similarity ranking, and risk-adjusted confidence."
        )
