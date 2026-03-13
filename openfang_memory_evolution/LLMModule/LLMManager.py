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
        dominant_timeframe = str(market_context.get("dominant_timeframe", "")).lower()
        anomaly_timeframe = str(market_context.get("anomaly_timeframe", "")).lower()
        anomaly_score = float(market_context.get("anomaly_score", 1.0))
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
        if "sideway" in text and regime == "sideway":
            confidence += 0.15
            risk -= 0.05

        # No fixed 0DTE trigger: any timeframe anomaly can lead, if it matches strategy context.
        strategy_timeframes = ["0dte", "weekly", "monthly"]
        for tf in strategy_timeframes:
            if tf in text:
                if dominant_timeframe == tf or anomaly_timeframe == tf:
                    confidence += 0.12
                    risk -= 0.04
                else:
                    confidence -= 0.08
                    risk += 0.04

        if "anomaly" in text and anomaly_score >= 1.6:
            confidence += 0.12
        if "dominant expiration" in text:
            confidence += 0.08
        if "breakout" in text and anomaly_score < 1.1:
            confidence -= 0.06

        confidence = min(max(confidence, 0.05), 0.99)
        risk = min(max(risk, 0.01), 0.99)
        reason = (
            f"regime={regime}, rsi={rsi:.1f}, dominant_tf={dominant_timeframe}, "
            f"anomaly_tf={anomaly_timeframe}, anomaly_score={anomaly_score:.2f}, "
            f"text_match={text[:80]}"
        )
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
