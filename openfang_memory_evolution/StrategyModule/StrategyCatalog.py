from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategySeed:
    key: str
    text: str
    summary: str


def get_default_strategy_seeds() -> list[StrategySeed]:
    """
    Strategy seeds based on Option flow + Max Pain + POC + Sideway framework.
    """
    return [
        StrategySeed(
            key="otl_anytime_anomaly_breakout_buy",
            text=(
                "BUY breakout when anomaly bubbles surge in any timeframe, price breaks key "
                "resistance, and retest holds above Max Pain/POC zone."
            ),
            summary="Any-timeframe anomaly breakout long continuation",
        ),
        StrategySeed(
            key="otl_anytime_anomaly_breakout_sell",
            text=(
                "SELL breakdown when anomaly put-side flow surges in any timeframe, price loses key support, "
                "and retest fails below Max Pain/POC zone."
            ),
            summary="Any-timeframe anomaly breakdown short continuation",
        ),
        StrategySeed(
            key="otl_dominant_expiration_flow_buy",
            text=(
                "BUY when dominant expiration contract carries the largest premium flow and "
                "shorter-term price action confirms its bullish direction."
            ),
            summary="Follow bullish flow from dominant expiration first",
        ),
        StrategySeed(
            key="otl_dominant_expiration_flow_sell",
            text=(
                "SELL when dominant expiration contract carries the largest premium flow and "
                "shorter-term price action confirms bearish direction."
            ),
            summary="Follow bearish flow from dominant expiration first",
        ),
        StrategySeed(
            key="otl_1dte_fakeout_short",
            text=(
                "SELL fakeout when price briefly breaks above option key level but quickly falls "
                "back below Max Pain with weak follow-through."
            ),
            summary="1DTE rejection after upside fake breakout",
        ),
        StrategySeed(
            key="otl_1dte_fakeout_long",
            text=(
                "BUY fakeout when price sweeps below option key support and rapidly reclaims "
                "Max Pain/POC, indicating downside trap."
            ),
            summary="1DTE reclaim after downside fake breakdown",
        ),
        StrategySeed(
            key="otf_maxpain_flip_bull",
            text=(
                "BUY when price flips and holds above Max Pain, with call-side flow dominance "
                "and improving momentum."
            ),
            summary="Bull regime when price sustains above Max Pain",
        ),
        StrategySeed(
            key="otf_maxpain_flip_bear",
            text=(
                "SELL when price flips and holds below Max Pain, with put-side flow dominance "
                "and weak momentum."
            ),
            summary="Bear regime when price sustains below Max Pain",
        ),
        StrategySeed(
            key="otf_maxpain_magnet_reversion",
            text=(
                "Mean-reversion strategy: when expiry approaches and trend momentum fades, "
                "trade pullback toward Max Pain as market-maker magnet level."
            ),
            summary="Expiry magnet reversion toward Max Pain",
        ),
        StrategySeed(
            key="otl_sideway_insidebar_rotation",
            text=(
                "Sideway rotation inside Inside Bar range: BUY near lower boundary and SELL near "
                "upper boundary while price repeatedly crosses Max Pain."
            ),
            summary="Range trading with Inside Bar + Max Pain cross",
        ),
        StrategySeed(
            key="otl_sideway_val_to_vah_buy",
            text=(
                "BUY near VAL in sideway regime and take profit toward VAH when option flow does "
                "not confirm trend breakout."
            ),
            summary="Value-area rotation long from VAL to VAH",
        ),
        StrategySeed(
            key="otl_sideway_vah_to_val_sell",
            text=(
                "SELL near VAH in sideway regime and target VAL when breakout attempt fails and "
                "price re-enters value area."
            ),
            summary="Value-area rotation short from VAH to VAL",
        ),
        StrategySeed(
            key="otl_weekly_above_mp_poc_bull",
            text=(
                "BUY dips when weekly price structure remains above Weekly Max Pain and Weekly "
                "POC, signaling medium-term bullish control."
            ),
            summary="Weekly bullish bias above MP and POC",
        ),
        StrategySeed(
            key="otl_monthly_70000_pivot",
            text=(
                "Monthly pivot strategy around 70000 when Monthly Max Pain and Monthly POC "
                "overlap: above pivot favors bullish continuation, below pivot favors reversion."
            ),
            summary="Monthly regime switch around MP/POC overlap",
        ),
    ]
