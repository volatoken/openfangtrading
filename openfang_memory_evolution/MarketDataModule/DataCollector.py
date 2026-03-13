from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random


@dataclass
class OptionBubble:
    timeframe: str
    expiry: str
    strike: float
    side: str
    premium_usd: float
    contracts: float
    bubble_size: float


@dataclass
class MarketSnapshot:
    symbol: str
    timestamp: datetime
    prices: list[float]
    volumes: list[float]
    market_regime: str
    option_bubbles: list[OptionBubble]


class DataCollector:
    """Collect market snapshots. Replace synthetic generator with exchange APIs in production."""

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)

    def collect(self, symbol: str, lookback: int = 60) -> MarketSnapshot:
        base_price = 30000 if symbol.upper().startswith("BTC") else 2000
        drift = self._rng.uniform(-50, 80)
        prices: list[float] = []
        volumes: list[float] = []
        current = base_price + self._rng.uniform(-1000, 1000)

        for _ in range(lookback):
            shock = self._rng.uniform(-120, 120)
            current = max(50.0, current + drift / lookback + shock)
            prices.append(round(current, 2))
            volumes.append(round(self._rng.uniform(20, 400), 2))

        if prices[-1] > prices[0] * 1.03:
            regime = "bull"
        elif prices[-1] < prices[0] * 0.97:
            regime = "bear"
        else:
            regime = "sideway"

        option_bubbles = self._generate_option_bubbles(
            symbol=symbol.upper(),
            spot=prices[-1],
        )

        return MarketSnapshot(
            symbol=symbol.upper(),
            timestamp=datetime.now(tz=timezone.utc),
            prices=prices,
            volumes=volumes,
            market_regime=regime,
            option_bubbles=option_bubbles,
        )

    def _generate_option_bubbles(self, symbol: str, spot: float) -> list[OptionBubble]:
        now = datetime.now(tz=timezone.utc)
        weekly_expiry = now + timedelta(days=(4 - now.weekday()) % 7 or 7)
        monthly_expiry = now + timedelta(days=14)

        expiry_map = {
            "0DTE": now.strftime("%d%b%y").upper(),
            "weekly": weekly_expiry.strftime("%d%b%y").upper(),
            "monthly": monthly_expiry.strftime("%d%b%y").upper(),
        }
        timeframe_weights = {"0DTE": 0.25, "weekly": 0.35, "monthly": 0.40}
        dominant_timeframe = self._rng.choices(
            list(timeframe_weights.keys()),
            weights=list(timeframe_weights.values()),
            k=1,
        )[0]

        events: list[OptionBubble] = []
        sides = ["long_call", "short_call", "long_put", "short_put"]
        for timeframe in ["0DTE", "weekly", "monthly"]:
            event_count = self._rng.randint(8, 16)
            tf_multiplier = 2.3 if timeframe == dominant_timeframe else 1.0
            for _ in range(event_count):
                strike_shift = self._rng.uniform(-0.15, 0.15)
                strike = round(spot * (1 + strike_shift), 0)
                premium = self._rng.uniform(30_000, 450_000) * tf_multiplier
                contracts = self._rng.uniform(8, 180)
                bubble_size = premium / 10_000
                events.append(
                    OptionBubble(
                        timeframe=timeframe,
                        expiry=expiry_map[timeframe],
                        strike=strike,
                        side=self._rng.choice(sides),
                        premium_usd=round(premium, 2),
                        contracts=round(contracts, 2),
                        bubble_size=round(bubble_size, 2),
                    )
                )
        return events
