from __future__ import annotations


class FeatureNormalizer:
    """Min-max style normalization with fixed priors for market indicators."""

    def normalize(self, features: dict[str, float]) -> dict[str, float]:
        bounds = {
            "rsi": (0, 100),
            "macd": (-500, 500),
            "signal": (-500, 500),
            "histogram": (-200, 200),
            "latest_price": (100, 100000),
            "avg_volume": (0, 10000),
            "volatility": (0, 0.1),
            "mean_return": (-0.05, 0.05),
        }

        normalized: dict[str, float] = {}
        for key, value in features.items():
            lo, hi = bounds.get(key, (-1.0, 1.0))
            if hi <= lo:
                normalized[key] = 0.0
                continue
            clipped = min(max(value, lo), hi)
            normalized[key] = (clipped - lo) / (hi - lo)

        return normalized
