from __future__ import annotations

class FeatureVectorizer:
    ORDER = [
        "rsi",
        "macd",
        "signal",
        "histogram",
        "latest_price",
        "avg_volume",
        "volatility",
        "mean_return",
    ]

    def to_vector(self, normalized_features: dict[str, float]) -> list[float]:
        values = [float(normalized_features.get(key, 0.0)) for key in self.ORDER]
        return values
