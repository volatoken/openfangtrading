from __future__ import annotations

import json
import math
from pathlib import Path


class FAISSVectorIndex:
    """
    FAISS-compatible interface.
    Current implementation uses pure Python cosine search to avoid hard runtime dependencies.
    """

    def __init__(self, dimension: int, store_path: Path) -> None:
        self.dimension = dimension
        self.store_path = store_path
        self._embeddings: dict[int, list[float]] = {}

    def _normalize(self, vec: list[float]) -> list[float]:
        values = [float(x) for x in vec]
        norm = math.sqrt(sum(x * x for x in values))
        if norm == 0:
            return values
        return [x / norm for x in values]

    def build_from_embeddings(self, embeddings: list[tuple[int, list[float]]]) -> None:
        self._embeddings = {sid: self._normalize(vec) for sid, vec in embeddings}

    def upsert(self, strategy_id: int, embedding: list[float]) -> None:
        self._embeddings[strategy_id] = self._normalize(embedding)

    def remove(self, strategy_id: int) -> None:
        if strategy_id in self._embeddings:
            del self._embeddings[strategy_id]

    def search(self, query: list[float], top_k: int = 5) -> list[tuple[int, float]]:
        if not self._embeddings:
            return []
        q = self._normalize(query)
        scores: list[tuple[int, float]] = []
        for sid, vec in self._embeddings.items():
            score = sum(a * b for a, b in zip(vec, q))
            scores.append((sid, float(score)))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]

    def save(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "dimension": self.dimension,
            "embeddings": self._embeddings,
        }
        self.store_path.with_suffix(".json").write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )

    def load(self) -> None:
        path = self.store_path.with_suffix(".json")
        if not path.exists():
            return

        raw = json.loads(path.read_text(encoding="utf-8"))
        embeddings: dict[str, list[float]] = raw.get("embeddings", {})
        self._embeddings = {int(k): self._normalize(v) for k, v in embeddings.items()}
