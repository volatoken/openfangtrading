from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    sqlite_path: Path
    faiss_store_path: Path
    embedding_dim: int = 8
    top_k_search: int = 5
    min_trades_for_pruning: int = 5
    min_win_rate_for_active: float = 0.4
    loss_weight_decay: float = 0.1
    win_weight_boost: float = 0.1


def load_settings(base_dir: str | Path) -> Settings:
    root = Path(base_dir).resolve()
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return Settings(
        base_dir=root,
        sqlite_path=data_dir / "memory.db",
        faiss_store_path=data_dir / "faiss_index",
    )
