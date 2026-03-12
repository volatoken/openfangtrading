from __future__ import annotations

from openfang_memory_evolution.MemoryModule.MemoryUpdater import MemoryUpdater
from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


class MemoryPruning:
    def __init__(
        self,
        sqlite_handler: SQLiteMemoryHandler,
        memory_updater: MemoryUpdater,
        min_trades: int = 5,
        min_win_rate: float = 0.4,
    ) -> None:
        self.sqlite_handler = sqlite_handler
        self.memory_updater = memory_updater
        self.min_trades = min_trades
        self.min_win_rate = min_win_rate

    def run(self) -> dict[str, int]:
        pruned = 0
        kept = 0
        for row in self.sqlite_handler.fetch_strategy_metrics():
            wins = int(row["wins"])
            losses = int(row["losses"])
            strategy_id = int(row["id"])
            total = wins + losses
            if total < self.min_trades:
                kept += 1
                continue

            win_rate = wins / total if total > 0 else 0.0
            if win_rate < self.min_win_rate:
                self.memory_updater.deactivate_strategy(strategy_id)
                pruned += 1
            else:
                kept += 1

        return {"pruned": pruned, "kept": kept}
