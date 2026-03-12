from __future__ import annotations

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


class MemorySummaries:
    def __init__(self, sqlite_handler: SQLiteMemoryHandler) -> None:
        self.sqlite_handler = sqlite_handler

    def run(self) -> int:
        updated = 0
        for row in self.sqlite_handler.fetch_strategy_metrics():
            strategy_id = int(row["id"])
            wins = int(row["wins"])
            losses = int(row["losses"])
            total = wins + losses
            win_rate = wins / total if total else 0.0
            weight = float(row["weight"])
            active = bool(row["active"])

            summary = (
                f"trades={total}, win_rate={win_rate:.2f}, "
                f"weight={weight:.2f}, active={active}"
            )
            self.sqlite_handler.update_summary(strategy_id, summary)
            updated += 1
        return updated
