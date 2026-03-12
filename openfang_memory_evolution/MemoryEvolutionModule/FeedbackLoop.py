from __future__ import annotations

from dataclasses import dataclass

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


@dataclass
class FeedbackEvent:
    strategy_id: int
    pnl: float
    is_win: bool


class FeedbackLoop:
    def __init__(
        self,
        sqlite_handler: SQLiteMemoryHandler,
        win_weight_boost: float = 0.1,
        loss_weight_decay: float = 0.1,
    ) -> None:
        self.sqlite_handler = sqlite_handler
        self.win_weight_boost = win_weight_boost
        self.loss_weight_decay = loss_weight_decay

    def apply(self, event: FeedbackEvent) -> None:
        delta = self.win_weight_boost if event.is_win else self.loss_weight_decay
        self.sqlite_handler.update_feedback(
            strategy_id=event.strategy_id,
            is_win=event.is_win,
            weight_delta=delta,
        )
