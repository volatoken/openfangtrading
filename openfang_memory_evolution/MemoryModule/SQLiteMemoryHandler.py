from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any


@dataclass
class StrategyRecord:
    id: int
    strategy_key: str
    strategy_text: str
    wins: int
    losses: int
    weight: float
    active: bool
    summary: str
    embedding: list[float]


class SQLiteMemoryHandler:
    def __init__(self, sqlite_path: Path) -> None:
        self.sqlite_path = sqlite_path
        self._conn = sqlite3.connect(self.sqlite_path)
        self._conn.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_key TEXT NOT NULL UNIQUE,
                strategy_text TEXT NOT NULL,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                weight REAL NOT NULL DEFAULT 1.0,
                active INTEGER NOT NULL DEFAULT 1,
                summary TEXT NOT NULL DEFAULT '',
                embedding_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id INTEGER,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                pnl REAL NOT NULL,
                is_win INTEGER NOT NULL,
                confidence REAL NOT NULL,
                risk REAL NOT NULL,
                reasoning TEXT NOT NULL,
                market_context_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def upsert_strategy(
        self,
        strategy_key: str,
        strategy_text: str,
        embedding: list[float],
        summary: str = "",
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        embedding_json = json.dumps([float(x) for x in embedding])

        row = self._conn.execute(
            "SELECT id FROM strategies WHERE strategy_key = ?",
            (strategy_key,),
        ).fetchone()

        if row:
            strategy_id = int(row["id"])
            self._conn.execute(
                """
                UPDATE strategies
                SET strategy_text = ?,
                    embedding_json = ?,
                    summary = ?,
                    active = 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (strategy_text, embedding_json, summary, now, strategy_id),
            )
            self._conn.commit()
            return strategy_id

        cur = self._conn.execute(
            """
            INSERT INTO strategies (
                strategy_key, strategy_text, embedding_json, summary, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (strategy_key, strategy_text, embedding_json, summary, now, now),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def fetch_active_strategies(self, limit: int = 50) -> list[StrategyRecord]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM strategies
            WHERE active = 1
            ORDER BY weight DESC, wins DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._row_to_strategy(r) for r in rows]

    def fetch_strategy(self, strategy_id: int) -> StrategyRecord | None:
        row = self._conn.execute(
            "SELECT * FROM strategies WHERE id = ?",
            (strategy_id,),
        ).fetchone()
        if not row:
            return None
        return self._row_to_strategy(row)

    def fetch_all_embeddings(self) -> list[tuple[int, list[float]]]:
        rows = self._conn.execute(
            "SELECT id, embedding_json FROM strategies WHERE active = 1"
        ).fetchall()
        output: list[tuple[int, list[float]]] = []
        for row in rows:
            embedding = [float(x) for x in json.loads(row["embedding_json"])]
            output.append((int(row["id"]), embedding))
        return output

    def update_feedback(
        self,
        strategy_id: int,
        is_win: bool,
        weight_delta: float,
    ) -> None:
        if is_win:
            self._conn.execute(
                """
                UPDATE strategies
                SET wins = wins + 1,
                    weight = MIN(3.0, weight + ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    weight_delta,
                    datetime.now(tz=timezone.utc).isoformat(),
                    strategy_id,
                ),
            )
        else:
            self._conn.execute(
                """
                UPDATE strategies
                SET losses = losses + 1,
                    weight = MAX(0.1, weight - ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    weight_delta,
                    datetime.now(tz=timezone.utc).isoformat(),
                    strategy_id,
                ),
            )
        self._conn.commit()

    def set_strategy_active(self, strategy_id: int, active: bool) -> None:
        self._conn.execute(
            """
            UPDATE strategies
            SET active = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                1 if active else 0,
                datetime.now(tz=timezone.utc).isoformat(),
                strategy_id,
            ),
        )
        self._conn.commit()

    def update_summary(self, strategy_id: int, summary: str) -> None:
        self._conn.execute(
            """
            UPDATE strategies
            SET summary = ?, updated_at = ?
            WHERE id = ?
            """,
            (summary, datetime.now(tz=timezone.utc).isoformat(), strategy_id),
        )
        self._conn.commit()

    def insert_trade(
        self,
        strategy_id: int | None,
        symbol: str,
        side: str,
        pnl: float,
        confidence: float,
        risk: float,
        reasoning: str,
        market_context: dict[str, Any],
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        cur = self._conn.execute(
            """
            INSERT INTO trades (
                strategy_id, symbol, side, pnl, is_win, confidence, risk, reasoning, market_context_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_id,
                symbol,
                side,
                pnl,
                1 if pnl > 0 else 0,
                confidence,
                risk,
                reasoning,
                json.dumps(market_context),
                now,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def fetch_strategy_metrics(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT id, strategy_key, strategy_text, wins, losses, weight, active, summary
            FROM strategies
            ORDER BY weight DESC, wins DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def _row_to_strategy(self, row: sqlite3.Row) -> StrategyRecord:
        embedding = [float(x) for x in json.loads(row["embedding_json"])]
        return StrategyRecord(
            id=int(row["id"]),
            strategy_key=str(row["strategy_key"]),
            strategy_text=str(row["strategy_text"]),
            wins=int(row["wins"]),
            losses=int(row["losses"]),
            weight=float(row["weight"]),
            active=bool(row["active"]),
            summary=str(row["summary"]),
            embedding=embedding,
        )
