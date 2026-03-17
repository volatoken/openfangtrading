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

            CREATE TABLE IF NOT EXISTS option_bubble_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                side TEXT NOT NULL,
                premium_usd REAL NOT NULL,
                contracts REAL NOT NULL,
                bubble_size REAL NOT NULL,
                snapshot_ts TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS option_flow_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                expiry TEXT NOT NULL,
                total_premium REAL NOT NULL,
                bubble_count INTEGER NOT NULL,
                max_bubble_size REAL NOT NULL,
                dominant_timeframe TEXT NOT NULL,
                dominant_expiry TEXT NOT NULL,
                anomaly_timeframe TEXT NOT NULL,
                anomaly_score REAL NOT NULL,
                snapshot_ts TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS telegram_sync_state (
                source_key TEXT PRIMARY KEY,
                last_update_id INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS telegram_message_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                update_id INTEGER NOT NULL,
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                posted_at TEXT NOT NULL,
                text_content TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(source_key, update_id)
            );

            CREATE TABLE IF NOT EXISTS telegram_metric_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                update_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                btc_index REAL,
                options_24h_vol REAL,
                options_24h_trades REAL,
                futures_24h_trades REAL,
                funding_8h REAL,
                top_volume_expiration TEXT,
                top_volume_strike REAL,
                dominant_contract TEXT,
                max_pain REAL,
                poc REAL,
                parsed_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS option_trade_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                cp TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                qty REAL NOT NULL,
                premium_usdt REAL NOT NULL,
                trade_type TEXT NOT NULL DEFAULT 'MARKET',
                source TEXT NOT NULL DEFAULT 'binance_ws',
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_option_trade_events_symbol_time
            ON option_trade_events(symbol, event_time);

            CREATE TABLE IF NOT EXISTS option_mark_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                cp TEXT NOT NULL,
                mark_price REAL NOT NULL,
                bid_iv REAL,
                ask_iv REAL,
                mark_iv REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                index_price REAL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_option_mark_snapshots_symbol_ts
            ON option_mark_snapshots(symbol, ts);

            CREATE TABLE IF NOT EXISTS option_oi_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                cp TEXT NOT NULL,
                oi_contracts REAL NOT NULL,
                oi_usdt REAL NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_option_oi_snapshots_symbol_ts
            ON option_oi_snapshots(symbol, ts);

            CREATE TABLE IF NOT EXISTS option_ticker_24h_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                cp TEXT NOT NULL,
                volume_contracts REAL NOT NULL,
                amount_usdt REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                last_price REAL NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_option_ticker_24h_symbol_ts
            ON option_ticker_24h_snapshots(symbol, ts);

            CREATE TABLE IF NOT EXISTS option_index_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                underlying TEXT NOT NULL,
                index_price REAL NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_option_index_snapshots_ts
            ON option_index_snapshots(ts);

            CREATE TABLE IF NOT EXISTS futures_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mark_price REAL,
                index_price REAL,
                funding_rate REAL,
                next_funding_time TEXT,
                volume_24h REAL,
                quote_volume_24h REAL,
                trades_24h INTEGER,
                oi REAL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_futures_snapshots_market_symbol_ts
            ON futures_snapshots(market, symbol, ts);
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

    def insert_option_bubble_history(
        self,
        symbol: str,
        snapshot_ts: str,
        bubbles: list[Any],
        dominant_timeframe: str,
        dominant_expiry: str,
        anomaly_timeframe: str,
        anomaly_score: float,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        bubble_rows: list[tuple[Any, ...]] = []
        aggregates: dict[tuple[str, str], dict[str, float]] = {}

        for bubble in bubbles:
            bubble_rows.append(
                (
                    symbol,
                    str(getattr(bubble, "timeframe")),
                    str(getattr(bubble, "expiry")),
                    float(getattr(bubble, "strike")),
                    str(getattr(bubble, "side")),
                    float(getattr(bubble, "premium_usd")),
                    float(getattr(bubble, "contracts")),
                    float(getattr(bubble, "bubble_size")),
                    snapshot_ts,
                    now,
                )
            )
            key = (str(getattr(bubble, "timeframe")), str(getattr(bubble, "expiry")))
            if key not in aggregates:
                aggregates[key] = {"total_premium": 0.0, "bubble_count": 0.0, "max_bubble_size": 0.0}
            aggregates[key]["total_premium"] += float(getattr(bubble, "premium_usd"))
            aggregates[key]["bubble_count"] += 1.0
            aggregates[key]["max_bubble_size"] = max(
                aggregates[key]["max_bubble_size"],
                float(getattr(bubble, "bubble_size")),
            )

        if bubble_rows:
            self._conn.executemany(
                """
                INSERT INTO option_bubble_history (
                    symbol, timeframe, expiry, strike, side, premium_usd, contracts, bubble_size, snapshot_ts, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                bubble_rows,
            )

        snapshot_rows: list[tuple[Any, ...]] = []
        for (timeframe, expiry), row in aggregates.items():
            snapshot_rows.append(
                (
                    symbol,
                    timeframe,
                    expiry,
                    row["total_premium"],
                    int(row["bubble_count"]),
                    row["max_bubble_size"],
                    dominant_timeframe,
                    dominant_expiry,
                    anomaly_timeframe,
                    anomaly_score,
                    snapshot_ts,
                    now,
                )
            )

        if snapshot_rows:
            self._conn.executemany(
                """
                INSERT INTO option_flow_snapshots (
                    symbol, timeframe, expiry, total_premium, bubble_count, max_bubble_size,
                    dominant_timeframe, dominant_expiry, anomaly_timeframe, anomaly_score,
                    snapshot_ts, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                snapshot_rows,
            )
        self._conn.commit()

    def fetch_recent_flow_totals(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 40,
    ) -> list[float]:
        rows = self._conn.execute(
            """
            SELECT total_premium
            FROM option_flow_snapshots
            WHERE symbol = ? AND timeframe = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (symbol, timeframe, limit),
        ).fetchall()
        return [float(r["total_premium"]) for r in rows]

    def fetch_recent_option_bubbles(
        self,
        symbol: str,
        timeframe: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if timeframe:
            rows = self._conn.execute(
                """
                SELECT symbol, timeframe, expiry, strike, side, premium_usd, contracts, bubble_size, snapshot_ts
                FROM option_bubble_history
                WHERE symbol = ? AND timeframe = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (symbol, timeframe, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT symbol, timeframe, expiry, strike, side, premium_usd, contracts, bubble_size, snapshot_ts
                FROM option_bubble_history
                WHERE symbol = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (symbol, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_sync_state(self, source_key: str) -> int:
        row = self._conn.execute(
            """
            SELECT last_update_id
            FROM telegram_sync_state
            WHERE source_key = ?
            """,
            (source_key,),
        ).fetchone()
        if not row:
            return 0
        return int(row["last_update_id"])

    def upsert_sync_state(self, source_key: str, last_update_id: int) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO telegram_sync_state (source_key, last_update_id, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE
            SET last_update_id = excluded.last_update_id,
                updated_at = excluded.updated_at
            """,
            (source_key, int(last_update_id), now),
        )
        self._conn.commit()

    def insert_telegram_message(
        self,
        source_key: str,
        update_id: int,
        channel_id: str,
        message_id: int,
        posted_at: str,
        text_content: str,
        raw_json: dict[str, Any],
    ) -> bool:
        now = datetime.now(tz=timezone.utc).isoformat()
        cur = self._conn.execute(
            """
            INSERT OR IGNORE INTO telegram_message_history (
                source_key, update_id, channel_id, message_id, posted_at, text_content, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_key,
                int(update_id),
                channel_id,
                int(message_id),
                posted_at,
                text_content,
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def insert_telegram_metric(
        self,
        source_key: str,
        update_id: int,
        message_id: int,
        metric: dict[str, Any],
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO telegram_metric_history (
                source_key, update_id, message_id, symbol, btc_index,
                options_24h_vol, options_24h_trades, futures_24h_trades, funding_8h,
                top_volume_expiration, top_volume_strike, dominant_contract,
                max_pain, poc, parsed_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_key,
                int(update_id),
                int(message_id),
                str(metric.get("symbol", "BTC")),
                metric.get("btc_index"),
                metric.get("options_24h_vol"),
                metric.get("options_24h_trades"),
                metric.get("futures_24h_trades"),
                metric.get("funding_8h"),
                metric.get("top_volume_expiration"),
                metric.get("top_volume_strike"),
                metric.get("dominant_contract"),
                metric.get("max_pain"),
                metric.get("poc"),
                json.dumps(metric),
                now,
            ),
        )
        self._conn.commit()

    def fetch_latest_telegram_metric(
        self,
        source_key: str,
        symbol: str = "BTC",
    ) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM telegram_metric_history
            WHERE source_key = ? AND symbol = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_key, symbol),
        ).fetchone()
        if not row:
            return None
        output = dict(row)
        try:
            output["parsed_json"] = json.loads(str(row["parsed_json"]))
        except json.JSONDecodeError:
            output["parsed_json"] = {}
        return output

    def fetch_recent_telegram_messages(
        self,
        source_key: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT update_id, channel_id, message_id, posted_at, text_content
            FROM telegram_message_history
            WHERE source_key = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (source_key, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def insert_option_trade_event(
        self,
        event_time: str,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        premium_usdt: float,
        trade_type: str,
        source: str,
        raw_json: dict[str, Any],
    ) -> None:
        expiry, strike, cp = self._parse_option_symbol(symbol)
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO option_trade_events (
                event_time, symbol, expiry, strike, cp, side, price, qty, premium_usdt, trade_type, source, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_time,
                symbol,
                expiry,
                strike,
                cp,
                side,
                float(price),
                float(qty),
                float(premium_usdt),
                trade_type,
                source,
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def insert_option_mark_snapshot(
        self,
        ts: str,
        symbol: str,
        mark_price: float,
        bid_iv: float | None,
        ask_iv: float | None,
        mark_iv: float | None,
        delta: float | None,
        gamma: float | None,
        theta: float | None,
        vega: float | None,
        index_price: float | None,
        raw_json: dict[str, Any],
    ) -> None:
        expiry, strike, cp = self._parse_option_symbol(symbol)
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO option_mark_snapshots (
                ts, symbol, expiry, strike, cp, mark_price, bid_iv, ask_iv, mark_iv,
                delta, gamma, theta, vega, index_price, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                symbol,
                expiry,
                strike,
                cp,
                float(mark_price),
                bid_iv,
                ask_iv,
                mark_iv,
                delta,
                gamma,
                theta,
                vega,
                index_price,
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def insert_option_oi_snapshot(
        self,
        ts: str,
        symbol: str,
        oi_contracts: float,
        oi_usdt: float,
        raw_json: dict[str, Any],
    ) -> None:
        expiry, strike, cp = self._parse_option_symbol(symbol)
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO option_oi_snapshots (
                ts, symbol, expiry, strike, cp, oi_contracts, oi_usdt, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                symbol,
                expiry,
                strike,
                cp,
                float(oi_contracts),
                float(oi_usdt),
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def insert_option_ticker_24h_snapshot(
        self,
        ts: str,
        symbol: str,
        volume_contracts: float,
        amount_usdt: float,
        trade_count: int,
        last_price: float,
        raw_json: dict[str, Any],
    ) -> None:
        expiry, strike, cp = self._parse_option_symbol(symbol)
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO option_ticker_24h_snapshots (
                ts, symbol, expiry, strike, cp, volume_contracts, amount_usdt, trade_count, last_price, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                symbol,
                expiry,
                strike,
                cp,
                float(volume_contracts),
                float(amount_usdt),
                int(trade_count),
                float(last_price),
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def insert_option_index_snapshot(
        self,
        ts: str,
        underlying: str,
        index_price: float,
        raw_json: dict[str, Any],
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO option_index_snapshots (
                ts, underlying, index_price, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                ts,
                underlying,
                float(index_price),
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def insert_futures_snapshot(
        self,
        ts: str,
        market: str,
        symbol: str,
        mark_price: float | None,
        index_price: float | None,
        funding_rate: float | None,
        next_funding_time: str | None,
        volume_24h: float | None,
        quote_volume_24h: float | None,
        trades_24h: int | None,
        oi: float | None,
        raw_json: dict[str, Any],
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO futures_snapshots (
                ts, market, symbol, mark_price, index_price, funding_rate, next_funding_time,
                volume_24h, quote_volume_24h, trades_24h, oi, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                market,
                symbol,
                mark_price,
                index_price,
                funding_rate,
                next_funding_time,
                volume_24h,
                quote_volume_24h,
                trades_24h,
                oi,
                json.dumps(raw_json),
                now,
            ),
        )
        self._conn.commit()

    def _parse_option_symbol(self, symbol: str) -> tuple[str, float, str]:
        parts = symbol.split("-")
        if len(parts) < 4:
            return ("", 0.0, "")
        raw_expiry = parts[1].upper()
        expiry = raw_expiry
        if len(raw_expiry) == 6 and raw_expiry.isdigit():
            try:
                dt = datetime.strptime(raw_expiry, "%y%m%d")
                expiry = dt.strftime("%d%b%y").upper()
            except ValueError:
                expiry = raw_expiry
        strike = 0.0
        try:
            strike = float(parts[2])
        except ValueError:
            strike = 0.0
        cp = parts[3].upper()
        return (expiry, strike, cp)

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
