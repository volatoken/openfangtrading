from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import time
from typing import Any
from urllib import parse, request

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


@dataclass(frozen=True)
class TelegramSyncConfig:
    bot_token: str
    source_key: str = "telegram_default"
    channel_id: str | None = None
    symbol: str = "BTC"
    poll_timeout_sec: int = 25
    poll_interval_sec: int = 10


class TelegramBotClient:
    def __init__(self, bot_token: str) -> None:
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    def get_updates(
        self,
        offset: int | None,
        timeout_sec: int,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"timeout": timeout_sec, "limit": limit}
        if offset is not None:
            params["offset"] = offset
        url = f"{self.base_url}/getUpdates?{parse.urlencode(params)}"
        with request.urlopen(url, timeout=timeout_sec + 10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("ok", False):
            return []
        return list(payload.get("result", []))


class TelegramMessageParser:
    def parse(self, text: str, symbol: str = "BTC") -> dict[str, Any]:
        upper_text = text.upper()
        metric: dict[str, Any] = {"symbol": symbol.upper()}

        metric["btc_index"] = self._extract_number(upper_text, r"BTC\s*INDEX[^0-9\$]*\$?\s*([0-9,]+(?:\.[0-9]+)?)")
        metric["options_24h_vol"] = self._extract_number(upper_text, r"OPTIONS\s*24H\s*VOL[^0-9\$]*\$?\s*([0-9,]+(?:\.[0-9]+)?)")
        metric["options_24h_trades"] = self._extract_number(upper_text, r"OPTIONS\s*24H\s*TRADES[^0-9]*([0-9,]+(?:\.[0-9]+)?)")
        metric["futures_24h_trades"] = self._extract_number(upper_text, r"FUTURES\s*24H\s*TRADES[^0-9]*([0-9,]+(?:\.[0-9]+)?)")
        metric["funding_8h"] = self._extract_number(upper_text, r"FUNDING\s*\(8H\)[^0-9\-\+]*([\-+]?[0-9,]+(?:\.[0-9]+)?)")
        metric["top_volume_expiration"] = self._extract_text(
            upper_text,
            r"TOP\s+VOLUME\s+EXPIRATION[^A-Z0-9]*([0-9]{1,2}[A-Z]{3}[0-9]{2}|[A-Z0-9\-_/]+)",
        )
        metric["top_volume_strike"] = self._extract_number(
            upper_text,
            r"TOP\s+VOLUME\s+STRIKE[^0-9\$]*\$?\s*([0-9,]+(?:\.[0-9]+)?)",
        )
        metric["dominant_contract"] = metric.get("top_volume_expiration")
        metric["max_pain"] = self._extract_number(
            upper_text,
            r"(?:MAX\s*PAIN|MP)\s*[:=\-]?\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)",
        )
        metric["poc"] = self._extract_number(
            upper_text,
            r"(?:POC|POINT\s*OF\s*CONTROL)\s*[:=\-]?\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)",
        )
        return metric

    def _extract_number(self, text: str, pattern: str) -> float | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        raw = match.group(1).replace(",", "").strip()
        try:
            return float(raw)
        except ValueError:
            return None

    def _extract_text(self, text: str, pattern: str) -> str | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        return match.group(1).strip()


class TelegramSyncService:
    def __init__(
        self,
        config: TelegramSyncConfig,
        sqlite_handler: SQLiteMemoryHandler,
    ) -> None:
        self.config = config
        self.sqlite_handler = sqlite_handler
        self.client = TelegramBotClient(config.bot_token)
        self.parser = TelegramMessageParser()

    def sync_once(self) -> dict[str, int]:
        last_update_id = self.sqlite_handler.get_sync_state(self.config.source_key)
        updates = self.client.get_updates(
            offset=(last_update_id + 1) if last_update_id > 0 else None,
            timeout_sec=self.config.poll_timeout_sec,
        )
        inserted_messages = 0
        inserted_metrics = 0
        max_update_id = last_update_id

        for item in updates:
            update_id = int(item.get("update_id", 0))
            max_update_id = max(max_update_id, update_id)

            message = item.get("channel_post") or item.get("message")
            if not message:
                continue
            channel_raw = message.get("chat", {}).get("id")
            channel_id = str(channel_raw) if channel_raw is not None else ""
            if self.config.channel_id and channel_id != self.config.channel_id:
                continue

            text_content = str(message.get("text") or message.get("caption") or "").strip()
            if not text_content:
                continue

            posted_ts = int(message.get("date", int(time.time())))
            posted_at = datetime.fromtimestamp(posted_ts, tz=timezone.utc).isoformat()
            message_id = int(message.get("message_id", 0))
            was_inserted = self.sqlite_handler.insert_telegram_message(
                source_key=self.config.source_key,
                update_id=update_id,
                channel_id=channel_id,
                message_id=message_id,
                posted_at=posted_at,
                text_content=text_content,
                raw_json=item,
            )
            if not was_inserted:
                continue

            inserted_messages += 1
            metric = self.parser.parse(text_content, symbol=self.config.symbol)
            has_metric = any(
                metric.get(k) is not None
                for k in [
                    "btc_index",
                    "options_24h_vol",
                    "top_volume_expiration",
                    "top_volume_strike",
                    "max_pain",
                    "poc",
                ]
            )
            if has_metric:
                self.sqlite_handler.insert_telegram_metric(
                    source_key=self.config.source_key,
                    update_id=update_id,
                    message_id=message_id,
                    metric=metric,
                )
                inserted_metrics += 1

        if max_update_id > last_update_id:
            self.sqlite_handler.upsert_sync_state(self.config.source_key, max_update_id)

        return {
            "fetched_updates": len(updates),
            "inserted_messages": inserted_messages,
            "inserted_metrics": inserted_metrics,
            "last_update_id": max_update_id,
        }

    def run_forever(self) -> None:
        while True:
            result = self.sync_once()
            print(
                "[telegram-sync] "
                f"updates={result['fetched_updates']} "
                f"new_messages={result['inserted_messages']} "
                f"new_metrics={result['inserted_metrics']} "
                f"last_update_id={result['last_update_id']}"
            )
            time.sleep(self.config.poll_interval_sec)
