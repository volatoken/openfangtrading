from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import getpass
from typing import Any

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler
from openfang_memory_evolution.TelegramModule.TelegramSyncService import (
    TelegramMessageParser,
    normalize_channel_username,
)

try:
    from telethon.errors import SessionPasswordNeededError
    from telethon.sync import TelegramClient
except ImportError:  # pragma: no cover
    TelegramClient = None  # type: ignore[assignment]
    SessionPasswordNeededError = Exception  # type: ignore[assignment]


@dataclass(frozen=True)
class TelegramUserSyncConfig:
    api_id: int
    api_hash: str
    source_key: str = "telegram_user_default"
    channel_username: str = ""
    symbol: str = "BTC"
    session_path: str = "openfang_memory_evolution/data/telethon_user"
    phone: str = ""
    poll_interval_sec: int = 10
    limit_per_sync: int = 500


class TelegramUserSyncService:
    def __init__(self, config: TelegramUserSyncConfig, sqlite_handler: SQLiteMemoryHandler) -> None:
        if TelegramClient is None:
            raise RuntimeError("telethon is not installed. Install with: pip install telethon")
        channel_username = normalize_channel_username(config.channel_username)
        if not channel_username:
            raise ValueError("channel_username is required for Telegram user session mode.")
        self.config = TelegramUserSyncConfig(
            api_id=int(config.api_id),
            api_hash=config.api_hash,
            source_key=config.source_key,
            channel_username=channel_username,
            symbol=config.symbol,
            session_path=config.session_path,
            phone=config.phone,
            poll_interval_sec=max(1, int(config.poll_interval_sec)),
            limit_per_sync=max(1, int(config.limit_per_sync)),
        )
        self.sqlite_handler = sqlite_handler
        self.parser = TelegramMessageParser()

    def sync_once(self) -> dict[str, int]:
        client = TelegramClient(
            self.config.session_path,
            self.config.api_id,
            self.config.api_hash,
        )
        client.connect()
        self._ensure_authorized(client)
        try:
            entity = client.get_entity(self.config.channel_username)
            last_update_id = self.sqlite_handler.get_sync_state(self.config.source_key)
            messages = client.get_messages(
                entity,
                limit=self.config.limit_per_sync,
                min_id=last_update_id,
            )

            inserted_messages = 0
            inserted_metrics = 0
            max_update_id = last_update_id

            valid_messages = [m for m in messages if getattr(m, "id", None) is not None]
            valid_messages.sort(key=lambda x: int(x.id))

            for msg in valid_messages:
                message_id = int(msg.id)
                if message_id <= last_update_id:
                    continue
                max_update_id = max(max_update_id, message_id)

                text_content = str(getattr(msg, "message", "") or "").strip()
                if not text_content:
                    continue
                date_val = getattr(msg, "date", None)
                if date_val is None:
                    posted_at = datetime.now(tz=timezone.utc).isoformat()
                else:
                    posted_at = date_val.astimezone(timezone.utc).isoformat()

                channel_id = str(getattr(entity, "id", ""))
                was_inserted = self.sqlite_handler.insert_telegram_message(
                    source_key=self.config.source_key,
                    update_id=message_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    posted_at=posted_at,
                    text_content=text_content,
                    raw_json={
                        "source": "telethon_user",
                        "channel": self.config.channel_username,
                        "message_id": message_id,
                    },
                )
                if not was_inserted:
                    continue

                inserted_messages += 1
                metric = self.parser.parse(text_content, symbol=self.config.symbol)
                if self._has_metric(metric):
                    self.sqlite_handler.insert_telegram_metric(
                        source_key=self.config.source_key,
                        update_id=message_id,
                        message_id=message_id,
                        metric=metric,
                    )
                    inserted_metrics += 1

            if max_update_id > last_update_id:
                self.sqlite_handler.upsert_sync_state(self.config.source_key, max_update_id)

            return {
                "fetched_updates": len(valid_messages),
                "inserted_messages": inserted_messages,
                "inserted_metrics": inserted_metrics,
                "last_update_id": max_update_id,
            }
        finally:
            client.disconnect()

    def _ensure_authorized(self, client: TelegramClient) -> None:
        if client.is_user_authorized():
            return
        if not self.config.phone:
            raise RuntimeError("First login requires --telegram-phone for Telethon user mode.")
        client.send_code_request(self.config.phone)
        code = input("Enter Telegram login code: ").strip()
        try:
            client.sign_in(self.config.phone, code)
        except SessionPasswordNeededError:
            password = getpass.getpass("Enter Telegram 2FA password: ")
            client.sign_in(password=password)

    def _has_metric(self, metric: dict[str, Any]) -> bool:
        return any(
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
