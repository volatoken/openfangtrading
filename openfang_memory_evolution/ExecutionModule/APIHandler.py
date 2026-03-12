from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class OrderResponse:
    order_id: str
    status: str
    filled_price: float
    timestamp: str


class APIHandler:
    """Stub exchange API handler."""

    def place_order(self, symbol: str, side: str, quantity: float, price: float) -> OrderResponse:
        now = datetime.now(tz=timezone.utc).isoformat()
        return OrderResponse(
            order_id=f"{symbol}-{side}-{now}",
            status="FILLED",
            filled_price=price,
            timestamp=now,
        )
