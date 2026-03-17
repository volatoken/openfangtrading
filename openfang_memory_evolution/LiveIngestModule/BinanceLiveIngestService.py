from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import queue
import threading
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from openfang_memory_evolution.MemoryModule.SQLiteMemoryHandler import SQLiteMemoryHandler


@dataclass
class BinanceLiveIngestConfig:
    base_asset: str = "BTC"
    underlying: str = "BTCUSDT"
    um_symbol: str = "BTCUSDT"
    cm_symbol: str = "BTCUSD_PERP"
    rest_poll_interval_sec: int = 15
    ws_enabled: bool = True
    request_timeout_sec: int = 12
    ws_queue_maxsize: int = 50000
    source_tag: str = "binance_live"
    max_option_symbols_rest: int = 80
    log_interval_sec: int = 20


class BinanceLiveIngestService:
    def __init__(self, config: BinanceLiveIngestConfig, sqlite_handler: SQLiteMemoryHandler) -> None:
        self.config = config
        self.sqlite_handler = sqlite_handler
        self._stop_event = threading.Event()
        self._ws_threads: list[threading.Thread] = []
        self._ws_apps: list[Any] = []
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=config.ws_queue_maxsize)
        self._option_symbols: list[str] = []
        self._option_expiries: list[str] = []
        self._last_symbol_refresh = 0.0
        self._last_log = 0.0
        self._hot_option_symbols: list[str] = []
        self._event_counters: dict[str, int] = {
            "option_trade": 0,
            "option_mark": 0,
            "um_ticker": 0,
            "um_mark": 0,
            "cm_ticker": 0,
            "rest": 0,
        }
        self._recent_trade_keys: deque[tuple[str, str, float, float, str]] = deque(maxlen=6000)
        self._recent_trade_set: set[tuple[str, str, float, float, str]] = set()

    def run_forever(self, iterations: int = 0) -> None:
        self._refresh_option_symbols(force=True)
        if self.config.ws_enabled:
            self._start_websockets()

        count = 0
        next_rest = 0.0
        while not self._stop_event.is_set():
            now = time.time()
            if now >= next_rest:
                self.sync_rest_once()
                next_rest = now + max(3, int(self.config.rest_poll_interval_sec))
                count += 1
                if iterations > 0 and count >= iterations:
                    break

            self._drain_ws_queue(max_items=1500)
            self._maybe_log_status()
            time.sleep(0.1)

        self.stop()

    def stop(self) -> None:
        self._stop_event.set()
        for app in self._ws_apps:
            try:
                app.close()
            except Exception:
                pass
        for t in self._ws_threads:
            if t.is_alive():
                t.join(timeout=1.5)

    def sync_rest_once(self) -> None:
        self._refresh_option_symbols()
        self._poll_options_index()
        self._poll_options_ticker()
        self._poll_options_recent_trades()
        self._poll_options_open_interest()
        self._poll_futures_rest()
        self._event_counters["rest"] += 1

    def _request_json(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        query = ""
        if params:
            cleaned = {k: v for k, v in params.items() if v is not None and v != ""}
            query = "?" + urlencode(cleaned)
        url = f"{base_url}{path}{query}"
        req = Request(url=url, method="GET")
        with urlopen(req, timeout=self.config.request_timeout_sec) as resp:
            raw = resp.read().decode("utf-8")
        return json.loads(raw)

    def _refresh_option_symbols(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_symbol_refresh < 300:
            return
        self._last_symbol_refresh = now
        try:
            data = self._request_json("https://eapi.binance.com", "/eapi/v1/exchangeInfo")
        except Exception:
            return
        symbols: list[str] = []
        expiries: set[str] = set()
        for item in data.get("optionSymbols", []):
            symbol = str(item.get("symbol", ""))
            if not symbol.startswith(f"{self.config.base_asset}-"):
                continue
            symbols.append(symbol)
            parts = symbol.split("-")
            if len(parts) >= 2:
                expiries.add(parts[1].upper())
        self._option_symbols = symbols[: self.config.max_option_symbols_rest]
        self._option_expiries = sorted(expiries)

    def _poll_options_index(self) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        try:
            data = self._request_json(
                "https://eapi.binance.com",
                "/eapi/v1/index",
                {"underlying": self.config.underlying},
            )
        except Exception:
            return
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            index_price = self._pick_float(row, ["indexPrice", "i", "price"])
            if index_price <= 0:
                continue
            self.sqlite_handler.insert_option_index_snapshot(
                ts=ts,
                underlying=self.config.underlying,
                index_price=index_price,
                raw_json=row,
            )

    def _poll_options_ticker(self) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        amounts: list[tuple[str, float]] = []
        try:
            data = self._request_json(
                "https://eapi.binance.com",
                "/eapi/v1/ticker",
                {"underlying": self.config.underlying},
            )
        except Exception:
            try:
                data = self._request_json("https://eapi.binance.com", "/eapi/v1/ticker")
            except Exception:
                return
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            symbol = str(row.get("symbol", ""))
            if not symbol.startswith(f"{self.config.base_asset}-"):
                continue
            amount = self._pick_float(row, ["amount", "A"])
            amounts.append((symbol, amount))
            self.sqlite_handler.insert_option_ticker_24h_snapshot(
                ts=ts,
                symbol=symbol,
                volume_contracts=self._pick_float(row, ["volume", "v"]),
                amount_usdt=amount,
                trade_count=int(self._pick_float(row, ["tradeCount", "n"])),
                last_price=self._pick_float(row, ["lastPrice", "c"]),
                raw_json=row,
            )
        amounts.sort(key=lambda x: x[1], reverse=True)
        self._hot_option_symbols = [s for s, _ in amounts[: min(20, len(amounts))]]

    def _poll_options_recent_trades(self) -> None:
        symbols = self._hot_option_symbols[:12] if self._hot_option_symbols else self._option_symbols[:8]
        for symbol in symbols:
            try:
                rows = self._request_json(
                    "https://eapi.binance.com",
                    "/eapi/v1/trades",
                    {"symbol": symbol, "limit": 30},
                )
            except Exception:
                continue
            if not isinstance(rows, list):
                continue
            for row in rows:
                price = self._pick_float(row, ["price", "p"])
                qty = abs(self._pick_float(row, ["qty", "q"]))
                premium = abs(self._pick_float(row, ["quoteQty", "amount"], default=price * qty))
                ts_ms = int(self._pick_float(row, ["time", "T", "E"]))
                event_time = (
                    datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
                    if ts_ms > 0
                    else datetime.now(timezone.utc).isoformat()
                )
                side = self._normalize_side(row.get("side", row.get("S", "")))
                dedup_key = (symbol, event_time, round(price, 8), round(qty, 8), side)
                if dedup_key in self._recent_trade_set:
                    continue
                if len(self._recent_trade_keys) == self._recent_trade_keys.maxlen:
                    old = self._recent_trade_keys.popleft()
                    self._recent_trade_set.discard(old)
                self._recent_trade_keys.append(dedup_key)
                self._recent_trade_set.add(dedup_key)
                self.sqlite_handler.insert_option_trade_event(
                    event_time=event_time,
                    symbol=symbol,
                    side=side,
                    price=price,
                    qty=qty,
                    premium_usdt=premium,
                    trade_type=str(row.get("type", "MARKET")),
                    source=f"{self.config.source_tag}_rest",
                    raw_json=row,
                )
                self._event_counters["option_trade"] += 1

    def _poll_options_open_interest(self) -> None:
        if not self._option_expiries:
            return
        ts = datetime.now(timezone.utc).isoformat()
        for expiry in self._option_expiries[:8]:
            try:
                data = self._request_json(
                    "https://eapi.binance.com",
                    "/eapi/v1/openInterest",
                    {"underlyingAsset": self.config.base_asset, "expiration": expiry},
                )
            except Exception:
                continue
            rows = data if isinstance(data, list) else [data]
            for row in rows:
                symbol = str(row.get("symbol", ""))
                if not symbol:
                    continue
                oi_contracts = self._pick_float(row, ["sumOpenInterest", "openInterest", "oi"])
                oi_usdt = self._pick_float(row, ["sumOpenInterestUsd", "openInterestUsd", "oiUsd"])
                self.sqlite_handler.insert_option_oi_snapshot(
                    ts=ts,
                    symbol=symbol,
                    oi_contracts=oi_contracts,
                    oi_usdt=oi_usdt,
                    raw_json=row,
                )

    def _poll_futures_rest(self) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        # UM
        um_premium: dict[str, Any] = {}
        um_ticker: dict[str, Any] = {}
        um_oi: dict[str, Any] = {}
        try:
            row = self._request_json(
                "https://fapi.binance.com",
                "/fapi/v1/premiumIndex",
                {"symbol": self.config.um_symbol},
            )
            if isinstance(row, dict):
                um_premium = row
        except Exception:
            pass
        try:
            row = self._request_json(
                "https://fapi.binance.com",
                "/fapi/v1/ticker/24hr",
                {"symbol": self.config.um_symbol},
            )
            if isinstance(row, dict):
                um_ticker = row
        except Exception:
            pass
        try:
            row = self._request_json(
                "https://fapi.binance.com",
                "/fapi/v1/openInterest",
                {"symbol": self.config.um_symbol},
            )
            if isinstance(row, dict):
                um_oi = row
        except Exception:
            pass

        self.sqlite_handler.insert_futures_snapshot(
            ts=ts,
            market="UM",
            symbol=self.config.um_symbol,
            mark_price=self._pick_float(um_premium, ["markPrice"]),
            index_price=self._pick_float(um_premium, ["indexPrice"]),
            funding_rate=self._pick_float(um_premium, ["lastFundingRate"]),
            next_funding_time=str(um_premium.get("nextFundingTime", "")),
            volume_24h=self._pick_float(um_ticker, ["volume"]),
            quote_volume_24h=self._pick_float(um_ticker, ["quoteVolume"]),
            trades_24h=int(self._pick_float(um_ticker, ["count"])),
            oi=self._pick_float(um_oi, ["openInterest"]),
            raw_json={"premium": um_premium, "ticker": um_ticker, "oi": um_oi},
        )

        # CM
        cm_premium: dict[str, Any] = {}
        cm_ticker: dict[str, Any] = {}
        cm_oi: dict[str, Any] = {}
        try:
            row = self._request_json(
                "https://dapi.binance.com",
                "/dapi/v1/premiumIndex",
                {"symbol": self.config.cm_symbol},
            )
            if isinstance(row, dict):
                cm_premium = row
        except Exception:
            pass
        try:
            row = self._request_json(
                "https://dapi.binance.com",
                "/dapi/v1/ticker/24hr",
                {"symbol": self.config.cm_symbol},
            )
            if isinstance(row, dict):
                cm_ticker = row
        except Exception:
            pass
        try:
            row = self._request_json(
                "https://dapi.binance.com",
                "/dapi/v1/openInterest",
                {"symbol": self.config.cm_symbol},
            )
            if isinstance(row, dict):
                cm_oi = row
        except Exception:
            pass

        self.sqlite_handler.insert_futures_snapshot(
            ts=ts,
            market="CM",
            symbol=self.config.cm_symbol,
            mark_price=self._pick_float(cm_premium, ["markPrice"]),
            index_price=self._pick_float(cm_premium, ["indexPrice"]),
            funding_rate=self._pick_float(cm_premium, ["lastFundingRate"]),
            next_funding_time=str(cm_premium.get("nextFundingTime", "")),
            volume_24h=self._pick_float(cm_ticker, ["volume"]),
            quote_volume_24h=self._pick_float(cm_ticker, ["quoteVolume", "baseVolume"]),
            trades_24h=int(self._pick_float(cm_ticker, ["count"])),
            oi=self._pick_float(cm_oi, ["openInterest"]),
            raw_json={"premium": cm_premium, "ticker": cm_ticker, "oi": cm_oi},
        )

    def _start_websockets(self) -> None:
        try:
            import websocket  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "WebSocket ingest requires websocket-client. Install with: pip install websocket-client"
            ) from exc

        streams = [
            ("option_trade", f"wss://nbstream.binance.com/eoptions/ws/{self.config.base_asset.lower()}@trade"),
            ("option_mark", f"wss://nbstream.binance.com/eoptions/ws/{self.config.base_asset.lower()}@markPrice"),
            ("um_ticker", f"wss://fstream.binance.com/ws/{self.config.um_symbol.lower()}@ticker"),
            ("um_mark", f"wss://fstream.binance.com/ws/{self.config.um_symbol.lower()}@markPrice@1s"),
            ("cm_ticker", f"wss://dstream.binance.com/ws/{self.config.cm_symbol.lower()}@ticker"),
        ]

        for kind, url in streams:
            app = websocket.WebSocketApp(
                url,
                on_message=self._make_on_message(kind),
                on_error=self._make_on_error(kind),
                on_close=self._make_on_close(kind),
            )
            t = threading.Thread(target=self._run_ws_forever, args=(kind, app), daemon=True)
            t.start()
            self._ws_apps.append(app)
            self._ws_threads.append(t)

    def _run_ws_forever(self, kind: str, app: Any) -> None:
        while not self._stop_event.is_set():
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            if not self._stop_event.is_set():
                time.sleep(2)

    def _make_on_message(self, kind: str):
        def _on_message(_: Any, message: str) -> None:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                return
            event = payload.get("data") if isinstance(payload, dict) and "stream" in payload else payload
            try:
                self._queue.put_nowait({"kind": kind, "data": event})
            except queue.Full:
                pass

        return _on_message

    def _make_on_error(self, kind: str):
        def _on_error(_: Any, error: Any) -> None:
            _ = (kind, error)

        return _on_error

    def _make_on_close(self, kind: str):
        def _on_close(_: Any, _code: Any, _reason: Any) -> None:
            _ = kind

        return _on_close

    def _drain_ws_queue(self, max_items: int) -> None:
        for _ in range(max_items):
            if self._queue.empty():
                return
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            kind = str(item.get("kind", ""))
            data = item.get("data", {})
            if kind == "option_trade":
                self._handle_option_trade(data)
            elif kind == "option_mark":
                self._handle_option_mark(data)
            elif kind == "um_ticker":
                self._handle_um_ticker(data)
            elif kind == "um_mark":
                self._handle_um_mark(data)
            elif kind == "cm_ticker":
                self._handle_cm_ticker(data)

    def _handle_option_trade(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        symbol = str(data.get("s", ""))
        if not symbol:
            return
        price = self._pick_float(data, ["p", "price"])
        qty = abs(self._pick_float(data, ["q", "qty", "quantity"]))
        trade_time_ms = int(self._pick_float(data, ["T", "E", "t"]))
        if trade_time_ms > 0:
            event_time = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).isoformat()
        else:
            event_time = datetime.now(timezone.utc).isoformat()
        side_raw = data.get("S", "")
        side = self._normalize_side(side_raw)
        trade_type = str(data.get("X", "MARKET"))
        premium = abs(price * qty)

        dedup_key = (symbol, event_time, round(price, 8), round(qty, 8), side)
        if dedup_key in self._recent_trade_set:
            return
        if len(self._recent_trade_keys) == self._recent_trade_keys.maxlen:
            old = self._recent_trade_keys.popleft()
            self._recent_trade_set.discard(old)
        self._recent_trade_keys.append(dedup_key)
        self._recent_trade_set.add(dedup_key)

        self.sqlite_handler.insert_option_trade_event(
            event_time=event_time,
            symbol=symbol,
            side=side,
            price=price,
            qty=qty,
            premium_usdt=premium,
            trade_type=trade_type,
            source=f"{self.config.source_tag}_ws",
            raw_json=data,
        )
        self._event_counters["option_trade"] += 1

    def _handle_option_mark(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        symbol = str(data.get("s", ""))
        if not symbol:
            return
        ts_ms = int(self._pick_float(data, ["E", "T"]))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            if ts_ms > 0
            else datetime.now(timezone.utc).isoformat()
        )
        self.sqlite_handler.insert_option_mark_snapshot(
            ts=ts,
            symbol=symbol,
            mark_price=self._pick_float(data, ["mp", "markPrice", "p"]),
            bid_iv=self._pick_nullable_float(data, ["bo", "bidIV"]),
            ask_iv=self._pick_nullable_float(data, ["ao", "askIV"]),
            mark_iv=self._pick_nullable_float(data, ["vo", "markIV", "iv"]),
            delta=self._pick_nullable_float(data, ["d", "delta"]),
            gamma=self._pick_nullable_float(data, ["g", "gamma"]),
            theta=self._pick_nullable_float(data, ["t", "theta"]),
            vega=self._pick_nullable_float(data, ["v", "vega"]),
            index_price=self._pick_nullable_float(data, ["i", "indexPrice"]),
            raw_json=data,
        )
        self._event_counters["option_mark"] += 1

    def _handle_um_ticker(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        ts_ms = int(self._pick_float(data, ["E"]))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            if ts_ms > 0
            else datetime.now(timezone.utc).isoformat()
        )
        symbol = str(data.get("s", self.config.um_symbol))
        self.sqlite_handler.insert_futures_snapshot(
            ts=ts,
            market="UM",
            symbol=symbol,
            mark_price=None,
            index_price=None,
            funding_rate=None,
            next_funding_time=None,
            volume_24h=self._pick_nullable_float(data, ["v"]),
            quote_volume_24h=self._pick_nullable_float(data, ["q"]),
            trades_24h=int(self._pick_float(data, ["n"])),
            oi=None,
            raw_json=data,
        )
        self._event_counters["um_ticker"] += 1

    def _handle_um_mark(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        ts_ms = int(self._pick_float(data, ["E"]))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            if ts_ms > 0
            else datetime.now(timezone.utc).isoformat()
        )
        symbol = str(data.get("s", self.config.um_symbol))
        next_funding = ""
        nfm = int(self._pick_float(data, ["T"]))
        if nfm > 0:
            next_funding = datetime.fromtimestamp(nfm / 1000, tz=timezone.utc).isoformat()
        self.sqlite_handler.insert_futures_snapshot(
            ts=ts,
            market="UM",
            symbol=symbol,
            mark_price=self._pick_nullable_float(data, ["p"]),
            index_price=self._pick_nullable_float(data, ["i"]),
            funding_rate=self._pick_nullable_float(data, ["r"]),
            next_funding_time=next_funding,
            volume_24h=None,
            quote_volume_24h=None,
            trades_24h=None,
            oi=None,
            raw_json=data,
        )
        self._event_counters["um_mark"] += 1

    def _handle_cm_ticker(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        ts_ms = int(self._pick_float(data, ["E"]))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            if ts_ms > 0
            else datetime.now(timezone.utc).isoformat()
        )
        symbol = str(data.get("s", self.config.cm_symbol))
        self.sqlite_handler.insert_futures_snapshot(
            ts=ts,
            market="CM",
            symbol=symbol,
            mark_price=None,
            index_price=None,
            funding_rate=None,
            next_funding_time=None,
            volume_24h=self._pick_nullable_float(data, ["v"]),
            quote_volume_24h=self._pick_nullable_float(data, ["q"]),
            trades_24h=int(self._pick_float(data, ["n"])),
            oi=None,
            raw_json=data,
        )
        self._event_counters["cm_ticker"] += 1

    def _normalize_side(self, side_raw: Any) -> str:
        if isinstance(side_raw, (int, float)):
            return "BUY" if float(side_raw) > 0 else "SELL"
        text = str(side_raw).upper().strip()
        if text in ("BUY", "B", "1", "+1"):
            return "BUY"
        if text in ("SELL", "S", "-1"):
            return "SELL"
        return "BUY" if "B" in text else "SELL"

    def _pick_float(self, payload: dict[str, Any], keys: list[str], default: float = 0.0) -> float:
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
        return default

    def _pick_nullable_float(self, payload: dict[str, Any], keys: list[str]) -> float | None:
        value = self._pick_float(payload, keys, default=float("nan"))
        if math.isnan(value):
            return None
        return value

    def _maybe_log_status(self) -> None:
        now = time.time()
        if now - self._last_log < max(5, self.config.log_interval_sec):
            return
        self._last_log = now
        print(
            "[binance-live]"
            f" rest={self._event_counters['rest']}"
            f" option_trade={self._event_counters['option_trade']}"
            f" option_mark={self._event_counters['option_mark']}"
            f" um_ticker={self._event_counters['um_ticker']}"
            f" um_mark={self._event_counters['um_mark']}"
            f" cm_ticker={self._event_counters['cm_ticker']}"
            f" queue={self._queue.qsize()}"
            f" symbols={len(self._option_symbols)}"
        )
