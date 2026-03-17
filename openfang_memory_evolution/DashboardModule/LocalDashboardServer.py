from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

from openfang_memory_evolution.config.settings import load_settings

HANOI_TZ = timezone(timedelta(hours=7))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_expiry_label(label: str) -> datetime | None:
    for fmt in ("%d%b%y", "%d%b%Y"):
        try:
            dt = datetime.strptime(label.upper(), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _classify_expiry_group(expiry: str, now_ref: datetime) -> str:
    dt = _parse_expiry_label(expiry)
    if not dt:
        return "unknown"
    dte = (dt.date() - now_ref.date()).days
    if dte <= 0:
        return "0DTE"
    if dte == 1:
        return "1DTE"
    if dte <= 7:
        return "weekly"
    return "monthly"


class DashboardMetricsBuilder:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._price_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def build(self, otl_strict: bool = False) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        now_hanoi = now_utc.astimezone(HANOI_TZ)
        with self._connect() as conn:
            bubbles = self._fetch_bubbles(conn, lookback_hours=48)
            contexts = self._fetch_trade_contexts(conn, limit=300)
            header = self._build_header(conn, contexts, bubbles, now_utc)
            maxpain_panels = self._build_maxpain_panels(
                conn=conn,
                bubbles=bubbles,
                contexts=contexts,
                now_utc=now_utc,
                now_local=now_hanoi,
                otl_strict=otl_strict,
            )
            analytics_panels = self._build_analytics_panels(
                bubbles=bubbles,
                contexts=contexts,
                now_utc=now_hanoi,
                spot_hint=_safe_float(header.get("btc_index")),
            )
            big_trades = self._build_big_trades(bubbles, threshold=100000.0)

        return {
            "timestamp": now_hanoi.isoformat(),
            "header": header,
            "maxpain_panels": maxpain_panels,
            "analytics_panels": analytics_panels,
            "big_trades": big_trades,
            "meta": {
                "db_path": str(self.db_path),
                "data_points": len(bubbles),
                "contexts": len(contexts),
                "otl_strict": bool(otl_strict),
                "display_tz": "Asia/Ho_Chi_Minh",
            },
        }

    def _fetch_bubbles(self, conn: sqlite3.Connection, lookback_hours: int) -> list[dict[str, Any]]:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        live_rows = conn.execute(
            """
            SELECT
              event_time,
              symbol,
              expiry,
              strike,
              cp,
              side,
              AVG(price) AS price,
              SUM(qty) AS qty,
              SUM(premium_usdt) AS premium_usdt,
              COUNT(*) AS dup_count
            FROM option_trade_events
            WHERE event_time >= ?
            GROUP BY event_time, symbol, expiry, strike, cp, side
            ORDER BY event_time ASC
            """,
            (since,),
        ).fetchall()
        if live_rows:
            output: list[dict[str, Any]] = []
            for r in live_rows:
                cp = str(r["cp"]).upper()
                side = str(r["side"]).upper()
                mapped_side = self._map_side(cp=cp, side=side)
                premium = _safe_float(r["premium_usdt"])
                qty = abs(_safe_float(r["qty"]))
                output.append(
                    {
                        "symbol": r["symbol"],
                        "timeframe": "",
                        "expiry": r["expiry"],
                        "strike": _safe_float(r["strike"]),
                        "side": mapped_side,
                        "premium_usd": premium,
                        "contracts": qty,
                        "bubble_size": max(1.0, premium / 10000.0),
                        "snapshot_ts": r["event_time"],
                    }
                )
            return output

        rows = conn.execute(
            """
            SELECT symbol, timeframe, expiry, strike, side, premium_usd, contracts, bubble_size, snapshot_ts
            FROM option_bubble_history
            WHERE snapshot_ts >= ?
            ORDER BY snapshot_ts ASC, id ASC
            """,
            (since,),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            item["strike"] = _safe_float(item.get("strike"))
            item["premium_usd"] = _safe_float(item.get("premium_usd"))
            item["contracts"] = _safe_float(item.get("contracts"))
            item["bubble_size"] = _safe_float(item.get("bubble_size"))
            output.append(item)
        return output

    def _fetch_live_price_series(self, conn: sqlite3.Connection, lookback_hours: int) -> list[dict[str, Any]]:
        since = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        rows = conn.execute(
            """
            SELECT ts, index_price
            FROM option_index_snapshots
            WHERE ts >= ?
            ORDER BY id ASC
            """,
            (since,),
        ).fetchall()
        series: list[dict[str, Any]] = []
        for r in rows:
            price = _safe_float(r["index_price"])
            if price <= 0:
                continue
            series.append({"ts": str(r["ts"]), "price": round(price, 2)})
        if not series:
            rows2 = conn.execute(
                """
                SELECT ts, COALESCE(index_price, mark_price) AS px
                FROM futures_snapshots
                WHERE ts >= ? AND market = 'UM' AND symbol = ?
                ORDER BY id ASC
                """,
                (since, self._default_um_symbol()),
            ).fetchall()
            for r in rows2:
                price = _safe_float(r["px"])
                if price <= 0:
                    continue
                series.append({"ts": str(r["ts"]), "price": round(price, 2)})
        return self._downsample_price_series(series, max_points=360)

    def _fetch_trade_contexts(self, conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT created_at, market_context_json
            FROM trades
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for r in rows:
            mc_raw = r["market_context_json"] or "{}"
            try:
                mc = json.loads(mc_raw)
            except json.JSONDecodeError:
                mc = {}
            output.append(
                {
                    "created_at": r["created_at"],
                    "market_context": mc,
                }
            )
        output.reverse()
        return output

    def _build_header(
        self,
        conn: sqlite3.Connection,
        contexts: list[dict[str, Any]],
        bubbles: list[dict[str, Any]],
        now_utc: datetime,
    ) -> dict[str, Any]:
        latest_mc = contexts[-1]["market_context"] if contexts else {}
        options_vol_24h = 0.0
        options_trades_24h = 0

        ticker_row = conn.execute(
            """
            SELECT
              COALESCE(SUM(t.amount_usdt), 0) AS amount_usdt,
              COALESCE(SUM(t.trade_count), 0) AS trade_count
            FROM option_ticker_24h_snapshots t
            INNER JOIN (
              SELECT symbol, MAX(id) AS max_id
              FROM option_ticker_24h_snapshots
              WHERE symbol LIKE ?
              GROUP BY symbol
            ) latest
            ON t.id = latest.max_id
            """,
            (f"{self._base_asset()}-%",),
        ).fetchone()
        if ticker_row and _safe_float(ticker_row["amount_usdt"]) > 0:
            options_vol_24h = _safe_float(ticker_row["amount_usdt"])
            options_trades_24h = int(_safe_float(ticker_row["trade_count"]))
        else:
            since_24h = now_utc - timedelta(hours=24)
            for b in bubbles:
                ts = _parse_iso(str(b.get("snapshot_ts", "")))
                if ts and ts >= since_24h:
                    options_vol_24h += _safe_float(b.get("premium_usd"))
                    options_trades_24h += 1

        futures_row = conn.execute(
            """
            SELECT market, symbol, funding_rate, trades_24h, mark_price, index_price
            FROM futures_snapshots
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_um = conn.execute(
            """
            SELECT
              (
                SELECT trades_24h FROM futures_snapshots
                WHERE market = 'UM' AND symbol = ? AND trades_24h IS NOT NULL
                ORDER BY id DESC LIMIT 1
              ) AS trades_24h,
              (
                SELECT funding_rate FROM futures_snapshots
                WHERE market = 'UM' AND symbol = ? AND funding_rate IS NOT NULL
                ORDER BY id DESC LIMIT 1
              ) AS funding_rate,
              (
                SELECT mark_price FROM futures_snapshots
                WHERE market = 'UM' AND symbol = ? AND mark_price IS NOT NULL
                ORDER BY id DESC LIMIT 1
              ) AS mark_price,
              (
                SELECT index_price FROM futures_snapshots
                WHERE market = 'UM' AND symbol = ? AND index_price IS NOT NULL
                ORDER BY id DESC LIMIT 1
              ) AS index_price
            """,
            (
                self._default_um_symbol(),
                self._default_um_symbol(),
                self._default_um_symbol(),
                self._default_um_symbol(),
            ),
        ).fetchone()
        latest_index = conn.execute(
            """
            SELECT index_price
            FROM option_index_snapshots
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        btc_index = _safe_float(latest_mc.get("telegram_btc_index") or latest_mc.get("latest_price"))
        if latest_index:
            btc_index = _safe_float(latest_index["index_price"], btc_index)
        if latest_um and btc_index <= 0:
            btc_index = _safe_float(latest_um["index_price"])
        if btc_index <= 0 and bubbles:
            btc_index = _safe_float(bubbles[-1]["strike"])

        futures_24h = _safe_float(latest_mc.get("futures_24h_trades"))
        funding_8h = _safe_float(latest_mc.get("funding_8h"))
        if latest_um:
            futures_24h = _safe_float(latest_um["trades_24h"], futures_24h)
            funding_8h = _safe_float(latest_um["funding_rate"], funding_8h)
        elif futures_row:
            funding_8h = _safe_float(futures_row["funding_rate"], funding_8h)
            futures_24h = _safe_float(futures_row["trades_24h"], futures_24h)

        return {
            "btc_index": round(btc_index, 2),
            "options_24h_vol_usdt": round(options_vol_24h, 2),
            "options_24h_trades": int(options_trades_24h),
            "futures_24h_trades": int(futures_24h),
            "funding_8h": round(funding_8h, 6),
            "dominant_timeframe": str(latest_mc.get("dominant_timeframe", "")),
            "dominant_expiry": str(latest_mc.get("dominant_expiry", "")),
        }

    def _build_maxpain_panels(
        self,
        conn: sqlite3.Connection,
        bubbles: list[dict[str, Any]],
        contexts: list[dict[str, Any]],
        now_utc: datetime,
        now_local: datetime,
        otl_strict: bool = False,
    ) -> dict[str, Any]:
        ticker_rows = self._latest_ticker_rows(conn)
        oi_rows = self._latest_oi_rows(conn)
        panel_expiries = self._select_panel_expiries(ticker_rows=ticker_rows, oi_rows=oi_rows, now_local=now_local)

        live_price_series = self._fetch_live_price_series(conn, lookback_hours=120)
        fallback_series: list[dict[str, Any]] = []
        if not live_price_series:
            for item in contexts[-280:]:
                mc = item["market_context"]
                price = _safe_float(mc.get("latest_price"))
                if price > 0:
                    fallback_series.append({"ts": str(item["created_at"]), "price": round(price, 2)})
            fallback_series = self._downsample_price_series(fallback_series, max_points=260)

        out: dict[str, Any] = {}
        panel_defs = [
            ("p1_0dte", "0DTE", panel_expiries.get("0DTE", "")),
            ("p2_1dte", "1DTE", panel_expiries.get("1DTE", "")),
            ("p3_weekly", "weekly", panel_expiries.get("weekly", "")),
            ("p4_monthly", "monthly", panel_expiries.get("monthly", "")),
        ]
        for panel_id, panel_group, expiry in panel_defs:
            contract_start = self._find_contract_start(conn=conn, expiry=expiry, now_utc=now_utc, now_local=now_local)
            price_series = (
                self._fetch_price_series_for_window(conn=conn, start_utc=contract_start, end_utc=now_utc)
                if expiry
                else list(live_price_series)
            )
            if not price_series:
                price_series = list(live_price_series) if live_price_series else list(fallback_series)

            panel_bubbles = self._fetch_expiry_bubbles(conn=conn, expiry=expiry, start_utc=contract_start, end_utc=now_utc)
            strike_flow = self._strike_flow_from_ticker(ticker_rows=ticker_rows, expiry=expiry)
            spot_price = _safe_float(price_series[-1]["price"]) if price_series else 0.0
            max_pain = self._compute_max_pain(
                oi_rows=oi_rows,
                expiry=expiry,
            )
            poc = max(strike_flow.items(), key=lambda x: x[1])[0] if strike_flow else 0.0
            if not panel_bubbles:
                panel_bubbles = self._build_bubbles_from_ticker_history(
                    conn=conn,
                    expiry=expiry,
                    start_utc=contract_start,
                    end_utc=now_utc,
                )
            if not panel_bubbles:
                panel_bubbles = self._build_synthetic_bubbles(price_series=price_series, strike_flow=strike_flow)

            price_series = self._to_hanoi_series(price_series)
            panel_bubbles = self._to_hanoi_bubbles(panel_bubbles)

            out[panel_id] = {
                "expiry": expiry,
                "group": panel_group,
                "contract_start": contract_start.astimezone(HANOI_TZ).isoformat(),
                "max_pain": round(max_pain, 2),
                "poc": round(poc, 2),
                "price_series": price_series,
                "bubbles": panel_bubbles,
                "strike_flow": [
                    {"strike": round(strike, 2), "premium_usd": round(value, 2)}
                    for strike, value in sorted(strike_flow.items(), key=lambda x: x[0])
                ],
                "count": len(panel_bubbles),
            }
        return out

    def _build_analytics_panels(
        self,
        bubbles: list[dict[str, Any]],
        contexts: list[dict[str, Any]],
        now_utc: datetime,
        spot_hint: float = 0.0,
    ) -> dict[str, Any]:
        spot = spot_hint if spot_hint > 0 else 0.0
        if contexts:
            spot = _safe_float(contexts[-1]["market_context"].get("latest_price"), spot)
        if spot <= 0 and bubbles:
            spot = _safe_float(bubbles[-1]["strike"])

        strike_volume: dict[float, float] = defaultdict(float)
        strike_premium: dict[float, float] = defaultdict(float)
        strike_delta: dict[float, float] = defaultdict(float)
        strike_gex: dict[float, float] = defaultdict(float)
        expiry_volume: dict[str, float] = defaultdict(float)
        expiry_contracts: dict[str, float] = defaultdict(float)
        expiry_group_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

        delta_sign = {
            "long_call": 1.0,
            "short_call": -1.0,
            "long_put": -1.0,
            "short_put": 1.0,
        }
        gamma_sign = {
            "long_call": 1.0,
            "short_call": -1.0,
            "long_put": 1.0,
            "short_put": -1.0,
        }

        for r in bubbles:
            strike = _safe_float(r["strike"])
            premium = _safe_float(r["premium_usd"])
            contracts = _safe_float(r["contracts"])
            side = str(r["side"])
            expiry = str(r["expiry"])
            group = _classify_expiry_group(expiry, now_utc)

            strike_volume[strike] += contracts
            strike_premium[strike] += premium
            strike_delta[strike] += contracts * delta_sign.get(side, 0.0)
            strike_gex[strike] += premium * gamma_sign.get(side, 0.0) * 0.01
            expiry_volume[expiry] += premium
            expiry_contracts[expiry] += contracts
            expiry_group_rows[group].append(r)
            expiry_group_rows["all"].append(r)

        strikes_sorted = sorted(strike_volume.keys())
        top_strikes = sorted(strikes_sorted, key=lambda s: strike_premium[s], reverse=True)[:30]
        top_strikes_sorted = sorted(top_strikes)

        pa1 = {
            "strikes": [round(s, 2) for s in top_strikes_sorted],
            "values": [round(strike_volume[s], 3) for s in top_strikes_sorted],
        }
        pa2 = {
            "expiry": list(expiry_volume.keys()),
            "values": [round(v, 2) for v in expiry_volume.values()],
        }
        pa3 = {
            "expiry": list(expiry_contracts.keys()),
            "values": [round(v, 3) for v in expiry_contracts.values()],
        }
        pa10 = {
            "strikes": [round(s, 2) for s in top_strikes_sorted],
            "values": [round(strike_delta[s], 4) for s in top_strikes_sorted],
        }
        pa11 = {
            "strikes": [round(s, 2) for s in top_strikes_sorted],
            "values": [round(strike_gex[s], 4) for s in top_strikes_sorted],
        }

        iv_payload = {
            "odte": self._build_iv_smile(expiry_group_rows.get("0DTE", []), spot),
            "weekly": self._build_iv_smile(expiry_group_rows.get("weekly", []), spot),
            "monthly": self._build_iv_smile(expiry_group_rows.get("monthly", []), spot),
            "all": self._build_iv_smile(expiry_group_rows.get("all", []), spot),
        }

        pdg = {
            "odte": self._build_premium_delta_gex(expiry_group_rows.get("0DTE", [])),
            "weekly": self._build_premium_delta_gex(expiry_group_rows.get("weekly", [])),
            "monthly": self._build_premium_delta_gex(expiry_group_rows.get("monthly", [])),
        }

        odte_rows = expiry_group_rows.get("0DTE", [])
        if not odte_rows:
            odte_rows = expiry_group_rows.get("weekly", []) or expiry_group_rows.get("monthly", [])
        odte_info = self._build_odte_info(odte_rows, expiry_volume, spot)

        return {
            "pa1_volume_by_strike": pa1,
            "pa2_volume_by_expiry": pa2,
            "pa3_oi_by_expiry": pa3,
            "pa4_odte_info": odte_info,
            "pa10_net_delta_by_strike": pa10,
            "pa11_gex_by_strike": pa11,
            "pa12_iv_smile": iv_payload,
            "pa5_6_7_pdg": pdg,
        }

    def _build_iv_smile(self, rows: list[dict[str, Any]], spot: float) -> dict[str, Any]:
        if not rows:
            return {"strikes": [], "call_iv": [], "put_iv": []}

        strike_prem_call: dict[float, float] = defaultdict(float)
        strike_prem_put: dict[float, float] = defaultdict(float)
        for r in rows:
            strike = _safe_float(r["strike"])
            premium = _safe_float(r["premium_usd"])
            side = str(r["side"])
            if "call" in side:
                strike_prem_call[strike] += premium
            else:
                strike_prem_put[strike] += premium

        strikes = sorted(set(list(strike_prem_call.keys()) + list(strike_prem_put.keys())))
        if len(strikes) > 30:
            strikes = strikes[:: max(1, len(strikes) // 30)]

        call_iv = []
        put_iv = []
        for strike in strikes:
            dist = abs(strike - spot) / max(spot, 1.0)
            base = 45.0 + min(30.0, dist * 220.0)
            call_boost = math.log1p(strike_prem_call.get(strike, 0.0)) / 10.0
            put_boost = math.log1p(strike_prem_put.get(strike, 0.0)) / 10.0
            call_iv.append(round(min(99.0, base + call_boost), 3))
            put_iv.append(round(min(99.0, base + put_boost + 1.5), 3))

        return {
            "strikes": [round(s, 2) for s in strikes],
            "call_iv": call_iv,
            "put_iv": put_iv,
        }

    def _build_premium_delta_gex(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {"strikes": [], "premium": [], "delta": [], "gex": []}

        premium_map: dict[float, float] = defaultdict(float)
        delta_map: dict[float, float] = defaultdict(float)
        gex_map: dict[float, float] = defaultdict(float)
        delta_sign = {"long_call": 1.0, "short_call": -1.0, "long_put": -1.0, "short_put": 1.0}
        gamma_sign = {"long_call": 1.0, "short_call": -1.0, "long_put": 1.0, "short_put": -1.0}

        for r in rows:
            strike = _safe_float(r["strike"])
            premium = _safe_float(r["premium_usd"])
            contracts = _safe_float(r["contracts"])
            side = str(r["side"])
            premium_map[strike] += premium
            delta_map[strike] += contracts * delta_sign.get(side, 0.0)
            gex_map[strike] += premium * gamma_sign.get(side, 0.0) * 0.01

        strikes = sorted(premium_map.keys())
        if len(strikes) > 28:
            strikes = strikes[:: max(1, len(strikes) // 28)]
        return {
            "strikes": [round(s, 2) for s in strikes],
            "premium": [round(premium_map[s], 2) for s in strikes],
            "delta": [round(delta_map[s], 4) for s in strikes],
            "gex": [round(gex_map[s], 4) for s in strikes],
        }

    def _build_odte_info(
        self,
        odte_rows: list[dict[str, Any]],
        expiry_volume: dict[str, float],
        spot: float,
    ) -> dict[str, Any]:
        strike_premium: dict[float, float] = defaultdict(float)
        for r in odte_rows:
            strike_premium[_safe_float(r["strike"])] += _safe_float(r["premium_usd"])
        top_strike = max(strike_premium.items(), key=lambda x: x[1])[0] if strike_premium else 0.0
        top_expiry = max(expiry_volume.items(), key=lambda x: x[1])[0] if expiry_volume else ""
        return {
            "atm_strike": round(min(strike_premium.keys(), key=lambda s: abs(s - spot)), 2) if strike_premium else 0.0,
            "top_volume_strike": round(top_strike, 2),
            "top_volume_expiration": top_expiry,
            "strike_vs_index": round(spot - top_strike, 2) if top_strike else 0.0,
        }

    def _build_big_trades(self, bubbles: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
        rows = [b for b in bubbles if _safe_float(b.get("premium_usd")) >= threshold]
        rows.sort(key=lambda x: str(x.get("snapshot_ts", "")), reverse=True)
        output: list[dict[str, Any]] = []
        for b in rows[:60]:
            output.append(
                {
                    "time": str(b.get("snapshot_ts", "")),
                    "expiry": str(b.get("expiry", "")),
                    "strike": round(_safe_float(b.get("strike")), 2),
                    "side": str(b.get("side", "")),
                    "premium_usd": round(_safe_float(b.get("premium_usd")), 2),
                    "contracts": round(_safe_float(b.get("contracts")), 3),
                }
            )
        return output

    def _latest_ticker_rows(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT
              t.symbol, t.expiry, t.strike, t.cp, t.amount_usdt, t.volume_contracts, t.trade_count, t.ts
            FROM option_ticker_24h_snapshots t
            INNER JOIN (
              SELECT symbol, MAX(id) AS max_id
              FROM option_ticker_24h_snapshots
              WHERE symbol LIKE ?
              GROUP BY symbol
            ) latest
            ON t.id = latest.max_id
            """,
            (f"{self._base_asset()}-%",),
        ).fetchall()
        return [dict(r) for r in rows]

    def _latest_oi_rows(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT
              o.symbol, o.expiry, o.strike, o.cp, o.oi_contracts, o.oi_usdt, o.ts
            FROM option_oi_snapshots o
            INNER JOIN (
              SELECT symbol, MAX(id) AS max_id
              FROM option_oi_snapshots
              WHERE symbol LIKE ?
              GROUP BY symbol
            ) latest
            ON o.id = latest.max_id
            """,
            (f"{self._base_asset()}-%",),
        ).fetchall()
        return [dict(r) for r in rows]

    def _select_panel_expiries(
        self,
        ticker_rows: list[dict[str, Any]],
        oi_rows: list[dict[str, Any]],
        now_local: datetime,
    ) -> dict[str, str]:
        stats: dict[str, dict[str, float]] = defaultdict(lambda: {"premium": 0.0, "oi": 0.0, "dte": 9999.0})
        for r in ticker_rows:
            expiry = str(r.get("expiry", ""))
            if not expiry:
                continue
            stats[expiry]["premium"] += _safe_float(r.get("amount_usdt"))
        for r in oi_rows:
            expiry = str(r.get("expiry", ""))
            if not expiry:
                continue
            stats[expiry]["oi"] += _safe_float(r.get("oi_usdt"))
        valid_expiries: list[str] = []
        for expiry in stats.keys():
            dt = _parse_expiry_label(expiry)
            if not dt:
                continue
            dte = (dt.date() - now_local.date()).days
            if dte < 0:
                continue
            stats[expiry]["dte"] = float(dte)
            valid_expiries.append(expiry)

        valid_expiries.sort(
            key=lambda e: (int(stats[e]["dte"]), -(stats[e]["premium"] + stats[e]["oi"] * 0.01)),
        )
        if not valid_expiries:
            return {"0DTE": "", "1DTE": "", "weekly": "", "monthly": ""}

        used: set[str] = set()

        def pick(filters: list[tuple[int, int]]) -> str:
            for min_dte, max_dte in filters:
                candidates = [
                    e
                    for e in valid_expiries
                    if e not in used and min_dte <= int(stats[e]["dte"]) <= max_dte
                ]
                if candidates:
                    chosen = max(candidates, key=lambda e: stats[e]["premium"] + stats[e]["oi"] * 0.01)
                    used.add(chosen)
                    return chosen
            for e in valid_expiries:
                if e not in used:
                    used.add(e)
                    return e
            return valid_expiries[0]

        # GMT+7 session mapping:
        # P1 = 0DTE (today) then nearest next day if today's contract missing.
        p1 = pick([(0, 0), (1, 1), (2, 2), (3, 7)])
        p2 = pick([(1, 1), (2, 2), (3, 3), (4, 7)])
        p3 = pick([(2, 7), (3, 10), (4, 14)])
        p4 = pick([(8, 45), (15, 90), (30, 180), (8, 365)])
        return {"0DTE": p1, "1DTE": p2, "weekly": p3, "monthly": p4}

    def _find_contract_start(
        self,
        conn: sqlite3.Connection,
        expiry: str,
        now_utc: datetime,
        now_local: datetime,
    ) -> datetime:
        if not expiry:
            return now_utc - timedelta(days=2)
        candidates: list[datetime] = []
        rows = [
            conn.execute("SELECT MIN(event_time) AS ts FROM option_trade_events WHERE expiry = ?", (expiry,)).fetchone(),
            conn.execute("SELECT MIN(ts) AS ts FROM option_ticker_24h_snapshots WHERE expiry = ?", (expiry,)).fetchone(),
            conn.execute("SELECT MIN(ts) AS ts FROM option_oi_snapshots WHERE expiry = ?", (expiry,)).fetchone(),
        ]
        for row in rows:
            if not row:
                continue
            dt = _parse_iso(str(row["ts"] or ""))
            if dt:
                candidates.append(dt.astimezone(timezone.utc))
        expiry_dt = _parse_expiry_label(expiry)
        if expiry_dt:
            dte = (expiry_dt.date() - now_local.date()).days
            if dte <= 1:
                baseline = now_utc - timedelta(days=2)
            elif dte <= 9:
                baseline = now_utc - timedelta(days=8)
            else:
                baseline = min(now_utc - timedelta(days=18), expiry_dt - timedelta(days=30))
        else:
            baseline = now_utc - timedelta(days=5)

        if not candidates:
            start_utc = baseline
        else:
            start_utc = min(min(candidates), baseline)
        if start_utc < now_utc - timedelta(days=120):
            start_utc = now_utc - timedelta(days=120)
        return start_utc

    def _fetch_expiry_bubbles(
        self,
        conn: sqlite3.Connection,
        expiry: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[dict[str, Any]]:
        if not expiry:
            return []
        rows = conn.execute(
            """
            SELECT event_time, strike, cp, side, qty, premium_usdt
            FROM option_trade_events
            WHERE expiry = ?
              AND event_time >= ?
              AND event_time <= ?
              AND premium_usdt > 0
            ORDER BY event_time ASC
            """,
            (expiry, start_utc.isoformat(), end_utc.isoformat()),
        ).fetchall()
        output: list[dict[str, Any]] = []
        for r in rows:
            premium = _safe_float(r["premium_usdt"])
            strike = _safe_float(r["strike"])
            qty = abs(_safe_float(r["qty"]))
            cp = str(r["cp"]).upper()
            side = str(r["side"]).upper()
            mapped_side = self._map_side(cp=cp, side=side)
            output.append(
                {
                    "ts": str(r["event_time"]),
                    "strike": strike,
                    "size": max(6.0, min(52.0, 5.0 + math.sqrt(max(premium, 1.0)) / 15.0)),
                    "side": mapped_side,
                    "premium_usd": round(premium, 2),
                    "contracts": round(qty, 3),
                }
            )
        if len(output) > 420:
            step = max(1, len(output) // 420)
            sampled = output[::step]
            if sampled and sampled[-1] != output[-1]:
                sampled.append(output[-1])
            output = sampled
        return output

    def _build_bubbles_from_ticker_history(
        self,
        conn: sqlite3.Connection,
        expiry: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[dict[str, Any]]:
        if not expiry:
            return []
        rows = conn.execute(
            """
            SELECT ts, strike, cp, SUM(amount_usdt) AS amount_usdt
            FROM option_ticker_24h_snapshots
            WHERE expiry = ?
              AND ts >= ?
              AND ts <= ?
            GROUP BY ts, strike, cp
            ORDER BY ts ASC
            """,
            (expiry, start_utc.isoformat(), end_utc.isoformat()),
        ).fetchall()
        if not rows:
            return []

        previous_amount: dict[tuple[float, str], float] = {}
        candidates: list[dict[str, Any]] = []
        deltas: list[float] = []
        for r in rows:
            ts = str(r["ts"])
            strike = _safe_float(r["strike"])
            cp = str(r["cp"]).upper()
            amount = _safe_float(r["amount_usdt"])
            if strike <= 0 or amount <= 0:
                continue
            key = (strike, cp)
            prev = previous_amount.get(key)
            if prev is None:
                delta = amount * 0.12
            elif amount >= prev:
                delta = amount - prev
            else:
                # 24h rolling ticker can reset; keep only a small fraction to avoid false spikes.
                delta = amount * 0.08
            previous_amount[key] = amount
            if delta <= 0:
                continue
            deltas.append(delta)
            candidates.append(
                {
                    "ts": ts,
                    "strike": strike,
                    "cp": cp,
                    "delta": delta,
                }
            )
        if not candidates:
            return []

        deltas_sorted = sorted(deltas)
        percentile_idx = int(len(deltas_sorted) * 0.86)
        threshold = max(1200.0, deltas_sorted[min(max(percentile_idx, 0), len(deltas_sorted) - 1)])

        out: list[dict[str, Any]] = []
        for c in candidates:
            if c["delta"] < threshold:
                continue
            side = "long_call" if c["cp"] == "C" else "long_put"
            out.append(
                {
                    "ts": c["ts"],
                    "strike": round(c["strike"], 2),
                    "size": max(6.0, min(50.0, 5.0 + math.sqrt(c["delta"]) / 12.0)),
                    "side": side,
                    "premium_usd": round(c["delta"], 2),
                    "contracts": 0.0,
                }
            )
        if len(out) > 420:
            step = max(1, len(out) // 420)
            sampled = out[::step]
            if sampled and sampled[-1] != out[-1]:
                sampled.append(out[-1])
            out = sampled
        return out

    def _strike_flow_from_ticker(self, ticker_rows: list[dict[str, Any]], expiry: str) -> dict[float, float]:
        out: dict[float, float] = defaultdict(float)
        if not expiry:
            return out
        for r in ticker_rows:
            if str(r.get("expiry", "")) != expiry:
                continue
            strike = _safe_float(r.get("strike"))
            premium = _safe_float(r.get("amount_usdt"))
            if strike <= 0 or premium <= 0:
                continue
            out[strike] += premium
        if len(out) <= 48:
            return out
        top = sorted(out.items(), key=lambda x: x[1], reverse=True)[:48]
        return {k: v for k, v in top}

    def _compute_max_pain(
        self,
        oi_rows: list[dict[str, Any]],
        expiry: str,
    ) -> float:
        call_notional: dict[float, float] = defaultdict(float)
        put_notional: dict[float, float] = defaultdict(float)
        for r in oi_rows:
            if str(r.get("expiry", "")) != expiry:
                continue
            strike = _safe_float(r.get("strike"))
            if strike <= 0:
                continue
            oi_notional = self._normalize_oi_notional(
                strike=strike,
                oi_contracts=_safe_float(r.get("oi_contracts")),
                oi_usdt=_safe_float(r.get("oi_usdt")),
            )
            if oi_notional <= 0:
                continue
            cp = str(r.get("cp", "")).upper()
            if cp == "C":
                call_notional[strike] += oi_notional
            elif cp == "P":
                put_notional[strike] += oi_notional

        strikes = sorted(set(call_notional.keys()) | set(put_notional.keys()))
        if not strikes:
            return 0.0

        # Pure max pain on a consistent notional universe:
        # pain(k) = sum_c max(0, k-s)*C_notional(s) + sum_p max(0, s-k)*P_notional(s)
        # Evaluate on all valid strikes from this expiry.
        prefix_call_w = [0.0]
        prefix_call_ws = [0.0]
        prefix_put_w = [0.0]
        prefix_put_ws = [0.0]
        for s in strikes:
            c = call_notional.get(s, 0.0)
            p = put_notional.get(s, 0.0)
            prefix_call_w.append(prefix_call_w[-1] + c)
            prefix_call_ws.append(prefix_call_ws[-1] + c * s)
            prefix_put_w.append(prefix_put_w[-1] + p)
            prefix_put_ws.append(prefix_put_ws[-1] + p * s)

        total_put_w = prefix_put_w[-1]
        total_put_ws = prefix_put_ws[-1]

        best_strike = strikes[0]
        best_pain = float("inf")
        for i, k in enumerate(strikes, start=1):
            call_left_w = prefix_call_w[i]
            call_left_ws = prefix_call_ws[i]
            put_left_w = prefix_put_w[i - 1]
            put_left_ws = prefix_put_ws[i - 1]

            put_right_w = total_put_w - put_left_w
            put_right_ws = total_put_ws - put_left_ws

            call_pain = k * call_left_w - call_left_ws
            put_pain = put_right_ws - k * put_right_w
            pain = call_pain + put_pain
            if pain < best_pain:
                best_pain = pain
                best_strike = k
        return best_strike

    def _normalize_oi_notional(self, strike: float, oi_contracts: float, oi_usdt: float) -> float:
        if oi_usdt > 0:
            return oi_usdt
        if oi_contracts > 0 and strike > 0:
            # Binance options contract size can vary by product; fallback assumes
            # 1 underlying unit notionalized by strike to keep units consistent.
            return oi_contracts * strike
        return 0.0

    def _to_hanoi_iso(self, ts: str) -> str:
        dt = _parse_iso(str(ts))
        if not dt:
            return str(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(HANOI_TZ).isoformat()

    def _to_hanoi_series(self, series: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for item in series:
            out.append(
                {
                    "ts": self._to_hanoi_iso(str(item.get("ts", ""))),
                    "price": _safe_float(item.get("price")),
                }
            )
        return out

    def _to_hanoi_bubbles(self, bubbles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for b in bubbles:
            row = dict(b)
            row["ts"] = self._to_hanoi_iso(str(b.get("ts", "")))
            out.append(row)
        return out

    def _build_synthetic_bubbles(
        self,
        price_series: list[dict[str, Any]],
        strike_flow: dict[float, float],
    ) -> list[dict[str, Any]]:
        if not strike_flow:
            return []
        top = sorted(strike_flow.items(), key=lambda x: x[1], reverse=True)[:12]
        if not top:
            return []
        times = [str(x.get("ts", "")) for x in price_series if str(x.get("ts", ""))]
        if not times:
            times = [datetime.now(timezone.utc).isoformat()]
        strikes_only = [s for s, _ in top]
        pivot = sorted(strikes_only)[len(strikes_only) // 2]
        output: list[dict[str, Any]] = []
        for idx, (strike, premium) in enumerate(top):
            ts = times[min(len(times) - 1, max(0, int((idx + 1) * len(times) / (len(top) + 1))))]
            side = "long_call" if strike >= pivot else "long_put"
            output.append(
                {
                    "ts": ts,
                    "strike": round(strike, 2),
                    "size": max(7.0, min(46.0, 6.0 + math.sqrt(max(premium, 1.0)) / 60.0)),
                    "side": side,
                    "premium_usd": round(premium, 2),
                    "contracts": 0.0,
                }
            )
        return output

    def _fetch_price_series_for_window(
        self,
        conn: sqlite3.Connection,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[dict[str, Any]]:
        if end_utc <= start_utc:
            return []
        start_iso = start_utc.isoformat()
        end_iso = end_utc.isoformat()
        local_rows = conn.execute(
            """
            SELECT ts, index_price
            FROM option_index_snapshots
            WHERE ts >= ? AND ts <= ?
            ORDER BY id ASC
            """,
            (start_iso, end_iso),
        ).fetchall()
        local_series: list[dict[str, Any]] = []
        for r in local_rows:
            price = _safe_float(r["index_price"])
            if price > 0:
                local_series.append({"ts": str(r["ts"]), "price": round(price, 2)})

        if not local_series:
            fut_rows = conn.execute(
                """
                SELECT ts, COALESCE(index_price, mark_price) AS px
                FROM futures_snapshots
                WHERE market = 'UM' AND symbol = ?
                  AND ts >= ? AND ts <= ?
                ORDER BY id ASC
                """,
                (self._default_um_symbol(), start_iso, end_iso),
            ).fetchall()
            for r in fut_rows:
                price = _safe_float(r["px"])
                if price > 0:
                    local_series.append({"ts": str(r["ts"]), "price": round(price, 2)})

        local_series = self._downsample_price_series(local_series, max_points=520)
        coverage_hours = (end_utc - start_utc).total_seconds() / 3600.0
        local_coverage_ratio = 0.0
        if len(local_series) >= 2:
            first_ts = _parse_iso(str(local_series[0]["ts"]))
            last_ts = _parse_iso(str(local_series[-1]["ts"]))
            if first_ts and last_ts and last_ts > first_ts and coverage_hours > 0:
                local_coverage_ratio = (last_ts - first_ts).total_seconds() / (coverage_hours * 3600.0)
        if local_series and ((len(local_series) >= 160 and local_coverage_ratio >= 0.55) or coverage_hours <= 28):
            return local_series

        remote_series = self._fetch_futures_klines(start_utc=start_utc, end_utc=end_utc, interval="5m")
        if remote_series:
            return self._downsample_price_series(remote_series, max_points=520)
        return local_series

    def _fetch_futures_klines(
        self,
        start_utc: datetime,
        end_utc: datetime,
        interval: str = "5m",
    ) -> list[dict[str, Any]]:
        start_ms = int(start_utc.timestamp() * 1000)
        end_ms = int(end_utc.timestamp() * 1000)
        if end_ms <= start_ms:
            return []

        interval_ms = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000}.get(interval, 300_000)
        bucketed_start = (start_ms // interval_ms) * interval_ms
        bucketed_end = (end_ms // interval_ms) * interval_ms
        cache_key = f"{self._default_um_symbol()}:{interval}:{bucketed_start}:{bucketed_end}"
        now_ts = datetime.now(timezone.utc).timestamp()
        cached = self._price_cache.get(cache_key)
        if cached and now_ts - cached[0] < 180:
            return cached[1]

        limit = 1500
        cursor = bucketed_start
        out: list[dict[str, Any]] = []
        requests_made = 0
        while cursor < bucketed_end and requests_made < 16:
            params = urlencode(
                {
                    "symbol": self._default_um_symbol(),
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": bucketed_end,
                    "limit": limit,
                }
            )
            url = f"https://fapi.binance.com/fapi/v1/klines?{params}"
            try:
                with urlopen(url, timeout=8) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                break
            if not isinstance(data, list) or not data:
                break
            last_open = cursor
            for row in data:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                open_ms = int(_safe_float(row[0]))
                close_price = _safe_float(row[4])
                if close_price <= 0:
                    continue
                out.append(
                    {
                        "ts": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).isoformat(),
                        "price": round(close_price, 2),
                    }
                )
                last_open = open_ms
            if len(data) < limit:
                break
            cursor = last_open + interval_ms
            requests_made += 1

        out = self._downsample_price_series(out, max_points=900)
        self._price_cache[cache_key] = (now_ts, out)
        return out

    def _expiry_sort_key(self, expiry: str, now_utc: datetime, premium: float) -> tuple[int, int, float]:
        dt = _parse_expiry_label(expiry)
        if not dt:
            return (9999, 9999, -premium)
        dte = (dt.date() - now_utc.date()).days
        return (max(dte, 0), dt.weekday(), -premium)

    def _ensure_group_data(
        self,
        grouped: dict[str, list[dict[str, Any]]],
        by_expiry: dict[str, list[dict[str, Any]]],
        sorted_expiries: list[str],
    ) -> None:
        if not sorted_expiries:
            return
        fallback_idx = {
            "0DTE": 0,
            "1DTE": 1,
            "weekly": min(2, len(sorted_expiries) - 1),
            "monthly": len(sorted_expiries) - 1,
        }
        for group_name, idx in fallback_idx.items():
            if grouped.get(group_name):
                continue
            chosen_expiry = sorted_expiries[min(max(idx, 0), len(sorted_expiries) - 1)]
            grouped[group_name] = list(by_expiry.get(chosen_expiry, []))

    def _downsample_price_series(self, series: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
        if len(series) <= max_points:
            return series
        step = max(1, len(series) // max_points)
        sampled = series[::step]
        if sampled and sampled[-1] != series[-1]:
            sampled.append(series[-1])
        return sampled

    def _map_side(self, cp: str, side: str) -> str:
        is_buy = side.upper() == "BUY"
        if cp.upper() == "C":
            return "long_call" if is_buy else "short_call"
        return "long_put" if is_buy else "short_put"

    def _base_asset(self) -> str:
        # Used only for dashboard filtering without introducing extra config.
        return "BTC"

    def _default_um_symbol(self) -> str:
        return "BTCUSDT"


class DashboardHandler(BaseHTTPRequestHandler):
    metrics_builder: DashboardMetricsBuilder | None = None
    static_index_path: Path | None = None

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def _send_text(self, content: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
        data = content.encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/metrics":
            if not self.metrics_builder:
                self._send_json({"error": "metrics_builder_not_configured"}, status=500)
                return
            try:
                q = parse_qs(parsed.query or "")
                strict_raw = str((q.get("otl_strict") or ["0"])[0]).lower()
                otl_strict = strict_raw in {"1", "true", "yes", "on", "strict"}
                payload = self.metrics_builder.build(otl_strict=otl_strict)
                self._send_json(payload)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": "metrics_build_failed", "detail": str(exc)}, status=500)
            return

        if parsed.path == "/healthz":
            self._send_json({"ok": True})
            return

        if parsed.path in ("/", "/index.html"):
            if not self.static_index_path or not self.static_index_path.exists():
                self._send_text("Dashboard index file not found.", status=500)
                return
            html = self.static_index_path.read_text(encoding="utf-8")
            self._send_text(html, content_type="text/html; charset=utf-8")
            return

        self._send_text("Not found", status=404)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def run_server(host: str, port: int, db_path: Path) -> None:
    static_index = Path(__file__).resolve().parent / "static" / "index.html"
    DashboardHandler.metrics_builder = DashboardMetricsBuilder(db_path=db_path)
    DashboardHandler.static_index_path = static_index
    httpd = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port}")
    print(f"Using DB: {db_path}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenFang local metrics dashboard server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8088, help="Bind port")
    parser.add_argument("--db", default="", help="Optional custom SQLite path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.db:
        db_path = Path(args.db).resolve()
    else:
        project_root = Path(__file__).resolve().parents[2]
        db_path = load_settings(project_root).sqlite_path
    run_server(args.host, args.port, db_path)


if __name__ == "__main__":
    main()
