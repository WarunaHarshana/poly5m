"""High-speed WebSocket Feeds for Polymarket RTDS Chainlink and CLOB Level 2 Orderbook."""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import threading
import time

import config

logger = logging.getLogger("feeds")


class ChainlinkRTDSStream:
    """Streams live Chainlink Data Streams via Polymarket RTDS.

    This is Polymarket's exact settlement oracle source for 5-minute Up/Down markets.
    One socket; ticks are bucketed per asset (btc, eth, sol, ...).
    """

    def __init__(
        self,
        url: str = config.CHAINLINK_RTDS_URL,
        asset: str | None = None,
        assets: list[str] | None = None,
    ):
        self.url = url
        if assets:
            self.assets = [str(a).lower() for a in assets if a]
        elif asset:
            self.assets = [str(asset).lower()]
        else:
            self.assets = list(config.ASSETS)
        if not self.assets:
            self.assets = ["btc"]
        self.asset = self.assets[0]
        self.symbol = f"{self.asset}/usd"
        self._symbol_to_asset: dict[str, str] = {}
        for a in self.assets:
            for alias in (f"{a}/usd", f"{a}-usd", f"{a}usd"):
                self._symbol_to_asset[alias] = a
        self._history: dict[str, collections.deque[tuple[float, float]]] = {
            a: collections.deque(maxlen=10000) for a in self.assets
        }
        self._lock = threading.Lock()
        self._stop = False
        self._thread: threading.Thread | None = None
        self._task: asyncio.Task | None = None
        self._ready_event = threading.Event()
        self._last_tick_time: dict[str, float] = {a: 0.0 for a in self.assets}
        self._backup_prices: dict[str, tuple[float, float]] = {}  # asset -> (price, ts)
        self._stream_clock_offset: float = 0.0  # (stream_ts - local_ts)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if self._task and not self._task.done():
            return
        self._stop = False
        try:
            self._task = asyncio.create_task(self._run())
        except RuntimeError:
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="chainlink-rtds")
            self._thread.start()

    def stop(self) -> None:
        self._stop = True
        if self._task:
            self._task.cancel()

    def wait_ready(self, timeout: float = 15.0) -> bool:
        return self._ready_event.wait(timeout)

    @property
    def ready(self) -> bool:
        return self._ready_event.is_set()

    def _asset_or_default(self, asset: str | None) -> str:
        a = str(asset or self.asset).lower()
        return a if a in self._history else self.asset

    def _fetch_backup_spot(self, asset: str) -> float | None:
        """Fallback to Binance or Coinbase REST if Chainlink tick is stale."""
        now = time.time()
        cached = self._backup_prices.get(asset)
        if cached and (now - cached[1]) < 1.0:
            return cached[0]

        import urllib.request
        sym = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "bnb": "BNBUSDT"}.get(asset.lower())
        if sym:
            try:
                req = urllib.request.Request(
                    f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                    headers={"User-Agent": "poly5m"},
                )
                with urllib.request.urlopen(req, timeout=1.2) as res:
                    data = json.loads(res.read().decode())
                    p = float(data["price"])
                    if p > 0:
                        self._backup_prices[asset] = (p, now)
                        return p
            except Exception:
                pass

        cb_sym = {"btc": "BTC-USD", "eth": "ETH-USD", "sol": "SOL-USD", "bnb": "BNB-USD"}.get(asset.lower())
        if cb_sym:
            try:
                req = urllib.request.Request(
                    f"https://api.coinbase.com/v2/prices/{cb_sym}/spot",
                    headers={"User-Agent": "poly5m"},
                )
                with urllib.request.urlopen(req, timeout=1.2) as res:
                    data = json.loads(res.read().decode())
                    p = float(data["data"]["amount"])
                    if p > 0:
                        self._backup_prices[asset] = (p, now)
                        return p
            except Exception:
                pass
        return None

    def price(self, asset: str | None = None) -> float:
        a = self._asset_or_default(asset)
        last_p = 0.0
        last_t = 0.0
        with self._lock:
            hist = self._history.get(a)
            if hist:
                last_p = hist[-1][1]
            last_t = self._last_tick_time.get(a, 0.0)

        # If fresh, return primary Chainlink price immediately
        if last_p > 0 and (time.time() - last_t) <= config.MAX_ORACLE_STALE_S:
            return last_p

        # If stale or missing, query fast backup REST
        backup_p = self._fetch_backup_spot(a)
        if backup_p and backup_p > 0:
            return backup_p

        return last_p

    def clock_offset_s(self) -> float:
        return self._stream_clock_offset

    def ready_for(self, asset: str) -> bool:
        a = str(asset).lower()
        with self._lock:
            return bool(self._history.get(a))

    def history_snapshot(self, asset: str | None = None) -> list[tuple[float, float]]:
        """Copy of the (ts, price) tick history for an asset (thread-safe)."""
        a = self._asset_or_default(asset)
        with self._lock:
            return list(self._history.get(a) or [])

    def get_slot_strike(
        self, slot_start_ts: float, max_dist_s: float = 5.0, asset: str | None = None
    ) -> float | None:
        """Find the tick closest to slot start time."""
        a = self._asset_or_default(asset)
        with self._lock:
            hist = self._history.get(a)
            if not hist:
                return None
            best_p = None
            best_diff = float("inf")
            for ts, p in hist:
                diff = abs(ts - slot_start_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_p = p
            if best_diff <= max_dist_s:
                return best_p
            return None

    def compute_realized_twap(
        self, start_ts: float, end_ts: float | None = None, asset: str | None = None
    ) -> tuple[float, float]:
        """Continuous trapezoidal TWAP integration over [start_ts, end_ts]."""
        a = self._asset_or_default(asset)
        with self._lock:
            hist = self._history.get(a)
            if not hist:
                return 0.0, 0.0

            ticks = list(hist)
            end = float(end_ts) if end_ts is not None else (ticks[-1][0])
            start = float(start_ts)
            elapsed = max(0.0, end - start)

            if elapsed <= 0.0:
                return ticks[-1][1], 0.0

            # Filter ticks in window [start, end]
            window_ticks = [(ts, p) for ts, p in ticks if ts >= start and ts <= end]

            if not window_ticks:
                # Nearest tick before window
                prior = [p for ts, p in ticks if ts < start]
                last_p = prior[-1] if prior else ticks[-1][1]
                return last_p, elapsed

            # Pad start and end
            pts = []
            if window_ticks[0][0] > start:
                # Use last known tick BEFORE the window to avoid lookahead bias
                prior = [p for ts, p in ticks if ts <= start]
                start_price = prior[-1] if prior else window_ticks[0][1]
                pts.append((start, start_price))
            pts.extend(window_ticks)
            if pts[-1][0] < end:
                pts.append((end, pts[-1][1]))

            # Trapezoidal integration
            integral = 0.0
            for i in range(len(pts) - 1):
                t0, p0 = pts[i]
                t1, p1 = pts[i + 1]
                dt = t1 - t0
                if dt > 0:
                    integral += dt * (p0 + p1) / 2.0

            realized_twap = integral / elapsed if elapsed > 0 else pts[-1][1]
            return realized_twap, elapsed

    def strike_crossings_since(
        self,
        start_ts: float,
        strike: float,
        noise_buffer_usd: float = 3.0,
        asset: str | None = None,
    ) -> int:
        """Count the number of times spot significantly crossed strike since start_ts, filtering noise."""
        a = self._asset_or_default(asset)
        with self._lock:
            hist = self._history.get(a) or []
            ticks = [p for ts, p in hist if ts >= start_ts]
        if len(ticks) < 2 or strike <= 0:
            return 0
        crossings = 0
        current_regime = None
        for p in ticks:
            if p >= strike + noise_buffer_usd:
                new_regime = 1
            elif p <= strike - noise_buffer_usd:
                new_regime = -1
            else:
                new_regime = current_regime
            if current_regime is not None and new_regime != current_regime:
                crossings += 1
            current_regime = new_regime
        return crossings

    def mark_stale(self, reason: str = "", asset: str | None = None) -> None:
        """Force is_stale() true until a fresh RTDS tick arrives (poly15m pattern)."""
        targets = [self._asset_or_default(asset)] if asset else list(self.assets)
        with self._lock:
            had = any(self._last_tick_time.get(a, 0.0) > 0 for a in targets)
            for a in targets:
                self._last_tick_time[a] = 0.0
        if had:
            who = asset.upper() if asset else "ALL"
            logger.warning(
                f"[ChainlinkRTDS] Marked stale {who} ({reason or 'unknown'}). Waiting for a fresh tick."
            )

    def is_stale(self, max_age_s: float | None = None, asset: str | None = None) -> bool:
        a = self._asset_or_default(asset)
        age = float(config.MAX_ORACLE_STALE_S if max_age_s is None else max_age_s)
        with self._lock:
            hist = self._history.get(a)
            if not hist:
                return True
            return (time.time() - self._last_tick_time.get(a, 0.0)) > age

    def _record(self, ts: float, price: float, asset: str | None = None) -> None:
        a = self._asset_or_default(asset)
        with self._lock:
            hist = self._history.setdefault(a, collections.deque(maxlen=10000))
            hist.append((ts, price))
            self._last_tick_time[a] = time.time()
            self._ready_event.set()

    def _run_loop(self) -> None:
        asyncio.run(self._run())

    async def _run(self) -> None:
        import websockets
        while not self._stop:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=12,
                    close_timeout=2,
                    max_size=2**20,
                ) as ws:
                    sub = {
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "",
                        }],
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("[ChainlinkRTDS] Connected and subscribed to tick feed")
                    while not self._stop:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=25.0)
                        except asyncio.TimeoutError:
                            logger.warning("[ChainlinkRTDS] Inactivity timeout (>25s no message), reconnecting stream...")
                            self.mark_stale("rtds_silence")
                            break
                        if not raw or not raw.strip() or raw in ("PONG", "pong"):
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        payload = msg.get("payload") or msg.get("data") or (msg if "symbol" in msg else {})
                        sym = str(payload.get("symbol") or "").lower()
                        asset = self._symbol_to_asset.get(sym)
                        if not asset:
                            continue
                        try:
                            value = float(payload.get("value") if payload.get("value") is not None else payload.get("price"))
                        except (TypeError, ValueError):
                            continue
                        ts_ms = payload.get("timestamp") or msg.get("timestamp")
                        try:
                            ts_s = float(ts_ms) / 1000.0 if float(ts_ms) > 1e11 else float(ts_ms)
                        except (TypeError, ValueError):
                            ts_s = time.time()
                        self._stream_clock_offset = ts_s - time.time()
                        if value > 0:
                            was_stale = self.is_stale(max_age_s=6.0, asset=asset)
                            self._record(ts_s, value, asset=asset)
                            if was_stale:
                                logger.info(f"[ChainlinkRTDS] {asset.upper()} stream resumed: ${value}")
            except Exception as e:
                self.mark_stale(f"rtds_error:{e}")
                logger.warning(f"[ChainlinkRTDS] connection error: {e}")
                await asyncio.sleep(2.0)


class PolymarketBookStream:
    """Streams live Level 2 orderbook quotes from Polymarket CLOB WebSocket."""

    def __init__(self, url: str = config.CLOB_WS_URL, max_age_s: float = 5.0):
        self.url = url
        self.max_age_s = max_age_s
        self._books: dict[str, dict] = {}  # token_id -> {bid, ask, bid_size, ask_size, updated_at}
        self._lock = threading.Lock()
        self._assets: set[str] = set()
        self._thread: threading.Thread | None = None
        self._task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if self._task and not self._task.done():
            return
        self._stop = False
        try:
            self._task = asyncio.create_task(self._run())
        except RuntimeError:
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="clob-ws")
            self._thread.start()

    def stop(self) -> None:
        self._stop = True
        if self._task:
            self._task.cancel()

    def watch(self, *token_ids: str) -> None:
        """Subscribe to token IDs."""
        new_ids = {t for t in token_ids if t}
        with self._lock:
            diff = new_ids - self._assets
            self._assets.update(new_ids)
            full_list = sorted(self._assets)
        if diff and self._ws:
            try:
                if self._loop and self._loop != asyncio.get_event_loop():
                    asyncio.run_coroutine_threadsafe(self._send_subscribe(self._ws, full_list), self._loop)
                else:
                    asyncio.create_task(self._send_subscribe(self._ws, full_list))
            except Exception:
                pass

    def get_top(self, token_id: str, opp_token_id: str | None = None) -> dict | None:
        """Return {bid, ask, spread, bid_size, ask_size, age_s} or None if missing/stale.

        If opp_token_id is provided, infers complementary quotes (e.g. ask = 1.0 - opp_bid)
        when one side of the book is empty.
        """
        now = time.time()
        with self._lock:
            b = self._books.get(token_id)
            b_opp = self._books.get(opp_token_id) if opp_token_id else None

            if not b and not b_opp:
                return None

            age = (now - b["updated_at"]) if b else (now - b_opp["updated_at"])
            if self.max_age_s > 0 and age > self.max_age_s:
                return None

            bid = b.get("bid") if b else None
            ask = b.get("ask") if b else None
            bid_size = b.get("bid_size", 0) if b else 0
            ask_size = b.get("ask_size", 0) if b else 0

            # Complementary inference: use opposite book to infer or improve quotes
            if b_opp:
                opp_bid = b_opp.get("bid")
                opp_ask = b_opp.get("ask")
                if opp_bid is not None and 0.0 < opp_bid < 1.0:
                    implied_ask = round(1.0 - opp_bid, 4)
                    if ask is None or implied_ask < ask:
                        ask = implied_ask
                        ask_size = b_opp.get("bid_size", 0)
                if opp_ask is not None and 0.0 < opp_ask < 1.0:
                    implied_bid = round(1.0 - opp_ask, 4)
                    if bid is None or implied_bid > bid:
                        bid = implied_bid
                        bid_size = b_opp.get("ask_size", 0)

            spread = round(ask - bid, 4) if (ask is not None and bid is not None) else None
            return {
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "age_s": age,
            }

    async def _send_subscribe(self, ws, assets: list[str]) -> None:
        if not assets:
            return
        await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))

    def _apply_update(self, token_id: str, bid: float | None, ask: float | None, bid_sz: float = 0, ask_sz: float = 0) -> None:
        now = time.time()
        with self._lock:
            b = self._books.setdefault(token_id, {"bid": None, "ask": None, "bid_size": 0, "ask_size": 0, "updated_at": now})
            if bid is not None and 0.0 < bid < 1.0:
                b["bid"] = bid
                b["bid_size"] = bid_sz
            if ask is not None and 0.0 < ask < 1.0:
                b["ask"] = ask
                b["ask_size"] = ask_sz
            b["updated_at"] = now

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._run())

    async def _run(self) -> None:
        import websockets
        while not self._stop:
            try:
                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._ws = ws
                    with self._lock:
                        assets = sorted(self._assets)
                    if assets:
                        await self._send_subscribe(ws, assets)

                    pinger = asyncio.create_task(self._pinger(ws))
                    try:
                        while not self._stop:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                            except asyncio.TimeoutError:
                                logger.warning("[CLOB WS] Heartbeat timeout (>15s no message), reconnecting...")
                                break
                            if not raw or raw == "PONG":
                                continue
                            try:
                                msg = json.loads(raw)
                                items = msg if isinstance(msg, list) else [msg]
                                for item in items:
                                    if not isinstance(item, dict):
                                        continue
                                    event_type = item.get("event_type")
                                    if event_type == "book":
                                        asset_id = item.get("asset_id")
                                        bids = item.get("bids") or []
                                        asks = item.get("asks") or []
                                        best_bid = None
                                        best_bid_sz = 0.0
                                        best_ask = None
                                        best_ask_sz = 0.0
                                        for b in bids:
                                            try:
                                                p = float(b["price"])
                                                if 0.0 < p < 1.0 and (best_bid is None or p > best_bid):
                                                    best_bid = p
                                                    best_bid_sz = float(b.get("size", 0) or 0)
                                            except (KeyError, TypeError, ValueError):
                                                continue
                                        for a in asks:
                                            try:
                                                p = float(a["price"])
                                                if 0.0 < p < 1.0 and (best_ask is None or p < best_ask):
                                                    best_ask = p
                                                    best_ask_sz = float(a.get("size", 0) or 0)
                                            except (KeyError, TypeError, ValueError):
                                                continue
                                        if asset_id:
                                            self._apply_update(asset_id, best_bid, best_ask, best_bid_sz, best_ask_sz)
                                    elif event_type == "price_change":
                                        changes = item.get("price_changes") or []
                                        for c in changes:
                                            asset_id = c.get("asset_id")
                                            if not asset_id:
                                                continue
                                            b_px = float(c["price"]) if c.get("side") == "BUY" else None
                                            a_px = float(c["price"]) if c.get("side") == "SELL" else None
                                            sz = float(c.get("size", 0) or 0)
                                            self._apply_update(asset_id, b_px, a_px, sz if b_px else 0, sz if a_px else 0)
                            except Exception:
                                continue
                    finally:
                        pinger.cancel()
            except Exception:
                await asyncio.sleep(2.0)

    async def _pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(10.0)
            try:
                await ws.send("PING")
            except Exception:
                break

