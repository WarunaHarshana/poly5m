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
    """Streams live Chainlink Data Streams (BTC/USD) via Polymarket RTDS.
    
    This is Polymarket's exact settlement oracle source for 5-minute BTC markets.
    """

    def __init__(self, url: str = config.CHAINLINK_RTDS_URL, asset: str = "btc"):
        self.url = url
        self.symbol = f"{asset}/usd"
        self._history: collections.deque[tuple[float, float]] = collections.deque(maxlen=10000)
        self._lock = threading.Lock()
        self._stop = False
        self._thread: threading.Thread | None = None
        self._ready_event = threading.Event()
        self._last_tick_time: float = 0.0
        self._stream_clock_offset: float = 0.0  # (stream_ts - local_ts)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="chainlink-rtds")
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def wait_ready(self, timeout: float = 15.0) -> bool:
        return self._ready_event.wait(timeout)

    @property
    def ready(self) -> bool:
        return self._ready_event.is_set()

    def price(self) -> float:
        with self._lock:
            if not self._history:
                return 0.0
            return self._history[-1][1]

    def clock_offset_s(self) -> float:
        return self._stream_clock_offset

    def get_slot_strike(self, slot_start_ts: float, max_dist_s: float = 30.0) -> float | None:
        """Find the tick closest to slot start time."""
        with self._lock:
            if not self._history:
                return None
            best_p = None
            best_diff = float("inf")
            for ts, p in self._history:
                diff = abs(ts - slot_start_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_p = p
            if best_diff <= max_dist_s:
                return best_p
            return None

    def compute_realized_twap(self, start_ts: float, end_ts: float | None = None) -> tuple[float, float]:
        """Continuous trapezoidal TWAP integration over [start_ts, end_ts]."""
        with self._lock:
            if not self._history:
                return 0.0, 0.0

            ticks = list(self._history)
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
                pts.append((start, window_ticks[0][1]))
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

    def strike_crossings_since(self, start_ts: float, strike: float) -> int:
        """Count the number of times spot crossed strike since start_ts."""
        with self._lock:
            ticks = [p for ts, p in self._history if ts >= start_ts]
        if len(ticks) < 2 or strike <= 0:
            return 0
        crossings = 0
        prev_side = ticks[0] >= strike
        for p in ticks[1:]:
            side = p >= strike
            if side != prev_side:
                crossings += 1
                prev_side = side
        return crossings

    def _record(self, ts: float, price: float) -> None:
        with self._lock:
            self._history.append((ts, price))
            self._last_tick_time = ts
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
                    while not self._stop:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        except asyncio.TimeoutError:
                            try:
                                await ws.send("PING")
                            except Exception:
                                break
                            continue
                        if not raw or raw in ("PONG", "pong"):
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue
                        payload = msg.get("payload") or msg.get("data") or {}
                        sym = str(payload.get("symbol") or "").lower()
                        if sym not in (self.symbol, f"{self.asset}-usd", f"{self.asset}usd"):
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
                            self._record(ts_s, value)
            except Exception:
                await asyncio.sleep(2.0)


class PolymarketBookStream:
    """Streams live Level 2 orderbook quotes from Polymarket CLOB WebSocket."""

    def __init__(self, url: str = config.CLOB_WS_URL, max_age_s: float = 5.0):
        self.url = url
        self.max_age_s = max_age_s
        self._books: dict[str, dict] = {}  # token_id -> {bid, ask, bid_size, ask_size, updated_at}
        self._lock = threading.Lock()
        self._assets: set[str] = set()
        self._stop = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="clob-ws")
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def watch(self, *token_ids: str) -> None:
        """Subscribe to token IDs."""
        new_ids = {t for t in token_ids if t}
        with self._lock:
            diff = new_ids - self._assets
            self._assets.update(new_ids)
            full_list = sorted(self._assets)
        if diff and self._loop and self._ws:
            try:
                asyncio.run_coroutine_threadsafe(self._send_subscribe(self._ws, full_list), self._loop)
            except Exception:
                pass

    def get_top(self, token_id: str) -> dict | None:
        """Return {bid, ask, spread, bid_size, ask_size, age_s} or None if missing/stale."""
        now = time.time()
        with self._lock:
            b = self._books.get(token_id)
            if not b:
                return None
            age = now - b["updated_at"]
            if self.max_age_s > 0 and age > self.max_age_s:
                return None
            ask = b.get("ask")
            bid = b.get("bid")
            spread = round(ask - bid, 4) if (ask is not None and bid is not None) else None
            return {
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "bid_size": b.get("bid_size"),
                "ask_size": b.get("ask_size"),
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
                        async for raw in ws:
                            if self._stop:
                                break
                            if raw == "PONG":
                                continue
                            try:
                                msg = json.loads(raw)
                                event_type = msg.get("event_type")
                                if event_type == "book":
                                    asset_id = msg.get("asset_id")
                                    bids = msg.get("bids") or []
                                    asks = msg.get("asks") or []
                                    best_bid = float(bids[0]["price"]) if bids else None
                                    best_bid_sz = float(bids[0].get("size", 0)) if bids else 0
                                    best_ask = float(asks[0]["price"]) if asks else None
                                    best_ask_sz = float(asks[0].get("size", 0)) if asks else 0
                                    if asset_id:
                                        self._apply_update(asset_id, best_bid, best_ask, best_bid_sz, best_ask_sz)
                                elif event_type == "price_change":
                                    changes = msg.get("price_changes") or []
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
