"""BTC price feed.

- SimulatedPriceFeed: reproducible geometric random walk (offline paper mode).
- ChainlinkDataStream: Polymarket RTDS `crypto_prices_chainlink` (btc/usd) —
  the SAME resolution source Polymarket cites for BTC 5m Up/Down markets
  (https://data.chain.link/streams/btc-usd). Prefer this for strike + settle.
- LivePriceFeed: multi-source spot (Data Streams first, then exchange/oracles).

NOTE: Exchange candles (Binance/Coinbase) and the Arbitrum Chainlink *aggregator*
are NOT the Polymarket resolution source. They can disagree on close calls.
"""
from __future__ import annotations
import asyncio
import json
import math
import random
import threading
import time
from collections import deque

import config


class SimulatedPriceFeed:
    """Geometric random walk with mild drift + volatility. Deterministic by seed."""

    def __init__(self, start: float = 68000.0, vol: float = 0.0009,
                 drift: float = 0.00003, seed: int | None = 42):
        self.rng = random.Random(seed)
        self.p = start
        self.vol = vol
        self.drift = drift
        self.history: deque[float] = deque(maxlen=512)
        self.history.append(self.p)

    def step(self) -> float:
        shock = self.rng.gauss(self.drift, self.vol)
        self.p *= math.exp(shock)
        self.history.append(self.p)
        return self.p

    def price(self) -> float:
        return self.p

    def recent(self, n: int) -> list[float]:
        return list(self.history)[-n:]


class ChainlinkDataStream:
    """Live Chainlink BTC/USD via Polymarket Real-Time Data Socket (RTDS).

    Polymarket 5m BTC Up/Down rules:
      resolve Up iff end_price >= start_price on Chainlink Data Streams BTC/USD.
    This class streams that feed (topic crypto_prices_chainlink, symbol btc/usd)
    and keeps a short history so we can look up prices near slot boundaries.
    """

    WS_URL = "wss://ws-live-data.polymarket.com"
    SYMBOL = "btc/usd"

    def __init__(self, history_maxlen: int = 20_000):
        # (unix_ts_seconds float, price float) — stream timestamps (UTC)
        self._ticks: deque[tuple[float, float]] = deque(maxlen=history_maxlen)
        self._latest: float | None = None
        self._latest_ts: float | None = None
        self._local_latest_ts: float | None = None  # Local arrival time to detect freezes
        # stream_wall_clock - local_wall_clock (fixes PC clock skew vs Polymarket)
        self._clock_offset: float = 0.0
        self._lock = threading.Lock()
        self._stop_requested: bool = False
        self._task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self._err: str | None = None

    @property
    def ready(self) -> bool:
        with self._lock:
            # If the latest tick is older than 10 seconds, mark stream as NOT ready (stale/disconnected)
            if self._local_latest_ts is not None:
                if time.time() - self._local_latest_ts > 10.0:
                    return False
        return self._ready.is_set() and self._latest is not None

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_requested = False
        self._ready.clear()
        self._task = asyncio.create_task(self._async_loop())

    def stop(self) -> None:
        self._stop_requested = True
        if self._task:
            self._task.cancel()

    async def wait_ready(self, timeout: float = 15.0) -> bool:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
            return self._latest is not None
        except Exception:
            return False

    def market_now(self) -> float:
        """Wall clock aligned to Chainlink stream timestamps (not skewed PC clock)."""
        with self._lock:
            return time.time() + self._clock_offset

    def clock_offset_s(self) -> float:
        with self._lock:
            return float(self._clock_offset)

    def price(self) -> float:
        with self._lock:
            if self._latest is None:
                raise RuntimeError(
                    self._err or "Chainlink Data Stream not ready yet")
            return float(self._latest)

    def latest_tick(self) -> tuple[float, float] | None:
        """Return (price, stream_ts) for the newest tick, or None."""
        with self._lock:
            if self._latest is None or self._latest_ts is None:
                return None
            return float(self._latest), float(self._latest_ts)

    def price_at(self, unix_ts: float, max_skew_s: float = 5.0
                 ) -> tuple[float, float] | None:
        """Nearest tick to unix_ts (stream/UTC seconds). Returns (price, skew) or None."""
        with self._lock:
            if not self._ticks:
                return None
            best = None
            best_skew = None
            for ts, px in self._ticks:
                skew = abs(ts - unix_ts)
                if best_skew is None or skew < best_skew:
                    best_skew = skew
                    best = px
            if best is None or best_skew is None or best_skew > max_skew_s:
                return None
            return float(best), float(best_skew)

    def price_at_or_before(self, unix_ts: float, max_age_s: float = 10.0
                           ) -> tuple[float, float] | None:
        """Last tick at or before unix_ts (better for slot boundary open/close)."""
        with self._lock:
            if not self._ticks:
                return None
            best = None
            best_ts = None
            for ts, px in self._ticks:
                if ts <= unix_ts + 0.5:  # tiny tolerance for ms rounding
                    if best_ts is None or ts > best_ts:
                        best_ts = ts
                        best = px
            if best is None or best_ts is None:
                return None
            age = unix_ts - best_ts
            if age > max_age_s:
                return None
            return float(best), float(age)

    def _record(self, ts_s: float, value: float) -> None:
        local = time.time()
        sample_off = ts_s - local
        with self._lock:
            first = self._latest is None
            self._latest = value
            self._latest_ts = ts_s
            self._local_latest_ts = local
            # Track PC clock skew vs stream (often 10–20s on unsynced Windows).
            if first:
                self._clock_offset = sample_off
            else:
                self._clock_offset = 0.8 * self._clock_offset + 0.2 * sample_off
            self._ticks.append((ts_s, value))
        self._ready.set()

    async def _async_loop(self) -> None:
        import websockets  # optional dep; required for paper_live Chainlink stream

        is_first_tick = True
        while not self._stop_requested:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=None,
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
                    is_first_tick = True  # reset on reconnect to drop stale cached snapshot
                    while not self._stop_requested:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
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
                        payload = msg.get("payload") or {}
                        sym = str(payload.get("symbol") or "").lower()
                        if sym not in (self.SYMBOL, "btc-usd", "btcusd"):
                            continue
                        try:
                            value = float(payload.get("value"))
                        except (TypeError, ValueError):
                            continue
                        
                        # --- Drop First Tick (stale cached snapshot) ---
                        if is_first_tick:
                            is_first_tick = False
                            continue

                        # --- Delta Price Validation (reject anomalous jumps > $1,500) ---
                        if self._latest is not None:
                            delta = abs(value - self._latest)
                            if delta > 1500.0:
                                print(f"  [price_feed] WARNING: Rejected anomalous price tick jump: "
                                      f"${self._latest:,.2f} -> ${value:,.2f} (delta=${delta:,.2f})")
                                continue

                        # payload timestamp is ms
                        ts_ms = payload.get("timestamp") or msg.get("timestamp")
                        try:
                            ts_s = float(ts_ms) / 1000.0
                        except (TypeError, ValueError):
                            ts_s = time.time()
                        if value > 0:
                            self._record(ts_s, value)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._err = str(e)
                # brief backoff then reconnect
                try:
                    await asyncio.sleep(2.0)
                except Exception:
                    time.sleep(2.0)


class LivePriceFeed:
    """Real BTC/USD spot. Prefers Chainlink Data Streams (Polymarket RTDS).

    Other sources (Arbitrum aggregator, Pyth, exchanges) are fallbacks for
    connectivity only — they are NOT Polymarket's 5m resolution source.
    """

    def __init__(self, sources: list[str] | None = None, timeout: float = 8.0,
                 chainlink_stream: ChainlinkDataStream | None = None):
        import requests  # lazy: offline sim needs no deps
        self.requests = requests
        self.sources = sources or config.BTC_PRICE_SOURCES
        self.timeout = timeout
        self.history: deque[float] = deque(maxlen=512)
        self.stream = chainlink_stream
        if self.stream is None and any(
                s in ("chainlink_stream", "chainlink_ds", "rtds")
                for s in self.sources):
            self.stream = ChainlinkDataStream()
            self.stream.start()

    def start_resolution_stream(self) -> ChainlinkDataStream:
        """Ensure the Polymarket Chainlink Data Stream is running."""
        if self.stream is None:
            self.stream = ChainlinkDataStream()
        self.stream.start()
        return self.stream

    def _chainlink_stream(self):
        if self.stream is None:
            raise RuntimeError("Chainlink Data Stream not started")
        return self.stream.price()

    # Alias names that can appear in BTC_PRICE_SOURCES
    _chainlink_ds = _chainlink_stream
    _rtds = _chainlink_stream

    def _chainlink(self):
        # Arbitrum BTC/USD *aggregator* — NOT Data Streams. Fallback only.
        addr = "0x6ce185860a4963106506C203335A2910413708e9"
        rpcs = [
            "https://arb1.arbitrum.io/rpc",
            "https://rpc.ankr.com/arbitrum",
            "https://1rpc.io/arb",
            "https://arbitrum.llamarpc.com"
        ]
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": addr,
                    "data": "0xfeaf968c"  # latestRoundData() selector
                },
                "latest"
            ],
            "id": 1
        }
        headers = {'content-type': 'application/json'}
        errors = []
        for rpc in rpcs:
            try:
                r = self.requests.post(rpc, json=payload, headers=headers, timeout=self.timeout / 2.0)
                if r.status_code == 200:
                    res = r.json()
                    if "result" in res and res["result"] != "0x":
                        hex_data = res["result"].replace("0x", "")
                        chunks = [hex_data[i:i+64] for i in range(0, len(hex_data), 64)]
                        if len(chunks) > 1:
                            return int(chunks[1], 16) / 1e8
            except Exception as e:
                errors.append(f"{rpc}: {e}")
        raise RuntimeError("Chainlink RPC failed: " + ", ".join(errors))

    def _pyth(self):
        # Pyth BTC/USD Price Feed ID with 0x prefix
        btc_feed_id = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
        url = f"https://hermes.pyth.network/v2/updates/price/latest?ids[]={btc_feed_id}"
        r = self.requests.get(url, timeout=self.timeout)
        if r.status_code == 200:
            data = r.json()
            parsed = data["parsed"][0]
            price_data = parsed["price"]
            return int(price_data["price"]) * (10 ** price_data["expo"])
        raise RuntimeError(f"Pyth HTTP status: {r.status_code}")

    def _coinbase(self):
        r = self.requests.get(
            "https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=self.timeout)
        return float(r.json()["data"]["amount"])

    def _binance(self):
        r = self.requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"}, timeout=self.timeout)
        return float(r.json()["price"])

    def _kraken(self):
        r = self.requests.get(
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": "XBTUSD"}, timeout=self.timeout)
        res = r.json()["result"]
        key = next(iter(res))
        return float(res[key]["c"][0])  # last trade close

    def price(self) -> float:
        # Always try the resolution stream first when available — matches Polymarket rules.
        if self.stream is not None and self.stream.ready:
            try:
                p = self.stream.price()
                self.history.append(p)
                return p
            except Exception:
                pass

        errors = []
        # Prefer chainlink_stream name if listed; otherwise walk configured sources.
        ordered = list(self.sources)
        for preferred in ("chainlink_stream", "chainlink_ds", "rtds"):
            if preferred in ordered:
                ordered.remove(preferred)
                ordered.insert(0, preferred)
        for src in ordered:
            try:
                fn = getattr(self, "_" + src, None)
                if fn is None:
                    continue
                p = fn()
                self.history.append(p)
                return p
            except Exception as e:  # try next source
                errors.append(f"{src}: {e}")
        raise RuntimeError("All BTC price sources failed -> " + " | ".join(errors))

    def recent(self, n: int) -> list[float]:
        return list(self.history)[-n:]

    def get_slot_start_price(self, slot_start_ts: int) -> float | None:
        """Price at the beginning of the slot window (Polymarket strike).

        1) Chainlink Data Stream tick nearest to slot open (official source)
        2) Exchange 5m candle open as *proxy only* (may disagree with Chainlink)
        """
        # 1) Official: Chainlink Data Streams via RTDS buffer
        # Wider windows help mid-slot restarts still latch a near-open tick.
        if self.stream is not None:
            hit = self.stream.price_at_or_before(float(slot_start_ts), max_age_s=30.0)
            if hit is None:
                hit2 = self.stream.price_at(float(slot_start_ts), max_skew_s=15.0)
                hit = (hit2[0], hit2[1]) if hit2 else None
            if hit is not None:
                price, skew = hit
                print(f"  [price_feed] Chainlink Data Stream strike for ts "
                      f"{slot_start_ts}: ${price:,.2f} (d={skew:.2f}s)")
                return price
            # If stream is live but we joined mid-slot, we may lack the open tick.
            print(f"  [price_feed] Chainlink stream has no tick near slot open "
                  f"{slot_start_ts}; falling back to exchange candle (proxy only)")

        # 2) Proxy: Coinbase 5m open (NOT official)
        try:
            headers = {"User-Agent": "polymarket-btc-bot/1.0"}
            r = self.requests.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/candles",
                params={"granularity": "300", "limit": "5"},
                headers=headers,
                timeout=(3.05, self.timeout),
            )
            if r.status_code == 200:
                for candle in r.json():
                    if int(candle[0]) == slot_start_ts:
                        price = float(candle[3])  # open is index 3
                        print(f"  [price_feed] Coinbase 5m open PROXY for ts "
                              f"{slot_start_ts}: ${price:,.2f} "
                              f"(not Chainlink - signal only)")
                        return price
        except Exception as e:
            print(f"  [price_feed] Coinbase candles failed: {e}")

        # 3) Proxy: Binance 5m open
        try:
            r = self.requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "5m", "limit": "5"},
                timeout=(3.05, self.timeout),
            )
            if r.status_code == 200:
                for candle in r.json():
                    if int(candle[0]) // 1000 == slot_start_ts:
                        price = float(candle[1])  # open is index 1
                        print(f"  [price_feed] Binance 5m open PROXY for ts "
                              f"{slot_start_ts}: ${price:,.2f} "
                              f"(not Chainlink - signal only)")
                        return price
        except Exception as e:
            print(f"  [price_feed] Binance klines failed: {e}")

        print(f"  [price_feed] Could not fetch start price for slot {slot_start_ts}")
        return None

    def get_slot_close_price(self, slot_start_ts: int) -> float | None:
        """Price at the end of the slot window (Polymarket settlement price).

        Prefer Chainlink Data Stream tick at slot_end = start + SLOT_SECONDS.
        Never use a stale stream "latest" as close (frozen ticks caused bad
        local resolves). Exchange candles are proxy-only fallback.
        """
        slot_end_ts = int(slot_start_ts) + int(getattr(config, "SLOT_SECONDS", 300))

        if self.stream is not None:
            # Wait for end-of-window ticks (stream can lag a few seconds).
            # Wider age/skew than open: close ticks often arrive slightly late.
            for attempt in range(30):  # up to ~15s
                hit = self.stream.price_at_or_before(
                    float(slot_end_ts), max_age_s=15.0)
                if hit is None:
                    hit2 = self.stream.price_at(
                        float(slot_end_ts), max_skew_s=8.0)
                    hit = (hit2[0], hit2[1]) if hit2 else None
                if hit is not None:
                    price, skew = hit
                    print(f"  [price_feed] Chainlink Data Stream close for ts "
                          f"{slot_end_ts}: ${price:,.2f} (d={skew:.2f}s)")
                    return price
                # Only accept "latest" if the tick timestamp is near slot end
                # (not a frozen price from minutes ago).
                latest = self.stream.latest_tick()
                if latest is not None:
                    p, ts = latest
                    age_vs_end = abs(ts - float(slot_end_ts))
                    if age_vs_end <= 8.0:
                        print(f"  [price_feed] Chainlink Data Stream close "
                              f"(fresh latest) for ts {slot_end_ts}: "
                              f"${p:,.2f} (tick_age_vs_end={age_vs_end:.1f}s)")
                        return p
                time.sleep(0.5)
            print(f"  [price_feed] no fresh Chainlink close near "
                  f"{slot_end_ts}; trying exchange candle proxy")

        # Proxy fallbacks (may disagree with Polymarket)
        try:
            headers = {"User-Agent": "polymarket-btc-bot/1.0"}
            r = self.requests.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/candles",
                params={"granularity": "300", "limit": "5"},
                headers=headers,
                timeout=(3.05, self.timeout),
            )
            if r.status_code == 200:
                for candle in r.json():
                    if int(candle[0]) == slot_start_ts:
                        price = float(candle[4])  # close is index 4
                        print(f"  [price_feed] Coinbase 5m close PROXY: ${price:,.2f}")
                        return price
        except Exception as e:
            print(f"  [price_feed] Coinbase close candles failed: {e}")

        try:
            r = self.requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "5m", "limit": "5"},
                timeout=(3.05, self.timeout),
            )
            if r.status_code == 200:
                for candle in r.json():
                    if int(candle[0]) // 1000 == slot_start_ts:
                        if len(candle) > 6 and int(candle[6]) // 1000 <= int(time.time()):
                            price = float(candle[4])
                            print(f"  [price_feed] Binance 5m close PROXY: ${price:,.2f}")
                            return price
        except Exception as e:
            print(f"  [price_feed] Binance close klines failed: {e}")

        return None
