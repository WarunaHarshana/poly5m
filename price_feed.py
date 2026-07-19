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

    def __init__(self, asset: str = "btc", history_maxlen: int = 20_000):
        self.asset = asset.lower()
        self.SYMBOL = f"{self.asset}/usd"
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
            # If the latest tick is older than 5 seconds, mark stream as NOT ready (stale/disconnected)
            if self._local_latest_ts is not None:
                if time.time() - self._local_latest_ts > 5.0:
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

    def ticks_since(self, unix_ts: float) -> list[tuple[float, float]]:
        """All (stream_ts, price) ticks at/after unix_ts, oldest first.

        The stream buffers ~1 tick/sec with real timestamps — far better than
        the sparse, irregularly-spaced polled history for realized vol and
        chop detection, and it can be scoped to the CURRENT slot.
        """
        with self._lock:
            return [(ts, px) for ts, px in self._ticks if ts >= unix_ts]

    def realized_vol_daily_since(self, unix_ts: float) -> float | None:
        """Realized daily vol from real tick spacing since unix_ts.

        Uses each tick's own dt instead of assuming a fixed sample interval
        (the polled history assumed 5s while really being 15s+ apart, which
        overstated vol ~1.7x and pulled model probabilities toward 0.5).
        """
        ticks = self.ticks_since(unix_ts)
        if len(ticks) < 4:
            return None
        var_sum = 0.0
        n = 0
        for (t0, p0), (t1, p1) in zip(ticks, ticks[1:]):
            dt = t1 - t0
            if dt <= 0 or p0 <= 0 or p1 <= 0:
                continue
            r = math.log(p1 / p0)
            var_sum += (r * r) / dt      # per-second variance contribution
            n += 1
        if n < 3:
            return None
        var_per_sec = var_sum / n
        daily = math.sqrt(max(var_per_sec, 0.0) * 86400.0)
        return max(0.01, min(0.15, daily))

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
                    # Protocol-level keepalive: with ping disabled the server
                    # silently dropped idle connections overnight, costing
                    # slot-open ticks (68 proxy_strike skips in one session).
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
                    is_first_tick = True  # reset on reconnect to drop stale cached snapshot
                    # Log how big the data hole was when we reconnect — this
                    # is the diagnostic for proxy_strike (missed slot opens).
                    with self._lock:
                        last_local = self._local_latest_ts
                    if last_local is not None:
                        gap = time.time() - last_local
                        if gap > 5.0:
                            print(f"  [price_feed] stream reconnected after "
                                  f"{gap:.0f}s data gap")
                    silent = 0
                    while not self._stop_requested:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                            silent = 0
                        except asyncio.TimeoutError:
                            silent += 1
                            # 12s of silence = connection is a zombie (ticks
                            # normally arrive ~1/s). Force a fresh socket
                            # instead of trusting app-level PING forever.
                            if silent >= 4:
                                print("  [price_feed] no ticks for ~12s; "
                                      "forcing reconnect")
                                break
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
                        if sym not in (self.SYMBOL, f"{self.asset}-usd", f"{self.asset}usd"):
                            continue
                        try:
                            value = float(payload.get("value"))
                        except (TypeError, ValueError):
                            continue
                        
                        # payload timestamp is ms
                        ts_ms = payload.get("timestamp") or msg.get("timestamp")
                        try:
                            ts_s = float(ts_ms) / 1000.0
                        except (TypeError, ValueError):
                            ts_s = time.time()

                        # --- First tick after (re)connect: drop only if it is
                        # actually a stale cached snapshot. Unconditionally
                        # dropping it threw away good ticks right at slot
                        # opens after reconnects (missed strikes).
                        if is_first_tick:
                            is_first_tick = False
                            if time.time() - ts_s > 5.0:
                                continue

                        # --- Delta Price Validation (reject anomalous jumps > $1,500) ---
                        if self._latest is not None:
                            delta = abs(value - self._latest)
                            if delta > 1500.0:
                                print(f"  [price_feed] WARNING: Rejected anomalous price tick jump: "
                                      f"${self._latest:,.2f} -> ${value:,.2f} (delta=${delta:,.2f})")
                                continue

                        if value > 0:
                            self._record(ts_s, value)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._err = str(e)
                # brief backoff then reconnect (0.5s: every second of hole at
                # a slot boundary is a potentially lost strike)
                try:
                    await asyncio.sleep(0.5)
                except Exception:
                    time.sleep(2.0)


class LivePriceFeed:
    """Real BTC/USD spot. Prefers Chainlink Data Streams (Polymarket RTDS).

    Other sources (Arbitrum aggregator, Pyth, exchanges) are fallbacks for
    connectivity only — they are NOT Polymarket's 5m resolution source.
    """

    def __init__(self, asset: str = "btc", sources: list[str] | None = None, timeout: float = 8.0,
                 chainlink_stream: ChainlinkDataStream | None = None):
        import requests  # lazy: offline sim needs no deps
        self.requests = requests
        self.asset = asset.lower()
        self.sources = sources or config.BTC_PRICE_SOURCES
        self.timeout = timeout
        self.history: deque[float] = deque(maxlen=512)
        self.stream = chainlink_stream
        if self.stream is None and any(
                s in ("chainlink_stream", "chainlink_ds", "rtds")
                for s in self.sources):
            self.stream = ChainlinkDataStream(asset=self.asset)
            self.stream.start()

    def start_resolution_stream(self) -> ChainlinkDataStream:
        """Ensure the Polymarket Chainlink Data Stream is running."""
        if self.stream is None:
            self.stream = ChainlinkDataStream(asset=self.asset)
        self.stream.start()
        return self.stream

    def _chainlink_stream(self):
        if self.stream is None:
            raise RuntimeError("Chainlink Data Stream not started")
        if not self.stream.ready:
            raise RuntimeError("Chainlink Data Stream is stale or disconnected")
        return self.stream.price()

    # Alias names that can appear in BTC_PRICE_SOURCES
    _chainlink_ds = _chainlink_stream
    _rtds = _chainlink_stream

    def _chainlink(self):
        # Arbitrum aggregator — NOT Data Streams. Fallback only.
        rpc_feeds = {
            "btc": "0x6ce185860a4963106506C203335A2910413708e9",
            "eth": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
            "sol": "0x32338Cff0B6ED70C15d0389EDCa48A8d53086ebc",
        }
        addr = rpc_feeds.get(self.asset, rpc_feeds["btc"])
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
        # Pyth Price Feed IDs with 0x prefix
        pyth_feed_ids = {
            "btc": "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
            "eth": "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0aec",
            "sol": "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
        }
        feed_id = pyth_feed_ids.get(self.asset, pyth_feed_ids["btc"])
        url = f"https://hermes.pyth.network/v2/updates/price/latest?ids[]={feed_id}"
        r = self.requests.get(url, timeout=self.timeout)
        if r.status_code == 200:
            data = r.json()
            parsed = data["parsed"][0]
            price_data = parsed["price"]
            return int(price_data["price"]) * (10 ** price_data["expo"])
        raise RuntimeError(f"Pyth HTTP status: {r.status_code}")

    def _coinbase(self):
        r = self.requests.get(
            f"https://api.coinbase.com/v2/prices/{self.asset.upper()}-USD/spot", timeout=self.timeout)
        return float(r.json()["data"]["amount"])

    def _binance(self):
        symbol = f"{self.asset.upper()}USDT"
        r = self.requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": symbol}, timeout=self.timeout)
        return float(r.json()["price"])

    def _kraken(self):
        pair = "XBTUSD" if self.asset == "btc" else f"{self.asset.upper()}USD"
        r = self.requests.get(
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": pair}, timeout=self.timeout)
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
            hit = self.stream.price_at_or_before(float(slot_start_ts), max_age_s=60.0)
            if hit is None:
                hit2 = self.stream.price_at(float(slot_start_ts), max_skew_s=20.0)
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
                f"https://api.exchange.coinbase.com/products/{self.asset.upper()}-USD/candles",
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
            symbol = f"{self.asset.upper()}USDT"
            r = self.requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": "5m", "limit": "5"},
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
                f"https://api.exchange.coinbase.com/products/{self.asset.upper()}-USD/candles",
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
            symbol = f"{self.asset.upper()}USDT"
            r = self.requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": "5m", "limit": "5"},
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
