"""Real-time order book over WebSocket, with atomic both-sides snapshots.

WHY THIS REPLACES THE REST POLLING
----------------------------------
The arb scanner used to re-read the book every ~15 seconds over REST, one side
at a time:

    q_up = _quote_side(pub, market, "UP")     # REST round trip
    q_dn = _quote_side(pub, market, "DOWN")   # a second, later round trip

Two separate problems, and the second is worse than the latency:

1. A 15-second scan interval on a market where quotes move in milliseconds
   means nearly every gap is missed, and the ones that are "caught" are stale
   by construction. Polymarket removed the 500ms taker buffer in Feb 2026, so
   stale quotes get picked off by faster bots long before a 15s poll notices.

2. UP and DOWN were read at DIFFERENT MOMENTS. A "pair" priced from two
   snapshots taken seconds apart may never have existed simultaneously — which
   would produce arb signals that were never actually takeable. That is a
   correctness bug, not just a speed one, and it cannot be fixed by polling
   faster.

This module keeps a live book in memory from pushed updates, so `pair()`
returns both sides read from the SAME state at the same instant, with an
explicit age so the caller can refuse anything stale.

It is deliberately a supplement, not a replacement: if the socket is down or
stale, callers fall back to REST. A degraded feed must slow the bot down, not
stop it.

Endpoint (official): wss://ws-subscriptions-clob.polymarket.com/ws/market
The server expects a PING text frame every ~10s and answers PONG.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 10.0
# A book older than this is not worth pricing an arb from.
DEFAULT_MAX_AGE_S = 2.0
# Cap the subscription. Every 5m slot adds two token ids; resolved markets stop
# pushing updates but still bloat each resubscribe frame.
MAX_TRACKED = int(os.getenv("BOOK_FEED_MAX_TRACKED", "8"))


@dataclass
class BookSide:
    """Top of book for one token."""
    best_bid: float | None = None
    best_ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    updated_at: float = 0.0

    def age(self, now: float | None = None) -> float:
        return (now if now is not None else time.time()) - self.updated_at


@dataclass
class PairQuote:
    """Both sides of a market, read from one in-memory state at one instant."""
    ask_up: float
    ask_down: float
    bid_up: float | None
    bid_down: float | None
    size_up: float | None
    size_down: float | None
    age_s: float          # staleness of the OLDER of the two sides
    taken_at: float

    @property
    def pair_cost(self) -> float:
        return self.ask_up + self.ask_down


def _top(levels, want_max: bool):
    """Best price + size from a list of {price,size} levels."""
    best_px = None
    best_sz = None
    for lv in levels or []:
        try:
            px = float(lv.get("price"))
            sz = float(lv.get("size", 0) or 0)
        except (TypeError, ValueError, AttributeError):
            continue
        if px <= 0 or px >= 1:
            continue
        if best_px is None or (px > best_px if want_max else px < best_px):
            best_px, best_sz = px, sz
    return best_px, best_sz


class BookFeed:
    """Subscribes to token ids and keeps their top-of-book current."""

    def __init__(self, max_age_s: float = DEFAULT_MAX_AGE_S):
        self._books: dict[str, BookSide] = {}
        self._lock = threading.Lock()
        self._assets: set[str] = set()
        self._subscribe_order: list[str] = []   # oldest-first, for pruning
        self._stop = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None
        self._connected = False
        self.max_age_s = max_age_s
        self.messages = 0
        self.reconnects = 0
        self.last_error: str = ""

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="book-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        loop, ws = self._loop, self._ws
        if loop and ws:
            try:
                asyncio.run_coroutine_threadsafe(ws.close(), loop)
            except Exception:
                pass

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            self.last_error = str(e)
        finally:
            try:
                self._loop.close()
            except Exception:
                pass

    async def _run(self) -> None:
        try:
            import websockets
        except ImportError:
            self.last_error = "websockets not installed; REST fallback only"
            return
        backoff = 1.0
        while not self._stop:
            try:
                async with websockets.connect(
                        WS_URL, ping_interval=None, close_timeout=5) as ws:
                    self._ws = ws
                    self._connected = True
                    backoff = 1.0
                    await self._subscribe(ws, sorted(self._assets))
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        async for raw in ws:
                            self._handle(raw)
                    finally:
                        pinger.cancel()
            except Exception as e:
                self.last_error = str(e)
            finally:
                self._connected = False
                self._ws = None
            if self._stop:
                break
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _ping(self, ws) -> None:
        # The service wants a literal PING text frame, not a protocol ping.
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                await ws.send("PING")
            except Exception:
                return

    async def _subscribe(self, ws, assets: list[str]) -> None:
        if not assets:
            return
        await ws.send(json.dumps({"assets_ids": assets, "type": "market"}))

    def watch(self, *token_ids: str) -> None:
        """Track these tokens. Safe to call every slot.

        Always re-sends the COMPLETE asset list. The channel REPLACES a
        connection's subscription rather than appending to it, so sending only
        the newly-added ids silently subscribed to nothing: in production the
        feed reported connected with 16 tracked tokens and books for only 4,
        and every arb read quietly fell back to REST. The socket looked healthy
        the whole time, which is exactly why the [ws]/[rest] tag in the log
        matters.
        """
        new = {t for t in token_ids if t}
        with self._lock:
            added = new - self._assets
            self._assets |= new
            # Bound the subscription: each 5m slot adds two tokens forever, and
            # long-dead markets push no updates but do bloat every resubscribe.
            if len(self._assets) > MAX_TRACKED:
                keep = [t for t in self._subscribe_order if t in self._assets]
                keep = keep[-(MAX_TRACKED - len(new)):] + sorted(new)
                self._assets = set(keep)
            for t in sorted(new):
                if t not in self._subscribe_order:
                    self._subscribe_order.append(t)
            full = sorted(self._assets)
        if added and self._connected and self._loop and self._ws:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._subscribe(self._ws, full), self._loop)
            except Exception:
                pass

    # -- message handling --------------------------------------------------
    def _handle(self, raw) -> None:
        if raw == "PONG":
            return
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        events = msg if isinstance(msg, list) else [msg]
        now = time.time()
        for ev in events:
            if not isinstance(ev, dict):
                continue
            self.messages += 1
            token = (ev.get("asset_id") or ev.get("market")
                     or ev.get("token_id") or "")
            if not token:
                continue
            etype = (ev.get("event_type") or ev.get("type") or "").lower()
            if etype in ("book", "snapshot") or "bids" in ev or "asks" in ev:
                bid, bsz = _top(ev.get("bids") or ev.get("buys"), want_max=True)
                ask, asz = _top(ev.get("asks") or ev.get("sells"), want_max=False)
                if bid is None and ask is None:
                    continue
                with self._lock:
                    b = self._books.setdefault(token, BookSide())
                    if bid is not None:
                        b.best_bid, b.bid_size = bid, bsz
                    if ask is not None:
                        b.best_ask, b.ask_size = ask, asz
                    b.updated_at = now
            elif etype in ("price_change", "tick_size_change", "last_trade_price"):
                # Best bid/ask deltas, when the venue sends them directly.
                try:
                    if ev.get("best_bid") is not None or ev.get("best_ask") is not None:
                        with self._lock:
                            b = self._books.setdefault(token, BookSide())
                            if ev.get("best_bid") is not None:
                                b.best_bid = float(ev["best_bid"])
                            if ev.get("best_ask") is not None:
                                b.best_ask = float(ev["best_ask"])
                            b.updated_at = now
                except (TypeError, ValueError):
                    pass

    # -- reads -------------------------------------------------------------
    def side(self, token_id: str) -> BookSide | None:
        with self._lock:
            b = self._books.get(token_id)
            return BookSide(b.best_bid, b.best_ask, b.bid_size, b.ask_size,
                            b.updated_at) if b else None

    def pair(self, up_token: str, down_token: str,
             max_age_s: float | None = None) -> PairQuote | None:
        """Both sides from ONE instant, or None if either is missing/stale.

        This is the whole point of the module. Two REST calls seconds apart can
        describe a pair that never simultaneously existed; this cannot.
        """
        limit = self.max_age_s if max_age_s is None else max_age_s
        now = time.time()
        with self._lock:
            u = self._books.get(up_token)
            d = self._books.get(down_token)
            if not u or not d:
                return None
            if u.best_ask is None or d.best_ask is None:
                return None
            age = max(now - u.updated_at, now - d.updated_at)
            if limit > 0 and age > limit:
                return None
            return PairQuote(
                ask_up=u.best_ask, ask_down=d.best_ask,
                bid_up=u.best_bid, bid_down=d.best_bid,
                size_up=u.ask_size, size_down=d.ask_size,
                age_s=age, taken_at=now,
            )

    def status(self) -> dict:
        with self._lock:
            tracked = len(self._assets)
            books = len(self._books)
        return {
            "connected": self._connected,
            "tracked_tokens": tracked,
            "books": books,
            "messages": self.messages,
            "reconnects": self.reconnects,
            "last_error": self.last_error[:200],
        }
