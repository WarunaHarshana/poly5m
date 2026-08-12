"""Offline self-test for the WebSocket book feed (no network).

The feed exists to fix two things, and both are asserted here:

  * SPEED    a 15s REST poll misses gaps that live for milliseconds
  * ATOMICITY reading UP then DOWN in separate round trips can price a pair
              that never simultaneously existed. That is a correctness bug and
              polling faster does not fix it.

  T1  parsing        book snapshots produce correct top-of-book
  T2  atomic pair    both sides come from one state, with an age
  T3  staleness      a stale book is refused, not silently used
  T4  partial book   one side missing means no pair, never a half-quote
  T5  price_change   incremental best bid/ask updates are applied
  T6  scanner        the arb scanner prefers WS and falls back to REST
"""
import json
import os
import tempfile
import time

os.environ["STATE_ROOT"] = tempfile.mkdtemp(prefix="selftest_book_")

import book_feed  # noqa: E402
from book_feed import BookFeed  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAIL.append(name)


UP, DN = "TOK_UP", "TOK_DN"


def snapshot(token, bid, ask, bid_sz=100.0, ask_sz=100.0):
    return json.dumps({
        "event_type": "book", "asset_id": token,
        "bids": [{"price": str(bid), "size": str(bid_sz)}],
        "asks": [{"price": str(ask), "size": str(ask_sz)}],
    })


# ---------------------------------------------------------------------------
print("T1 book parsing")
f = BookFeed()
f._handle(snapshot(UP, 0.42, 0.44, 60, 80))
s = f.side(UP)
check("best bid parsed", s and s.best_bid == 0.42, str(s))
check("best ask parsed", s and s.best_ask == 0.44, str(s))
check("ask size parsed", s and s.ask_size == 80.0, str(s))

# Multiple levels: best ask is the LOWEST, best bid the HIGHEST.
f._handle(json.dumps({
    "event_type": "book", "asset_id": UP,
    "bids": [{"price": "0.40", "size": "5"}, {"price": "0.43", "size": "9"}],
    "asks": [{"price": "0.47", "size": "5"}, {"price": "0.45", "size": "7"}],
}))
s = f.side(UP)
check("best bid is the highest", s.best_bid == 0.43, str(s))
check("best ask is the lowest", s.best_ask == 0.45, str(s))

# Degenerate prices must never enter the book.
f2 = BookFeed()
f2._handle(json.dumps({"event_type": "book", "asset_id": UP,
                       "bids": [{"price": "0", "size": "5"}],
                       "asks": [{"price": "1", "size": "5"}]}))
check("0/1 quotes are rejected as degenerate", f2.side(UP) is None,
      str(f2.side(UP)))

# ---------------------------------------------------------------------------
print("T2 atomic pair")
f = BookFeed()
f._handle(snapshot(UP, 0.42, 0.44, 60, 80))
f._handle(snapshot(DN, 0.50, 0.52, 40, 30))
p = f.pair(UP, DN)
check("a pair is returned", p is not None)
check("both asks come from the stored state",
      p.ask_up == 0.44 and p.ask_down == 0.52, str(p))
check("pair cost is the sum of both asks",
      abs(p.pair_cost - 0.96) < 1e-9, str(p.pair_cost))
check("sizes are carried for depth capping",
      p.size_up == 80.0 and p.size_down == 30.0, str(p))
check("the snapshot reports its own age", p.age_s >= 0, str(p.age_s))
check("a fresh pair is near-zero age", p.age_s < 1.0, str(p.age_s))

# ---------------------------------------------------------------------------
print("T3 staleness is refused")
f = BookFeed(max_age_s=2.0)
f._handle(snapshot(UP, 0.42, 0.44))
f._handle(snapshot(DN, 0.50, 0.52))
# Age the UP side only: the pair is only as fresh as its OLDER side.
with f._lock:
    f._books[UP].updated_at = time.time() - 10.0
check("a stale side makes the whole pair unusable", f.pair(UP, DN) is None)
check("age is measured on the older side",
      f.pair(UP, DN, max_age_s=60.0).age_s > 9.0,
      str(f.pair(UP, DN, max_age_s=60.0)))
check("max_age_s=0 disables the staleness gate",
      f.pair(UP, DN, max_age_s=0) is not None)

# ---------------------------------------------------------------------------
print("T4 partial book")
f = BookFeed()
f._handle(snapshot(UP, 0.42, 0.44))
check("one side alone yields no pair", f.pair(UP, DN) is None)
# A book with bids but no asks cannot be bought from.
f._handle(json.dumps({"event_type": "book", "asset_id": DN,
                      "bids": [{"price": "0.50", "size": "10"}], "asks": []}))
check("a side with no ask yields no pair", f.pair(UP, DN) is None)
f._handle(snapshot(DN, 0.50, 0.52))
check("the pair appears once both sides have asks", f.pair(UP, DN) is not None)

# ---------------------------------------------------------------------------
print("T5 incremental updates")
# The REAL price_change shape, captured from the live socket: no top-level
# asset_id, best bid/ask nested inside a price_changes array, and `market`
# holding the condition id shared by BOTH outcomes. 98% of the feed looks like
# this, and an earlier version dropped all of it.
f = BookFeed()
f._handle(snapshot(UP, 0.42, 0.44))
f._handle(snapshot(DN, 0.50, 0.52))
f._handle(json.dumps({
    "event_type": "price_change",
    "market": "0x220d91bfd45f9c67448d7ebf1ee05889af83fdabec83ec99bca861a9c",
    "timestamp": "1786534500000",
    "price_changes": [
        {"asset_id": UP, "price": "0.01", "size": "3954.56", "side": "BUY",
         "best_bid": "0.45", "best_ask": "0.46"},
        {"asset_id": DN, "price": "0.02", "size": "100.0", "side": "SELL",
         "best_bid": "0.53", "best_ask": "0.54"},
    ],
}))
s = f.side(UP)
check("price_change updates the UP side from the nested array",
      s.best_bid == 0.45 and s.best_ask == 0.46, str(s))
s2 = f.side(DN)
check("...and the DOWN side in the same message",
      s2.best_bid == 0.53 and s2.best_ask == 0.54, str(s2))
check("the condition id is NOT stored as a token",
      "0x220d91bfd45f9c67448d7ebf1ee05889af83fdabec83ec99bca861a9c" not in f._books,
      str(list(f._books)))
check("a price_change refreshes the pair's freshness",
      f.pair(UP, DN) is not None and f.pair(UP, DN).ask_up == 0.46,
      str(f.pair(UP, DN)))

# last_trade_price carries a top-level asset_id and no book — must not wipe one.
f._handle(json.dumps({"event_type": "last_trade_price", "asset_id": UP,
                      "price": "0.46", "size": "10"}))
check("last_trade_price leaves the book intact",
      f.side(UP).best_ask == 0.46, str(f.side(UP)))
check("PONG frames are ignored", f._handle("PONG") is None)
check("malformed frames do not raise", f._handle("{not json") is None)

# ---------------------------------------------------------------------------
print("T6 scanner prefers WS, falls back to REST")
import config  # noqa: E402

config.ARB_SCAN_ENABLED = True
config.ARB_MIN_EDGE = 0.02
import bot  # noqa: E402

MARKET = {"tokens": {"UP": UP, "DOWN": DN}, "conditionId": "0xc"}


class RestPub:
    """REST source, deliberately quoting a DIFFERENT pair than the socket."""
    def __init__(self):
        self.calls = 0

    def get_order_book(self, token_id):
        self.calls += 1
        return {}


def _rest_quote(pub, market, side):
    pub.calls += 1
    return {"price": 0.49, "ask_size": 25.0}


bot._quote_side = _rest_quote

feed = BookFeed()
# 0.49 + 0.50 = 0.99 -> a 1c gap, under the 2c raw-edge floor.
feed._handle(snapshot(UP, 0.47, 0.49, 60, 80))
feed._handle(snapshot(DN, 0.48, 0.50, 40, 30))

pub = RestPub()
res = bot._scan_sum_to_one_arb(pub, MARKET, feed)
check("a 1c gap is below the raw-edge floor", res is None, str(res))
check("no REST call was made when the socket had the book", pub.calls == 0,
      f"{pub.calls} REST calls")

# A genuine gap on the socket: 0.44 + 0.50 = 0.94 -> 6c.
feed._handle(snapshot(UP, 0.42, 0.44, 60, 80))
feed._handle(snapshot(DN, 0.48, 0.50, 40, 30))
res = bot._scan_sum_to_one_arb(pub, MARKET, feed)
check("a real gap is detected from the socket", res is not None, str(res))
if res:
    check("it is priced from the socket, not REST",
          res["src"].startswith("ws"), res["src"])
    check("the pair matches the socket book",
          abs(res["sum"] - 0.94) < 1e-9, str(res["sum"]))
    check("depth comes from the socket", res["up_size"] == 80.0, str(res))

# Stale socket -> REST fallback, rather than no scan at all.
with feed._lock:
    feed._books[UP].updated_at = time.time() - 30.0
pub2 = RestPub()
res = bot._scan_sum_to_one_arb(pub2, MARKET, feed)
check("a stale socket falls back to REST", pub2.calls == 2,
      f"{pub2.calls} REST calls")
check("the REST fallback still produces a quote", res is not None, str(res))

# No feed at all must behave exactly as before.
pub3 = RestPub()
res = bot._scan_sum_to_one_arb(pub3, MARKET, None)
check("no feed -> pure REST path still works", pub3.calls == 2 and res is not None,
      f"{pub3.calls} calls, {res}")

# ---------------------------------------------------------------------------
print("T7 subscribing after connect sends the FULL asset list")
# The channel REPLACES a connection's subscription rather than appending to it.
# Sending only the newly-added ids therefore subscribes to nothing: production
# showed connected=true, 16 tracked tokens, books for only 4, and every arb read
# silently falling back to REST while the feed looked perfectly healthy.
sent = []


class _FakeWS:
    async def send(self, frame):
        sent.append(json.loads(frame))


f = BookFeed()
f._connected = True
f._ws = _FakeWS()


class _FakeLoop:
    """Runs the coroutine inline so the test does not need a real loop."""
    @staticmethod
    def _drive(coro):
        try:
            coro.send(None)
        except StopIteration:
            pass


f._loop = _FakeLoop()
import asyncio as _aio

_real_rcts = _aio.run_coroutine_threadsafe
_aio.run_coroutine_threadsafe = lambda coro, loop: _FakeLoop._drive(coro)
try:
    f.watch("A1", "A2")
    f.watch("B1", "B2")
finally:
    _aio.run_coroutine_threadsafe = _real_rcts

check("a subscribe frame was sent for each watch call", len(sent) == 2, str(sent))
if len(sent) == 2:
    check("the FIRST frame carries slot A",
          set(sent[0]["assets_ids"]) == {"A1", "A2"}, str(sent[0]))
    check("the SECOND frame re-sends A *and* B, not just the delta",
          set(sent[1]["assets_ids"]) == {"A1", "A2", "B1", "B2"},
          f"got {sent[1]['assets_ids']} — a delta-only frame unsubscribes the rest")
    check("frames are typed as market subscriptions",
          all(s.get("type") == "market" for s in sent), str(sent))

# The subscription must stay bounded: each slot adds two ids forever.
f2 = BookFeed()
for i in range(12):
    f2.watch(f"s{i}a", f"s{i}b")
check("the tracked set is capped", len(f2._assets) <= book_feed.MAX_TRACKED,
      f"{len(f2._assets)} tracked")
check("the newest slot is never pruned",
      {"s11a", "s11b"} <= f2._assets, str(sorted(f2._assets)))

# ---------------------------------------------------------------------------
print("T8 a silent socket is not 'healthy'")
# A half-open connection keeps TCP up while no data flows. `async for` never
# raises, so without a watchdog the feed reports connected=true forever with a
# frozen message count while every read falls back to REST. Seen in production
# after ~100 minutes, stuck at 86813 messages.
f = BookFeed()
f._connected = True
f._last_msg_at = time.time()
st = f.status()
check("a freshly-fed socket is live", st["live"] is True, str(st))
check("quiet time is reported", st["quiet_s"] < 1.0, str(st))

f._last_msg_at = time.time() - (book_feed.STALE_RECONNECT_S + 5)
st = f.status()
check("a silent socket is NOT live even though connected",
      st["connected"] is True and st["live"] is False, str(st))
check("the silence duration is visible",
      st["quiet_s"] > book_feed.STALE_RECONNECT_S, str(st))
check("the reconnect threshold is well under a slot",
      book_feed.STALE_RECONNECT_S < 300, str(book_feed.STALE_RECONNECT_S))

# The watchdog itself: it must close a silent socket, and leave a busy one be.
import asyncio as _a2


class _WS:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


def _run_watchdog(quiet_for):
    fd = BookFeed()
    fd._last_msg_at = time.time() - quiet_for
    ws = _WS()
    orig_check, orig_recon = book_feed.STALE_CHECK_S, book_feed.STALE_RECONNECT_S
    book_feed.STALE_CHECK_S = 0.01
    book_feed.STALE_RECONNECT_S = 5.0
    try:
        _a2.run(_a2.wait_for(fd._watchdog(ws), timeout=1.0))
    except (_a2.TimeoutError, Exception):
        pass
    finally:
        book_feed.STALE_CHECK_S, book_feed.STALE_RECONNECT_S = orig_check, orig_recon
    return ws.closed, fd


closed_silent, fd_s = _run_watchdog(quiet_for=60)
check("the watchdog closes a silent socket", closed_silent)
check("it says why", "no data" in fd_s.last_error, fd_s.last_error)

closed_busy, _ = _run_watchdog(quiet_for=0)
check("it leaves a busy socket alone", not closed_busy)

# The resubscriber: a single lost subscribe must not be permanent.
print("T9 the subscription self-heals")


class _SendWS:
    def __init__(self):
        self.frames = []

    async def send(self, frame):
        self.frames.append(json.loads(frame))


def _run_resub(assets, ticks=2):
    fd = BookFeed()
    fd._assets = set(assets)
    ws = _SendWS()
    orig = book_feed.RESUBSCRIBE_S
    book_feed.RESUBSCRIBE_S = 0.01
    try:
        _a2.run(_a2.wait_for(fd._resubscriber(ws), timeout=0.2))
    except Exception:
        pass
    finally:
        book_feed.RESUBSCRIBE_S = orig
    return ws.frames


frames = _run_resub(["X1", "X2"])
check("it keeps re-sending the subscription", len(frames) >= 2, str(len(frames)))
if frames:
    check("each refresh carries the full asset list",
          all(set(f["assets_ids"]) == {"X1", "X2"} for f in frames), str(frames[0]))
    check("refreshes are typed as market subscriptions",
          all(f.get("type") == "market" for f in frames), str(frames[0]))
check("the refresh beat is faster than the staleness gate",
      book_feed.RESUBSCRIBE_S < book_feed.STALE_RECONNECT_S,
      f"{book_feed.RESUBSCRIBE_S} vs {book_feed.STALE_RECONNECT_S}")

# ---------------------------------------------------------------------------
print("T10 our own PONG must not count as liveness")
# The keepalive answers PONG every 10s. Counting that as traffic kept the
# watchdog permanently satisfied on a socket that had stopped delivering books:
# production sat at a frozen message count for minutes while live stayed True.
fp = BookFeed()
fp._last_msg_at = time.time() - 300
before = fp._last_msg_at
fp._handle("PONG")
check("a PONG does not refresh liveness", fp._last_msg_at == before,
      f"moved by {fp._last_msg_at - before:.1f}s")
check("a PONG is not counted as a message", fp.messages == 0, str(fp.messages))

fp._handle(snapshot(UP, 0.42, 0.44))
check("real data DOES refresh liveness", fp._last_msg_at > before)
check("real data is counted", fp.messages >= 1, str(fp.messages))

fp._connected = True
fp._last_msg_at = time.time() - (book_feed.STALE_RECONNECT_S + 5)
fp._handle("PONG")
check("a socket answering only PONG still reads as NOT live",
      fp.status()["live"] is False, str(fp.status()))

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED book_feed: {FAIL}")
print("SELFTEST_OK book_feed")
