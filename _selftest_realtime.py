"""Offline self-test for the arb realtime loop (network stubbed, no sleeps).

The bot trades exactly one strategy: sum-to-one arbitrage. These phases cover
the loop that finds and books it.

  A  paper arb    both asks 0.44 -> pair 0.88; every round places a pair and
                  books positive locked-in PnL after slippage AND real fees
  B  live arb     both legs confirmed by the venue must be booked as FILLED,
                  with a real stake and both leg order ids on the trail
  C  cadence      the scanner re-polls WITHIN a slot rather than looking once,
                  and paces itself instead of spinning
  D  floor        scanning stops at ARB_MIN_REMAINING; the tail of the slot is
                  waited out, not burned
"""
import asyncio
import json as _json
import math
import os
import random
import tempfile
import time

import config
import bot
import order_state
import polymarket_client

# Isolate ALL state writes — selftest trades must never pollute the real
# dashboard files or the permanent archive in state/.
_tmp = tempfile.mkdtemp(prefix="selftest_state_")
bot.STATE_DIR = _tmp
bot.TRADES_PATH = os.path.join(_tmp, "trades.json")
bot.STATS_PATH = os.path.join(_tmp, "stats.json")
bot.STATUS_PATH = os.path.join(_tmp, "status.json")
bot.ARCHIVE_DIR = os.path.join(_tmp, "archive")
bot.HISTORY_JSONL = os.path.join(bot.ARCHIVE_DIR, "trades_history.jsonl")
bot.HISTORY_CSV = os.path.join(bot.ARCHIVE_DIR, "trades_history.csv")
bot.SESSIONS_DIR = os.path.join(bot.ARCHIVE_DIR, "sessions")
os.makedirs(bot.SESSIONS_DIR, exist_ok=True)

# Fast mode + deterministic gates (independent of whatever .env says)
config.MODE = "paper_live"
config.ROUND_SECONDS = 0
config.SAMPLE_SECONDS = 0
config.STARTING_BANKROLL = 250.0
config.DAILY_LOSS_LIMIT_USD = 300.0
config.DAILY_LOSS_LIMIT_PCT = 0.0
config.MAX_TRADES_PER_DAY = 0
config.MAX_CONSECUTIVE_LOSSES = 0
config.MAX_EXPOSURE_USD = 50.0
config.ARB_MAX_EXPOSURE_USD = 50.0
config.PAPER_FILL_SLIPPAGE = 0.01
config.DECISION_JOURNAL = False
config.MIN_ORDER_SHARES = 5.0
config.ENFORCE_MIN_SHARES = True
config.BOOK_FEED_ENABLED = False        # no sockets in an offline test
config.ARB_SCAN_ENABLED = True
config.ARB_TRADE_ENABLED = True
config.ARB_MIN_EDGE = 0.02
config.ARB_MIN_LOCKED_EDGE = 0.02
config.ARB_MAX_EDGE = 0.15
config.ARB_MIN_REMAINING = 20
config.ARB_MAX_STAKE_USD = 10.0
config.ARB_SCAN_INTERVAL_S = 1.0
config.TAKER_FEE_RATE = 0.07            # real crypto_fees_v2 rate

rng = random.Random(7)


class FakeFeed:
    """Spot price source. Arb never reads it for a decision, only for display
    and the settle-time close price."""
    def __init__(self, *a, **k):
        self.p = 68000.0
        self.history = []

    def price(self):
        self.p *= math.exp(rng.gauss(0.0, 0.0002))
        self.history.append(self.p)
        return self.p

    def recent(self, n):
        return self.history[-n:]

    def get_slot_start_price(self, slot_start_ts):
        return self.p - 15.0

    def get_slot_close_price(self, slot_start_ts):
        return self.p


class FakeChainlink:
    """Stands in for the RTDS stream so the suite stays offline and instant.

    The real one costs a 20s wait_ready per run; arb only needs it for the slot
    clock, so a local clock is a faithful stand-in.
    """
    def __init__(self, *a, **k):
        self.ready = True

    def start(self):
        pass

    async def wait_ready(self, timeout=0.0):
        return True

    def market_now(self):
        return time.time()

    def clock_offset_s(self):
        return 0.0

    def ticks_since(self, *a, **k):
        return []

    def stop(self):
        pass


class FakePubArb:
    """Both sides ask 0.44 -> pair 0.88; must clear slippage (0.02/pair) AND
    real taker fees (2 x 0.07 x 0.44 x 0.56 = 0.0345/pair)."""
    slot_seconds = 30.0

    def __init__(self, *a, **k):
        self.n = 0

    def find_btc_5min_market(self, *args, **kwargs):
        self.n += 1
        return {
            "question": "arb test slot %d" % self.n,
            "tokens": {"UP": "AUP_%d" % self.n, "DOWN": "ADN_%d" % self.n},
            "up": "AUP_%d" % self.n, "down": "ADN_%d" % self.n,
            "conditionId": "0xtest",
            "slug": "btc-updown-5m-test-a%d" % self.n,
            "end_ts": time.time() + self.slot_seconds,
        }

    def midpoint(self, token_id):
        return 0.44

    def get_official_btc_updown_outcome(self, slug):
        return "UP"


bot.LivePriceFeed = FakeFeed
bot.ChainlinkDataStream = FakeChainlink
polymarket_client.PolymarketPublic = FakePubArb

# ---- Phase A: sum-to-one arb, paper fills ----
stats_a = asyncio.run(bot.run_realtime(rounds=10, paper=True))
assert stats_a["trades"] == 10, f"arb: expected 10 trades, got {stats_a['trades']}"
assert stats_a["wins"] == 10, f"arb: expected all wins, got {stats_a['wins']}"
assert stats_a["realized_pnl"] > 0, stats_a["realized_pnl"]
# pair cost = 2 * (0.44 + 0.01 slip) = 0.90
assert abs(stats_a["avg_entry"] - 0.90) < 1e-6, stats_a["avg_entry"]
print("PHASE_A_OK (paper arb) trades=", stats_a["trades"],
      "pnl=", stats_a["realized_pnl"], "avg_entry=", stats_a["avg_entry"])

# ---- Phase B: a LIVE arb whose BOTH legs fill must be booked as FILLED ----
# Regression test. An arb has no single order id — it has one per leg — so
# _try_arb passed order_id=None to the settler. The live settle path read a
# falsy order id as "nothing to verify", called cancel_all() and zeroed the
# position: a pair the venue confirmed on BOTH legs was booked as UNFILLED with
# pnl = -fee, while the wallet held the real thing.
class FakeArbClobInner:
    def get_trades(self):
        return []


class FakeArbLiveClient:
    calls = []

    def __init__(self, *a, **k):
        pass

    def buy(self, token_id, price, size, condition_id, order_type="GTC"):
        FakeArbLiveClient.calls.append(("buy", token_id, price, size, order_type))
        return {"status": "matched", "orderID": "arb-%s" % token_id}

    def sell(self, token_id, price, size, condition_id, order_type="FAK"):
        FakeArbLiveClient.calls.append(("sell", token_id, price, size, order_type))
        return {"status": "matched", "orderID": "unwind-%s" % token_id}

    def cancel_order(self, order_id):
        FakeArbLiveClient.calls.append(("cancel", order_id))
        return {}

    def cancel_all(self):
        FakeArbLiveClient.calls.append(("cancel_all", None))
        return {}

    def get_usdc_balance(self, refresh=True):
        return {"ok": True, "balance_usd": 25.0, "funder": "0xtest"}

    def _client(self):
        return FakeArbClobInner()


polymarket_client.PolymarketClient = FakeArbLiveClient
config.MODE = "live"
config.PRIVATE_KEY = "0x" + "1" * 64
config.FUNDER_ADDRESS = "0x" + "2" * 40
config.LIVE_CONFIRM = "yes"
config.LIVE_AGGRESSIVE = False
config.ARB_LIVE_ENABLED = True
config.LIVE_BLACKOUT_WINDOWS_UTC = ""
FakeArbLiveClient.calls = []

# Pin the egress lookup so the suite stays offline, and point it at a region
# Polymarket blocks: the bot must say so at startup rather than discovering it
# one refused leg at a time. Eleven live attempts were recorded as "first leg
# killed, no risk taken" — true, and completely misleading, because it reads as
# a verdict on the edge when no order ever reached the book.
bot.egress_location = lambda *a, **k: {"ip": "1.2.3.4", "loc": "US", "colo": "SJC"}

stats_b = asyncio.run(bot.run_realtime(rounds=2, paper=False))
config.MODE = "paper_live"

with open(bot.STATUS_PATH, encoding="utf-8") as _f:
    _st = _json.load(_f)
assert _st.get("egress_loc") == "US", \
    f"the egress region must be recorded, got {_st.get('egress_loc')!r}"
assert _st.get("egress_blocked") is True, \
    "a region Polymarket blocks must be flagged before any order is sent"

with open(bot.TRADES_PATH, encoding="utf-8") as _f:
    live_arbs = [t for t in _json.load(_f)
                 if t.get("side") == "ARB" and t.get("mode") != "paper_live"]
assert stats_b["trades"] >= 1, f"a live arb with both legs filling must trade: {stats_b}"
assert live_arbs, "no live ARB row was written at all"

# The whole bug in one assertion: the position must survive settlement.
zeroed = [t for t in live_arbs if not t.get("shares")]
assert not zeroed, (
    "a venue-confirmed ARB pair was booked with zero shares: %r"
    % [{k: t.get(k) for k in ('shares', 'stake_usd', 'pnl', 'fill_unverified')}
       for t in zeroed[:3]])
assert all(t.get("stake_usd", 0) > 0 for t in live_arbs), \
    f"live arb stake was zeroed: {[t.get('stake_usd') for t in live_arbs]}"
assert all(not t.get("fill_unverified") for t in live_arbs), \
    "both legs were confirmed by the venue; nothing here is unverified"
assert all(t.get("pnl", 0) > 0 for t in live_arbs), \
    f"a locked arb pays either way: {[t.get('pnl') for t in live_arbs]}"
assert stats_b["skips"].get("unfilled", 0) == 0, \
    f"a filled pair must not count as unfilled: {stats_b['skips']}"

# Two FOK buys per pair, and nothing sold — there was no stranded leg.
buys = [c for c in FakeArbLiveClient.calls if c[0] == "buy"]
sells = [c for c in FakeArbLiveClient.calls if c[0] == "sell"]
assert len(buys) == 2 * len(live_arbs), f"expected 2 legs per pair, got {buys}"
assert all(b[4] == "FOK" for b in buys), f"arb legs must be FOK: {buys}"
assert not sells, f"nothing was stranded, so nothing should be sold: {sells}"

# ...and the order trail must name the orders the venue actually matched,
# rather than the empty string a two-leg pair used to leave behind.
_trail = order_state.trail(live_arbs[-1]["round_id"])
_filled = [e for e in _trail if e.get("state") == order_state.FILLED]
assert _filled, f"a filled arb pair should record a FILLED event: {_trail}"
assert "," in (_filled[-1].get("order_id") or ""), (
    "the trail should carry BOTH leg order ids, got %r"
    % _filled[-1].get("order_id"))
print("PHASE_B_OK (locked live arb booked as filled) pairs=", len(live_arbs),
      "legs=", len(buys), "pnl=", round(sum(t["pnl"] for t in live_arbs), 4),
      "order_ids=", _filled[-1].get("order_id"))

# ...and a permitted region must NOT be flagged. Runs last, because it places
# more orders and would otherwise skew the leg counts asserted above.
bot.egress_location = lambda *a, **k: {"ip": "5.6.7.8", "loc": "NL", "colo": "AMS"}
config.MODE = "live"
asyncio.run(bot.run_realtime(rounds=1, paper=False))
config.MODE = "paper_live"
with open(bot.STATUS_PATH, encoding="utf-8") as _f:
    _st2 = _json.load(_f)
assert _st2.get("egress_loc") == "NL" and _st2.get("egress_blocked") is False, \
    f"a permitted region must not be flagged: {_st2.get('egress_loc')!r} " \
    f"blocked={_st2.get('egress_blocked')!r}"
print("PHASE_B_GEO_OK (blocked region named at startup, not discovered later)")

# ---- Phase C: the scanner re-polls WITHIN one slot, and paces itself ----
# Regression test for the loop restructure. The arb re-scan cadence used to
# live inside momentum's "slot too early" branch; deleting that block without
# replacing the scheduler would have silently reduced arb to ONE look per slot
# — a change that breaks nothing visibly and quietly stops finding trades.
scan_calls = {"n": 0}
_real_scan = bot._scan_sum_to_one_arb


def _counting_scan(pub, market, feed=None):
    """No signal for the first few looks, then a viable pair."""
    scan_calls["n"] += 1
    if scan_calls["n"] < 5:
        return None
    return {"ask_up": 0.44, "ask_down": 0.44, "sum": 0.88, "edge": 0.12,
            "up_size": 500.0, "down_size": 500.0,
            "bid_up": 0.43, "bid_down": 0.43, "src": "test"}


bot._scan_sum_to_one_arb = _counting_scan
config.ROUND_SECONDS = 1          # not fast-mode: the loop must actually pace
config.ARB_SCAN_INTERVAL_S = 0.05  # floored to 0.2s by the loop
config.ARB_MIN_REMAINING = 1
FakePubArb.slot_seconds = 4.0

_t0 = time.time()
stats_c = asyncio.run(bot.run_realtime(rounds=1, paper=True))
_elapsed = time.time() - _t0

assert scan_calls["n"] >= 5, (
    "the scanner looked %d time(s) in one slot — it must re-poll, not look "
    "once and give up" % scan_calls["n"])
assert stats_c["trades"] == 1, f"expected the 5th look to trade: {stats_c}"
# Pacing: at a 0.2s floor the count cannot meaningfully exceed elapsed/0.2.
# A loop that forgot to sleep would show thousands of calls in the same time.
_ceiling = (_elapsed / 0.2) + 10
assert scan_calls["n"] <= _ceiling, (
    "scanner spun without pacing: %d calls in %.2fs (ceiling %.0f)"
    % (scan_calls["n"], _elapsed, _ceiling))
print("PHASE_C_OK (re-polls within a slot, paced) scans=", scan_calls["n"],
      "elapsed=", round(_elapsed, 2), "s trades=", stats_c["trades"])

# ---- Phase D: scanning stops at ARB_MIN_REMAINING ----
# The tail of the slot is too short to fill both legs, so it must be waited
# out rather than scanned (or, worse, spun on).
scan_calls["n"] = 0
seen_remaining = []


def _recording_scan(pub, market, feed=None):
    scan_calls["n"] += 1
    seen_remaining.append(market.get("end_ts", 0) - time.time())
    if scan_calls["n"] < 3:
        return None
    return {"ask_up": 0.44, "ask_down": 0.44, "sum": 0.88, "edge": 0.12,
            "up_size": 500.0, "down_size": 500.0,
            "bid_up": 0.43, "bid_down": 0.43, "src": "test"}


bot._scan_sum_to_one_arb = _recording_scan
config.ARB_MIN_REMAINING = 2
FakePubArb.slot_seconds = 3.0
asyncio.run(bot.run_realtime(rounds=1, paper=True))
assert seen_remaining, "the scanner never ran"
assert min(seen_remaining) >= 2 - 0.3, (
    "scanned with %.2fs left, under the %ds floor — the tail of a slot is not "
    "fillable and must be waited out" % (min(seen_remaining), 2))
print("PHASE_D_OK (stops at the floor) scans=", scan_calls["n"],
      "min_remaining_seen=", round(min(seen_remaining), 2))

# ---- Phase E: the scan loop must keep status.json fresh ----
# Regression. _heartbeat_sleep only refreshes status when the sleep is LONGER
# than its 5s chunk, so the 1s scan cadence never touched it: production ran
# 119 healthy scans while the dashboard showed STALE and its clock offset —
# derived from status.updated_at — drifted by the whole slot.
scan_calls["n"] = 0
writes = []
_real_write = bot.write_status


def _counting_write(*a, **k):
    writes.append((time.time(), k.get("phase") or k.get("message") or ""))
    return _real_write(*a, **k)


def _slow_scan(pub, market, feed=None):
    # Scans for a good while, then signals — the run has to END, and the only
    # thing that ends it is reaching `rounds`. A stub that never signals just
    # spins through slots until max_slots.
    scan_calls["n"] += 1
    if scan_calls["n"] < 55:
        return None
    return {"ask_up": 0.44, "ask_down": 0.44, "sum": 0.88, "edge": 0.12,
            "up_size": 500.0, "down_size": 500.0,
            "bid_up": 0.43, "bid_down": 0.43, "src": "test"}


bot.write_status = _counting_write
bot._scan_sum_to_one_arb = _slow_scan
config.ARB_MIN_REMAINING = 1
config.ARB_SCAN_INTERVAL_S = 0.05     # floored to 0.2s
FakePubArb.slot_seconds = 12.0
_t0 = time.time()
asyncio.run(bot.run_realtime(rounds=1, paper=True))
_elapsed = time.time() - _t0
bot.write_status = _real_write

_beats = [w for w in writes if w[0] >= _t0]
_gaps = [(b[0] - a[0], a[1], b[1]) for a, b in zip(_beats, _beats[1:])]
_gaps.sort(reverse=True)
_worst = _gaps[0][0] if _gaps else _elapsed
assert scan_calls["n"] > 10, \
    f"expected a long scan window to exercise this, got {scan_calls['n']} scans"
assert _worst <= 8.0, (
    "status.json went %.1fs without a write — the dashboard reads that as "
    "STALE during entirely normal operation.\nWorst gaps (seconds, "
    "phase-before -> phase-after):\n  %s" % (
        _worst, "\n  ".join(f"{g:.1f}s  {a!r} -> {b!r}" for g, a, b in _gaps[:5])))
print("PHASE_E_OK (status stays fresh while scanning) scans=", scan_calls["n"],
      "writes=", len(_beats), "worst gap=", round(_worst, 2), "s")

bot._scan_sum_to_one_arb = _real_scan
config.ROUND_SECONDS = 0

print("\nSELFTEST_OK arb realtime loop")
