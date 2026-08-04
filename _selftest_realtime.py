"""Offline self-test for the realtime loop (network stubbed, no sleeps).

Phase A: late_momentum — steady +$15 gap vs strike, favorite ask 0.70;
         every round must trade UP and win (official outcome stubbed UP).
Phase B: sum-to-one ARB — both asks 0.46 (pair 0.92 < 1); every round must
         place an ARB pair and book positive locked-in PnL.
"""
import asyncio
import math
import os
import random
import tempfile
import time

import config
import bot
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
# The observation log lives in its own module, so redirect it too or the
# selftest's synthetic slots end up in the real learning dataset.
import observations
observations.ARCHIVE_DIR = bot.ARCHIVE_DIR
observations.OBSERVATIONS_JSONL = os.path.join(
    bot.ARCHIVE_DIR, "observations.jsonl")

# Fast mode + deterministic gates (independent of whatever .env says)
config.MODE = "paper_live"
config.ROUND_SECONDS = 0
config.SAMPLE_SECONDS = 0
config.STARTING_BANKROLL = 250.0
config.DAILY_LOSS_LIMIT_USD = 300.0
config.DAILY_LOSS_LIMIT_PCT = 0.0
config.MAX_TRADES_PER_DAY = 0
config.MAX_CONSECUTIVE_LOSSES = 0
config.FIXED_STAKE_USD = 0
config.BASE_TRADE_USD = 2.0
config.MAX_TRADE_USD = 5.0
config.MAX_EXPOSURE_USD = 50.0
config.STRATEGY = "late_momentum"
config.apply_strategy_flags()
config.LATE_MOMENTUM_MIN_REMAINING = 8
config.LATE_MOMENTUM_TIME_LIMIT = 40
config.LATE_MOMENTUM_MIN_MOVE = 10.0
config.LATE_MOMENTUM_MIN_ENTRY = 0.55
config.LATE_MOMENTUM_MAX_ENTRY = 0.92
config.LATE_MOMENTUM_MIN_EDGE = 0.0   # floor off
config.LATE_MOMENTUM_MAX_EDGE = 0.0   # ceiling off (kept deterministic)
config.LATE_SCALP_REQUIRE_CHAINLINK = False  # no real stream in the test
config.LATE_SCALP_REQUOTE = False
config.LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS = 0
config.LATE_SCALP_MAX_SPREAD = 0.0
config.LATE_SCALP_MAX_STAKE_USD = 50.0
config.PAPER_FILL_SLIPPAGE = 0.01
config.DECISION_JOURNAL = False
config.MIN_ORDER_SHARES = 5.0
config.ENFORCE_MIN_SHARES = True
config.get_time_suitability = lambda ts=None: ("OK", "selftest")

rng = random.Random(7)


class FakeFeed:
    """Spot always $15 above the slot strike -> momentum side is UP."""
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


class FakePubMomentum:
    """UP asks 0.70 / DOWN asks 0.30 (no arb: sum = 1.0). Official = UP."""
    def __init__(self, *a, **k):
        self.n = 0
    def find_btc_5min_market(self, *args, **kwargs):
        self.n += 1
        up, down = "MUP_%d" % self.n, "MDN_%d" % self.n
        return {
            "question": "momentum test slot %d" % self.n,
            "tokens": {"UP": up, "DOWN": down},
            "up": up, "down": down,
            "conditionId": "0xtest",
            "slug": "btc-updown-5m-test-m%d" % self.n,
            "end_ts": time.time() + 30.0,  # inside the 8-40s late window
        }
    def midpoint(self, token_id):
        return 0.70 if token_id.startswith("MUP") else 0.30
    def get_official_btc_updown_outcome(self, slug):
        return "UP"


class FakePubArb(FakePubMomentum):
    """Both sides ask 0.44 -> pair 0.88; must clear slippage (0.02/pair) AND
    real taker fees (2 x 0.07 x 0.45 = 0.063/pair): locked = 0.037/pair."""
    def find_btc_5min_market(self, *args, **kwargs):
        m = super().find_btc_5min_market()
        m["tokens"] = {"UP": "AUP_%d" % self.n, "DOWN": "ADN_%d" % self.n}
        m["slug"] = "btc-updown-5m-test-a%d" % self.n
        m["question"] = "arb test slot %d" % self.n
        return m
    def midpoint(self, token_id):
        return 0.44


bot.LivePriceFeed = FakeFeed

# ---- Phase A: late_momentum ----
config.ARB_TRADE_ENABLED = False
polymarket_client.PolymarketPublic = FakePubMomentum
stats_a = asyncio.run(bot.run_realtime(rounds=25, paper=True))
assert stats_a["trades"] == 25, f"momentum: expected 25 trades, got {stats_a['trades']}"
assert stats_a["wins"] == 25, f"momentum: expected all wins, got {stats_a['wins']}"
# pessimistic fill: 0.70 ask + 0.01 slippage
assert abs(stats_a["avg_entry"] - 0.71) < 1e-6, stats_a["avg_entry"]
print("\nPHASE_A_OK (late_momentum) trades=", stats_a["trades"],
      "wr=", stats_a["win_rate"], "avg_entry=", stats_a["avg_entry"])

# ---- Phase B: sum-to-one arb (fee-aware) ----
config.ARB_TRADE_ENABLED = True
config.ARB_MIN_EDGE = 0.02
config.ARB_MIN_LOCKED_EDGE = 0.02
config.ARB_MAX_EDGE = 0.15
config.ARB_MIN_REMAINING = 20
config.ARB_MAX_STAKE_USD = 10.0
config.ARB_SCAN_ENABLED = True
config.TAKER_FEE_RATE = 0.07          # real crypto_fees_v2 rate
config.LATE_MOMENTUM_MIN_MOVE = 1e9   # momentum can never fire
polymarket_client.PolymarketPublic = FakePubArb
stats_b = asyncio.run(bot.run_realtime(rounds=10, paper=True))
assert stats_b["trades"] == 10, f"arb: expected 10 trades, got {stats_b['trades']}"
assert stats_b["wins"] == 10, f"arb: expected all wins, got {stats_b['wins']}"
assert stats_b["realized_pnl"] > 0, stats_b["realized_pnl"]
# pair cost = 2 * (0.44 + 0.01 slip) = 0.90; fees 0.063/pair -> locked 0.037
assert abs(stats_b["avg_entry"] - 0.90) < 1e-6, stats_b["avg_entry"]
print("PHASE_B_OK (arb) trades=", stats_b["trades"],
      "pnl=", stats_b["realized_pnl"], "avg_entry=", stats_b["avg_entry"])

# ---- Phase C: LIVE order path (stubbed CLOB — no real orders) ----
# Exercises the paper=False branch: safety profile, order placement,
# cancel + fill matching, live PnL accounting. This is the code that
# touches real money, so it must be covered by tests.
class FakeClobInner:
    def get_trades(self):
        return [{"taker_order_id": "ord-1", "size": 5.0}]


class FakeLiveClient:
    calls = []
    def __init__(self, *a, **k):
        pass
    def buy(self, token_id, price, size, condition_id):
        FakeLiveClient.calls.append(("buy", token_id, price, size))
        return {"orderID": "ord-1"}
    def cancel_order(self, order_id):
        FakeLiveClient.calls.append(("cancel", order_id))
        return {"canceled": [order_id]}
    def cancel_all(self):
        return {}
    def get_usdc_balance(self, refresh=True):
        return {"ok": True, "balance_usd": 25.0, "funder": "0xtest"}
    def _client(self):
        return FakeClobInner()


polymarket_client.PolymarketClient = FakeLiveClient
polymarket_client.PolymarketPublic = FakePubMomentum
config.MODE = "live"
config.PRIVATE_KEY = "0x" + "1" * 64
config.FUNDER_ADDRESS = "0x" + "2" * 40
config.LIVE_CONFIRM = "yes"
config.LIVE_AGGRESSIVE = False
config.LATE_MOMENTUM_MIN_MOVE = 10.0  # momentum active again
config.ARB_TRADE_ENABLED = False
stats_c = asyncio.run(bot.run_realtime(rounds=3, paper=False))
assert stats_c["trades"] == 3, f"live: expected 3 trades, got {stats_c['trades']}"
assert stats_c["wins"] == 3, f"live: expected 3 wins, got {stats_c['wins']}"
buys = [c for c in FakeLiveClient.calls if c[0] == "buy"]
cancels = [c for c in FakeLiveClient.calls if c[0] == "cancel"]
assert len(buys) == 3, f"live: expected 3 orders, got {len(buys)}"
assert len(cancels) == 3, f"live: expected 3 cancels, got {len(cancels)}"
# Fill check matched 5.0 shares -> stake rebooked at 5 * entry(0.70) = 3.50
assert abs(stats_c["avg_stake"] - 3.50) < 1e-6, stats_c["avg_stake"]
config.MODE = "paper_live"
print("PHASE_C_OK (live order path) trades=", stats_c["trades"],
      "orders=", len(buys), "cancels=", len(cancels),
      "avg_stake=", stats_c["avg_stake"])

# ---- Phase D: maker-first execution ----
# D1: market NEVER trades down to our resting price -> must NOT fill as
#     maker; falls back to crossing at the ask and paying the taker fee.
config.MODE = "paper_live"
config.MAKER_FIRST_ENABLED = True
config.MAKER_IMPROVE_TICKS = 1
config.MAKER_WAIT_SECONDS = 1.0
config.MAKER_MIN_REMAINING = 0.0
config.LATE_MOMENTUM_MIN_MOVE = 10.0
config.LATE_MOMENTUM_MAX_ENTRY = 0.92
config.ARB_TRADE_ENABLED = False
polymarket_client.PolymarketPublic = FakePubMomentum   # ask pinned at 0.70
stats_d1 = asyncio.run(bot.run_realtime(rounds=5, paper=True))
# ask never drops to 0.69 -> taker path: 0.70 + 0.01 slippage
assert abs(stats_d1["avg_entry"] - 0.71) < 1e-6, stats_d1["avg_entry"]
assert stats_d1["fees_paid"] > 0, "taker fallback must pay fees"
print("PHASE_D1_OK (no maker fill when market never comes to us) "
      "avg_entry=", stats_d1["avg_entry"], "fees=", stats_d1["fees_paid"])


class FakePubMakerFill(FakePubMomentum):
    """Per slot: ask reads 0.70 first, then 0.69 -> our resting bid is hit.

    Counted PER TOKEN so every round behaves identically (a run-wide counter
    made only the first round fill).
    """
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.reads = {}
    def midpoint(self, token_id):
        if token_id.startswith("MUP"):
            self.reads[token_id] = self.reads.get(token_id, 0) + 1
            return 0.70 if self.reads[token_id] <= 1 else 0.69
        return 0.30


polymarket_client.PolymarketPublic = FakePubMakerFill
stats_d2 = asyncio.run(bot.run_realtime(rounds=5, paper=True))
# maker fill at 0.70 - 0.01 = 0.69, NO slippage, NO fee
assert abs(stats_d2["avg_entry"] - 0.69) < 1e-6, stats_d2["avg_entry"]
assert stats_d2["fees_paid"] == 0, f"maker fills pay no fee, got {stats_d2['fees_paid']}"
print("PHASE_D2_OK (maker fill when market trades to us) "
      "avg_entry=", stats_d2["avg_entry"], "fees=", stats_d2["fees_paid"])

# ---- Phase E: live pre-order ask rise must KEEP >= 5 shares ----
# Reproduces the first real-money failure: approved 5 x 0.70, ask ticked up
# before the order, old code shrank shares to stake/new_ask < 5 and the
# venue rejected it ("Size (4.93) lower than the minimum: 5").
class FakePubAskRise(FakePubMomentum):
    """Ask 0.70 during analysis; 0.72 on the final pre-order re-check."""
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.reads = {}
    def midpoint(self, token_id):
        if token_id.startswith("MUP"):
            n = self.reads.get(token_id, 0) + 1
            self.reads[token_id] = n
            # Read 1: analysis quote (requote off). Read 2+: pre-order check.
            return 0.70 if n <= 1 else 0.72
        return 0.30


class FakeLiveClientE(FakeLiveClient):
    calls = []
    def buy(self, token_id, price, size, condition_id):
        FakeLiveClientE.calls.append((price, size))
        assert size >= 5.0, f"order below venue minimum: {size}"
        return {"orderID": "ord-E"}
    def cancel_order(self, order_id):
        return {"canceled": [order_id]}
    def _client(self):
        class I:
            def get_trades(self):
                return [{"taker_order_id": "ord-E", "size": 5.0}]
        return I()


polymarket_client.PolymarketClient = FakeLiveClientE
polymarket_client.PolymarketPublic = FakePubAskRise
config.MODE = "live"
config.MAKER_FIRST_ENABLED = False
config.LATE_SCALP_MAX_STAKE_USD = 50.0
stats_e = asyncio.run(bot.run_realtime(rounds=2, paper=False))
config.MODE = "paper_live"
assert stats_e["trades"] == 2, f"expected 2 live trades, got {stats_e['trades']}"
for px, size in FakeLiveClientE.calls:
    assert size >= 5.0, f"venue-min violated: {size}"
# shares kept at 5, price bumped to 0.72 -> stake 3.60
assert abs(stats_e["avg_stake"] - 3.60) < 1e-6, stats_e["avg_stake"]
print("PHASE_E_OK (ask-rise keeps venue min) orders=",
      [(p, s) for p, s in FakeLiveClientE.calls], "avg_stake=", stats_e["avg_stake"])

print("\nSELFTEST_OK momentum+arb+live+maker+venue-min")
