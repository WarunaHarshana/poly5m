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

print("\nSELFTEST_OK momentum+arb")
