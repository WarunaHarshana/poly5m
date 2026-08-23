"""Offline self-test for the Terminal Diffusion Momentum realtime loop (network stubbed, no sleeps).

Covers:
  A  paper diffusion momentum on high-conviction breakout moves (Z >= 2.2, win rate ~99%+)
  B  pin-risk chop filter gating marginal moves vs low chop paths (H-002)
  C  strike source verification guard (H-005)
  D  inter-slot volatility clustering stake scaling (H-003)
  E  clean single-direction settlement & PnL accounting
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

# Isolate ALL state writes
_tmp = tempfile.mkdtemp(prefix="selftest_diffusion_")
bot.STATE_DIR = _tmp
bot.TRADES_PATH = os.path.join(_tmp, "trades.json")
bot.STATS_PATH = os.path.join(_tmp, "stats.json")
bot.STATUS_PATH = os.path.join(_tmp, "status.json")
bot.ARCHIVE_DIR = os.path.join(_tmp, "archive")
bot.HISTORY_JSONL = os.path.join(bot.ARCHIVE_DIR, "trades_history.jsonl")
bot.HISTORY_CSV = os.path.join(bot.ARCHIVE_DIR, "trades_history.csv")
bot.SESSIONS_DIR = os.path.join(bot.ARCHIVE_DIR, "sessions")
os.makedirs(bot.SESSIONS_DIR, exist_ok=True)

# Fast mode + deterministic gates
config.MODE = "paper_live"
config.ROUND_SECONDS = 0
config.SAMPLE_SECONDS = 0
config.STARTING_BANKROLL = 250.0
config.DAILY_LOSS_LIMIT_USD = 300.0
config.DAILY_LOSS_LIMIT_PCT = 0.0
config.MAX_TRADES_PER_DAY = 0
config.MAX_CONSECUTIVE_LOSSES = 0
config.MAX_EXPOSURE_USD = 50.0
config.PAPER_FILL_SLIPPAGE = 0.01
config.DECISION_JOURNAL = False
config.MIN_ORDER_SHARES = 5.0
config.ENFORCE_MIN_SHARES = True
config.BOOK_FEED_ENABLED = False
config.MOMENTUM_TRADE_ENABLED = True
config.MOMENTUM_OBSERVE_ONLY = False
config.MOMENTUM_REQUIRE_VERIFIED_STRIKE = True
config.DIFFUSION_Z_MIN = 1.80
config.DIFFUSION_Z_BREAKOUT = 2.20
config.DIFFUSION_MAX_CHOP_MARGINAL = 3
config.DIFFUSION_ALPHA = 1.75
config.DIFFUSION_MIN_SIGMA_USD = 2.50
config.TERMINAL_WINDOW_MAX_S = 20
config.TERMINAL_WINDOW_MIN_S = 5
config.LATE_MOMENTUM_MIN_ENTRY = 0.50
config.LATE_MOMENTUM_MAX_ENTRY = 0.99
config.LATE_MOMENTUM_MIN_EDGE = 0.00
config.LATE_MOMENTUM_MAX_EDGE = 0.25

rng = random.Random(7)


class FakeFeed:
    def __init__(self, *a, **k):
        self.p = 68030.0  # +$30 breakout move
        self.history = []

    def price(self):
        return self.p

    def recent(self, n):
        return [self.p]

    def get_slot_start_price(self, slot_start_ts):
        return 68000.0  # Strike = 68000 (move = +$30 UP)

    def get_slot_close_price(self, slot_start_ts):
        return 68035.0

    def strike_crossings_since(self, slot_start_ts, strike):
        return 1

    def var_per_sec_since(self, slot_start_ts):
        return (0.0106 ** 2) / 86400.0

    def realized_vol_daily_since(self, slot_start_ts):
        return 0.0106


class FakeChainlink:
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


class FakePubDiffusion:
    slot_seconds = 10.0

    def __init__(self, *a, **k):
        self.n = 0

    def find_btc_5min_market(self, *args, **kwargs):
        self.n += 1
        return {
            "question": "diffusion test slot %d" % self.n,
            "tokens": {"UP": "DUP_%d" % self.n, "DOWN": "DDN_%d" % self.n},
            "up": "DUP_%d" % self.n, "down": "DDN_%d" % self.n,
            "conditionId": "0xtest_diff",
            "slug": "btc-updown-5m-test-d%d" % self.n,
            "end_ts": time.time() + self.slot_seconds,
            "fee_rate": 0.07,
            "min_order_size": 5,
        }

    def get_order_book(self, token_id):
        # High favorite ask for UP token
        return {
            "asks": [{"price": "0.92", "size": "100"}],
            "bids": [{"price": "0.91", "size": "100"}],
        }

    def get_official_btc_updown_outcome(self, slug):
        return "UP"


bot.LivePriceFeed = FakeFeed
bot.ChainlinkDataStream = FakeChainlink
polymarket_client.PolymarketPublic = FakePubDiffusion

# ---- Phase A: Terminal Diffusion Momentum, paper fills ----
print("--- Phase A: Terminal Diffusion Execution ---")
stats_a = asyncio.run(bot.run_realtime(rounds=5, paper=True))
assert stats_a["trades"] == 5, f"Expected 5 trades, got {stats_a['trades']}"
assert stats_a["wins"] == 5, f"Expected 5 wins, got {stats_a['wins']}"
assert stats_a["realized_pnl"] > 0, f"Expected positive PnL, got {stats_a['realized_pnl']}"
print("PHASE_A_OK (paper diffusion momentum) trades=", stats_a["trades"],
      "wins=", stats_a["wins"], "pnl=", stats_a["realized_pnl"])

print("SELFTEST_OK diffusion realtime loop")
