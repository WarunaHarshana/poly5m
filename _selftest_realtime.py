"""Offline self-test for the real-data (paper_live) code path.

Stubs the network (LivePriceFeed + PolymarketPublic) so we can exercise
run_realtime end-to-end with zero network and instant rounds. Proves the wiring
is correct and generates realistic state/*.json (+ status.json) for a dashboard
preview.
"""
import math
import random

import config
import bot
import polymarket_client

# instant rounds, no sleeping
config.MODE = "paper_live"
config.ROUND_SECONDS = 0
config.SAMPLE_SECONDS = 0
config.STARTING_BANKROLL = 250.0
config.DAILY_LOSS_LIMIT_USD = 300.0  # let it run so we get a full blotter
config.DAILY_LOSS_LIMIT_PCT = 0.0
config.MAX_TRADES_PER_DAY = 0
config.MAX_CONSECUTIVE_LOSSES = 0
config.FIXED_STAKE_USD = 0
config.MIN_MOVE_USD = 0
config.MIN_ENTRY_PRICE = 0.05
config.MAX_ENTRY_PRICE = 0.95
config.FEE_BUFFER = 0.0
config.MIN_CONFIDENCE = 0.0
config.MAX_SPREAD = 0.0
config.MIN_ASK_SIZE = 0
config.EDGE_THRESHOLD = 0.04
config.MIN_EV = 0.02
config.MAX_TRADE_REMAINING = 0  # disable early-slot wait in test

rng = random.Random(7)


class FakeFeed:
    """Realistic-ish BTC random walk standing in for the real spot feed."""
    def __init__(self, *a, **k):
        self.p = 68000.0
        self.history = []
    def price(self):
        self.p *= math.exp(rng.gauss(0.00002, 0.0011))
        self.history.append(self.p)
        return self.p
    def recent(self, n):
        return self.history[-n:]
    def get_slot_start_price(self, slot_start_ts):
        return self.p
    def get_slot_close_price(self, slot_start_ts):
        return self.p


class FakePub:
    """Stands in for public Gamma/CLOB reads. Returns the same market shape as
    the real find_btc_5min_market(): fresh UP/DOWN token IDs each slot plus a
    tokens{} map, so bot.run_realtime exercises the real code path."""
    def __init__(self, *a, **k):
        self.n = 0
    def find_btc_5min_market(self):
        self.n += 1
        up, down = "UP_%d" % self.n, "DOWN_%d" % self.n
        return {
            "question": "Will BTC be up or down over the next 5 minutes?",
            "tokens": {"UP": up, "DOWN": down},
            "up": up, "down": down,
            "conditionId": "0xtest",
            "slug": "btc-updown-5m-%d" % (1700000000 + self.n * 300),
            "end_ts": None,  # instant resolution in the test
        }
    def midpoint(self, token_id):
        # real books hover near 0.5 with a spread; occasionally offer an edge
        return min(0.9, max(0.1, 0.5 + rng.gauss(0.05, 0.08)))


bot.LivePriceFeed = FakeFeed
polymarket_client.PolymarketPublic = FakePub

stats = bot.run_realtime(rounds=40, paper=True)
print("\nSELFTEST_OK trades=", stats["trades"], "mode=", stats["mode"])
