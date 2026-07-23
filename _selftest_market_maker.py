"""Unit tests for the market-making core — the subtle bits, in isolation."""
from market_maker import MMQuote, simulate_slot, MMStats


def approx(a, b, eps=1e-6):
    return abs(a - b) < eps


# 1. Both bids fill, UP wins -> spread captured, hedged, positive.
#    UP bid 0.85 fills (ask dips to 0.85), DOWN bid 0.14 fills (ask dips 0.14).
#    Cost = 5*0.85 + 5*0.14 = 4.95. UP wins -> payout 5*1 = 5. pnl = +0.05.
r = simulate_slot(MMQuote(up_bid=0.85, down_bid=0.14, size=5),
                  up_ask_path=[0.88, 0.86, 0.85, 0.87],
                  down_ask_path=[0.15, 0.14, 0.16],
                  resolved_up=True, slot_id="s1")
assert r.filled_up == 5 and r.filled_down == 5, r
assert approx(r.cost, 4.95), r.cost
assert approx(r.pnl, 0.05), r.pnl
assert r.net_inventory == 0, r  # fully hedged
assert r.hedged_pairs == 5, r
print("T1 both-fill hedged spread capture OK  pnl=%.2f" % r.pnl)

# 2. Only UP bid fills (DOWN ask never drops to our bid) -> NAKED long UP.
#    If UP then LOSES, we lose the whole UP cost. This is the real MM risk.
r = simulate_slot(MMQuote(up_bid=0.85, down_bid=0.10, size=5),
                  up_ask_path=[0.86, 0.85],
                  down_ask_path=[0.20, 0.18, 0.19],   # never <= 0.10
                  resolved_up=False, slot_id="s2")     # UP loses
assert r.filled_up == 5 and r.filled_down == 0, r
assert approx(r.pnl, -4.25), r.pnl                     # lost 5*0.85
assert r.net_inventory == 5, r                          # naked long UP
assert r.hedged_pairs == 0, r
print("T2 one-sided fill leaves naked directional risk OK  pnl=%.2f" % r.pnl)

# 3. Neither bid fills (market never traded down to us) -> nothing happens.
r = simulate_slot(MMQuote(up_bid=0.80, down_bid=0.10, size=5),
                  up_ask_path=[0.90, 0.91, 0.92],
                  down_ask_path=[0.20, 0.19],
                  resolved_up=True, slot_id="s3")
assert r.filled_up == 0 and r.filled_down == 0 and r.pnl == 0, r
print("T3 no fill when market never comes to us OK")

# 4. Maker pays NO fee: both fill at 0.50/0.48 (pair 0.98 < 1), either
#    resolution pays the same 5 shares. pnl = 5 - 4.90 = +0.10, no fee taken.
for side in (True, False):
    r = simulate_slot(MMQuote(up_bid=0.50, down_bid=0.48, size=5),
                      up_ask_path=[0.50], down_ask_path=[0.48],
                      resolved_up=side)
    assert approx(r.pnl, 0.10), (side, r.pnl)
print("T4 maker fills pay zero fee, direction-independent OK  pnl=+0.10")

# 5. Aggregate stats: mix of both/one/none, honest risk accounting.
st = MMStats()
st.add(simulate_slot(MMQuote(0.85, 0.14, 5), [0.85], [0.14], True))   # both, +0.05
st.add(simulate_slot(MMQuote(0.85, 0.10, 5), [0.85], [0.30], False))  # one, -4.25
st.add(simulate_slot(MMQuote(0.80, 0.10, 5), [0.95], [0.30], True))   # none, 0
s = st.summary()
assert s["slots"] == 3
assert s["both_filled_pct"] == 33.3, s
assert s["one_filled_pct"] == 33.3, s
assert approx(s["total_pnl"], -4.20), s
assert s["gross_directional_shares"] == 5.0, s   # the one-sided slot's risk
print("T5 aggregate stats + risk accounting OK  ->", s)

# 6. Selective quoting: disabling a side means it never fills, even if the
#    market traded through where our bid would have been.
r = simulate_slot(MMQuote(up_bid=0.85, down_bid=0.14, size=5),
                  up_ask_path=[0.85], down_ask_path=[0.14],
                  resolved_up=True, quote_up=True, quote_down=False)
assert r.filled_up == 5 and r.filled_down == 0, r   # DOWN disabled -> no fill
assert r.net_inventory == 5, r
r2 = simulate_slot(MMQuote(up_bid=0.85, down_bid=0.14, size=5),
                   up_ask_path=[0.85], down_ask_path=[0.14],
                   resolved_up=True, quote_up=False, quote_down=False)
assert r2.filled_up == 0 and r2.filled_down == 0 and r2.pnl == 0, r2
print("T6 selective quoting (disabled side never fills) OK")

print("\nSELFTEST_OK market_maker")
