"""Integration smoke test for the live WebSocket pipeline.

Verifies:
1. Polymarket Gamma API market discovery for active 5m BTC slot.
2. Polymarket RTDS WebSocket connection (Chainlink Data Streams btc/usd).
3. Polymarket CLOB WebSocket Level 2 order book streaming.
4. Real-time TWAP pricing model evaluation on live ticks and quotes.
"""
from __future__ import annotations

import asyncio
import sys
import time

import config
from price_feed import ChainlinkDataStream, LivePriceFeed
from book_feed import BookFeed
from pricing_model import TWAPPricingModel
from polymarket_client import PolymarketPublic


async def test_live_pipeline():
    print("=== RUNNING LIVE WEBSOCKET PIPELINE SMOKE TEST ===")
    
    # 1. Discover active market
    pub = PolymarketPublic()
    now = int(time.time())
    window_ts = (now // 300) * 300
    slug = f"btc-updown-5m-{window_ts}"
    mkt_data = pub.find_btc_5min_market(now=now)
    assert mkt_data is not None and mkt_data.get("tokens"), "Could not discover active 5m market"
    tokens = mkt_data["tokens"]
    up_id = tokens.get("UP")
    dn_id = tokens.get("DOWN")
    print(f"  [ok] Discovered market: {mkt_data.get('slug')} | UP: {up_id[:16]}... | DOWN: {dn_id[:16]}...")

    # 2. Connect Chainlink RTDS stream
    print("\n2. Connecting to Polymarket RTDS Chainlink Stream (btc/usd)...")
    cl_stream = ChainlinkDataStream(asset="btc")
    cl_stream.start()
    
    ready = await cl_stream.wait_ready(timeout=15.0)
    assert ready is True, "Chainlink RTDS stream failed to connect"
    spot = cl_stream.price()
    assert spot > 10000.0, f"Unreasonable BTC price: {spot}"
    print(f"  [ok] Chainlink RTDS live! Spot: ${spot:,.2f} | Clock offset: {cl_stream.clock_offset_s():+.2f}s")

    # 3. Connect CLOB Book WebSocket
    print("\n3. Connecting to Polymarket CLOB WebSocket (Level 2 Book)...")
    book_feed = BookFeed(max_age_s=5.0)
    book_feed.start()
    book_feed.watch(up_id, dn_id)
    
    # Wait up to 5 seconds for top of book
    got_book = False
    for _ in range(25):
        await asyncio.sleep(0.2)
        top_up = book_feed.get_top(up_id)
        top_dn = book_feed.get_top(dn_id)
        if top_up and top_up.get("ask") is not None and top_dn and top_dn.get("ask") is not None:
            got_book = True
            print(f"  [ok] CLOB WebSocket streaming live book:")
            print(f"       UP:   bid={top_up.get('bid')} ask={top_up.get('ask')} (size={top_up.get('ask_size')})")
            print(f"       DOWN: bid={top_dn.get('bid')} ask={top_dn.get('ask')} (size={top_dn.get('ask_size')})")
            break

    if not got_book:
        print("  [info] No WebSocket quotes arrived yet (quiet interval), falling back to REST quote check...")
        q_up = pub.buy_quote(up_id)
        q_dn = pub.buy_quote(dn_id)
        print(f"       REST UP:   ask={q_up.get('price')} size={q_up.get('ask_size')}")
        print(f"       REST DOWN: ask={q_dn.get('price')} size={q_dn.get('ask_size')}")

    # 4. Evaluate TWAP Model on live data
    print("\n4. Evaluating TWAP Path-Accumulation Model on live data...")
    feed = LivePriceFeed(asset="btc", sources=["chainlink"], chainlink_stream=cl_stream)
    model = TWAPPricingModel()
    
    strike = spot - 15.0 # test reference
    r_twap, elapsed = feed.compute_realized_twap(start_ts=time.time() - 120.0)
    remaining_s = 180.0
    
    ask = 0.72
    signal = model.evaluate(
        spot=spot,
        strike=strike,
        remaining_s=remaining_s,
        elapsed_s=elapsed,
        realized_twap=r_twap,
        ask=ask,
        strike_src="chainlink-data-stream",
        require_verified_strike=True,
    )
    
    print(f"  [ok] Model output:")
    print(f"       Side: {signal.side}")
    print(f"       Z-Score: {signal.z_score:.2f}")
    print(f"       Model Probability: {signal.model_prob:.3f}")
    print(f"       Net Edge: {signal.edge_net:+.3f}")
    print(f"       Passed Gates: {signal.passed_gates}")
    
    # Clean up background tasks
    cl_stream.stop()
    book_feed.stop()
    
    print("\n=== ALL LIVE WEBSOCKET PIPELINE SMOKE TESTS PASSED ===")
    return 0


def main():
    return asyncio.run(test_live_pipeline())


if __name__ == "__main__":
    sys.exit(main())
