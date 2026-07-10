"""Diagnostic: show what the discovery logic finds RIGHT NOW.

Run this if `paper_live`/`live` reports it can't find the market. It prints the
current slot slug it computes, the raw event the API returns, and the mapped
UP/DOWN token IDs — so you can see exactly what's happening (and copy a slug into
your browser to sanity-check on polymarket.com).

    python discover.py
"""
import json
import time

import config
from polymarket_client import PolymarketPublic

EVENT_URL = "https://polymarket.com/event/"


def main():
    pub = PolymarketPublic()
    now = time.time()
    slot = config.SLOT_SECONDS
    base = int(now // slot * slot)
    print(f"now={now:.0f}  current slot start={base}")
    print("candidate slugs (open one in your browser to verify):")
    for s in (base, base + slot, base - slot):
        slug = f"{config.BTC_SLUG_PREFIX}{s}"
        state = "future" if s + slot > now else "past"
        print(f"  {slug}  end={s + slot} ({state})  {EVENT_URL}{slug}")

    print("\n--- raw event for current slot ---")
    ev = pub._fetch_event_by_slug(f"{config.BTC_SLUG_PREFIX}{base}")
    if not ev:
        print("  (no event returned for the current slot slug)")
    else:
        print(f"  title: {ev.get('title')}")
        for m in (ev.get("markets") or []):
            print(f"    market: {m.get('question')}")
            print(f"      closed={m.get('closed')} acceptingOrders={m.get('acceptingOrders')}")
            print(f"      clobTokenIds={m.get('clobTokenIds')}")
            print(f"      outcomes={m.get('outcomes')}")

    print("\n--- find_btc_5min_market() result ---")
    try:
        print(json.dumps(pub.find_btc_5min_market(), indent=2))
    except Exception as e:
        print(f"  FAILED: {e}")


if __name__ == "__main__":
    main()
