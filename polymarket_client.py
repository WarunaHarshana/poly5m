"""Polymarket Gamma Market Discovery and CLOB Trading Client."""
from __future__ import annotations

import json
import logging
import time
import requests

import config

logger = logging.getLogger("client")


class PolymarketPublicClient:
    """Read-only client for Gamma market discovery and CLOB REST quotes."""

    def __init__(self, timeout: float = 10.0):
        self.session = requests.Session()
        self.timeout = timeout

    def find_active_5m_slot(self, now_ts: float | None = None) -> dict | None:
        """Find active Polymarket 5-minute BTC market metadata by deterministic slug."""
        now = time.time() if now_ts is None else float(now_ts)
        slot = config.SLOT_SECONDS
        window_start = int(now // slot * slot)

        candidates = [window_start, window_start + slot, window_start - slot]
        for start_ts in candidates:
            slug = f"{config.ASSET}-updown-5m-{start_ts}"
            try:
                r = self.session.get(f"{config.GAMMA_HOST}/events", params={"slug": slug}, timeout=self.timeout)
                data = r.json()
                ev = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
                if ev and ev.get("markets"):
                    m = ev["markets"][0]
                    if not m.get("closed"):
                        tokens_raw = json.loads(m.get("clobTokenIds", "[]")) if isinstance(m.get("clobTokenIds"), str) else m.get("clobTokenIds", [])
                        outcomes = json.loads(m.get("outcomes", "[]")) if isinstance(m.get("outcomes"), str) else m.get("outcomes", [])
                        
                        tokens = {}
                        for idx, outcome in enumerate(outcomes):
                            o = str(outcome).lower()
                            if o in ("up", "yes", "higher", "above"):
                                tokens["UP"] = tokens_raw[idx]
                            elif o in ("down", "no", "lower", "below"):
                                tokens["DOWN"] = tokens_raw[idx]
                        
                        if "UP" not in tokens or "DOWN" not in tokens:
                            if len(tokens_raw) >= 2:
                                tokens["UP"] = tokens_raw[0]
                                tokens["DOWN"] = tokens_raw[1]

                        fee_rate = config.TAKER_FEE_RATE
                        try:
                            fs = m.get("feeSchedule")
                            if isinstance(fs, str):
                                fs = json.loads(fs)
                            if isinstance(fs, dict) and "rate" in fs:
                                fee_rate = float(fs["rate"])
                        except Exception:
                            pass

                        return {
                            "slug": slug,
                            "condition_id": m.get("conditionId", ""),
                            "slot_start_ts": start_ts,
                            "end_ts": start_ts + slot,
                            "tokens": tokens,
                            "fee_rate": fee_rate,
                            "question": m.get("question", "BTC 5-minute Up/Down"),
                        }
            except Exception:
                continue

        return None

    def get_order_book(self, token_id: str) -> dict | None:
        """Fetch REST snapshot of the order book for token_id."""
        try:
            r = self.session.get(f"{config.CLOB_HOST}/book", params={"token_id": token_id}, timeout=self.timeout)
            return r.json()
        except Exception:
            return None


class PolymarketLiveClient:
    """Authenticated trading client for Polymarket CLOB (MODE=live)."""

    def __init__(self):
        if not config.PRIVATE_KEY or not config.FUNDER_ADDRESS:
            raise ValueError("MODE=live requires PRIVATE_KEY and FUNDER_ADDRESS in .env")
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            self.client = ClobClient(
                host=config.CLOB_HOST,
                key=config.PRIVATE_KEY,
                chain_id=config.CHAIN_ID,
                signature_type=config.SIGNATURE_TYPE,
                funder=config.FUNDER_ADDRESS,
            )
            # Create or derive API credentials
            self.client.set_api_creds(self.client.create_or_derive_api_creds())
        except Exception as e:
            raise RuntimeError(f"Failed to initialize py_clob_client: {e}")

    def place_order(self, token_id: str, side: str, price: float, size: float) -> dict:
        """Place an order via CLOB API."""
        from py_clob_client.clob_types import OrderArgs, OrderType
        order_args = OrderArgs(
            token_id=token_id,
            price=round(price, 2),
            size=round(size, 2),
            side=side.upper(),
        )
        signed_order = self.client.create_order(order_args)
        resp = self.client.post_order(signed_order, OrderType.FOK)
        return resp
