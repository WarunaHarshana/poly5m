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



_FILLED_STATUSES = {"matched", "filled", "closed"}
_OPEN_STATUSES = {"live", "open", "delayed", "unmatched"}


def _as_dict(obj) -> dict:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    out = {}
    for key in (
        "status", "size_matched", "sizeMatched", "original_size", "originalSize",
        "size", "id", "order_id", "orderID", "associate_trades",
    ):
        if hasattr(obj, key):
            out[key] = getattr(obj, key)
    return out


def extract_order_status(payload) -> str:
    data = _as_dict(payload)
    return str(data.get("status") or "").strip().lower()


def extract_filled_shares(payload) -> float:
    data = _as_dict(payload)
    for key in ("size_matched", "sizeMatched", "filled_shares", "filledSize"):
        val = data.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return 0.0


def is_fill_confirmed(payload) -> bool:
    status = extract_order_status(payload)
    filled = extract_filled_shares(payload)
    return status in _FILLED_STATUSES or filled > 0


class PolymarketLiveClient:
    """Authenticated trading client for Polymarket CLOB (MODE=live)."""

    def __init__(self):
        if not config.PRIVATE_KEY or not config.FUNDER_ADDRESS:
            raise ValueError("MODE=live requires PRIVATE_KEY and FUNDER_ADDRESS in .env")
        self.signature_type = int(config.SIGNATURE_TYPE)
        self.client = self._build_client(self.signature_type)
        try:
            self.client.set_api_creds(self.client.create_or_derive_api_creds())
        except Exception as e:
            raise RuntimeError(f"Failed to initialize py_clob_client: {e}")

    def _clean_key(self) -> str:
        pk = str(config.PRIVATE_KEY).strip()
        if not pk.startswith("0x") and len(pk) == 64:
            pk = "0x" + pk
        return pk

    def _build_client(self, signature_type: int):
        from py_clob_client.client import ClobClient
        return ClobClient(
            host=config.CLOB_HOST,
            key=self._clean_key(),
            chain_id=config.CHAIN_ID,
            signature_type=signature_type,
            funder=config.FUNDER_ADDRESS,
        )

    def place_order(self, token_id: str, side: str, price: float, size: float, order_type: str = "GTC") -> dict:
        """Place an order via CLOB API. Post success != fill — check size_matched/status."""
        from py_clob_client.clob_types import OrderArgs, OrderType
        order_args = OrderArgs(
            token_id=token_id,
            price=round(price, 2),
            size=round(size, 2),
            side=side.upper(),
        )
        ot = OrderType.GTC if order_type.upper() == "GTC" else OrderType.FOK

        def _submit():
            signed_order = self.client.create_order(order_args)
            return self.client.post_order(signed_order, ot)

        try:
            resp = _submit()
        except Exception as initial_err:
            err_str = str(initial_err).lower()
            if ("order version" in err_str or "signature" in err_str or "invalid order inputs" in err_str):
                logger.warning(f"CLOB signature error ({initial_err}). Retrying fallback signature types...")
                resp = None
                for fallback_sig in (3, 1, 2, 0):
                    if fallback_sig == self.signature_type:
                        continue
                    try:
                        logger.info(f"Retrying order with Signature Type {fallback_sig}...")
                        self.client = self._build_client(fallback_sig)
                        self.client.set_api_creds(self.client.create_or_derive_api_creds())
                        self.signature_type = fallback_sig
                        resp = _submit()
                        logger.info(f"Fallback to Signature Type {fallback_sig} SUCCEEDED!")
                        break
                    except Exception as fb_err:
                        logger.warning(f"Fallback Signature Type {fallback_sig} failed: {fb_err}")
                if resp is None:
                    logger.error(f"Live order placement failed: {initial_err}")
                    return {
                        "success": False,
                        "filled": False,
                        "error": str(initial_err),
                        "order_id": None,
                        "status": "",
                        "shares": 0.0,
                        "response": None,
                    }
            else:
                logger.error(f"Live order placement failed: {initial_err}")
                return {
                    "success": False,
                    "filled": False,
                    "error": str(initial_err),
                    "order_id": None,
                    "status": "",
                    "shares": 0.0,
                    "response": None,
                }

        data = _as_dict(resp)
        order_id = data.get("orderID") or data.get("order_id") or data.get("id")
        status = extract_order_status(resp)
        filled_shares = extract_filled_shares(resp)
        filled = is_fill_confirmed(resp)
        # GTC resting bids are "posted" even when unmatched; FOK must be filled
        posted_ok = bool(order_id) or filled or status in _OPEN_STATUSES or status in _FILLED_STATUSES
        return {
            "success": bool(posted_ok),
            "filled": filled,
            "status": status or ("matched" if filled else "live"),
            "order_id": order_id,
            "shares": filled_shares if filled_shares > 0 else float(size),
            "response": resp if isinstance(resp, dict) else data,
        }

    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open resting limit order."""
        try:
            return self.client.cancel(order_id)
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return {}

    def get_collateral_balance(self) -> float:
        """Fetch on-chain USDC/collateral balance from Polymarket CLOB."""
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=self.signature_type)
            resp = self.client.get_balance_allowance(params)
            balance = float(resp.get("balance", 0.0))
            if balance > 10000.0:
                balance = balance / 1e6
            return balance
        except Exception as e:
            logger.error(f"Error fetching balance: {e}")
            return 0.0

