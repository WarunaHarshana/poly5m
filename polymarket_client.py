"""Polymarket Gamma Market Discovery and CLOB Trading Client."""
from __future__ import annotations

import json
import logging
import time
import requests

import config

logger = logging.getLogger("client")


def _json_list(val) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str) and val.strip():
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def fillable_ask_shares(book: dict | None, limit_px: float, requested_shares: float = 0.0) -> tuple[float, float]:
    """Shares and VWAP available to a buy at or below limit_px (FOK simulation).

    If requested_shares > 0, stops accumulating once enough liquidity is found
    and returns the VWAP for just that portion of the book.
    """
    if not book or limit_px <= 0:
        return 0.0, 0.0
    asks = book.get("asks") or []
    shares = 0.0
    notional = 0.0
    for level in asks:
        try:
            px = float(level.get("price"))
            sz = float(level.get("size") or 0.0)
        except (TypeError, ValueError):
            continue
        if px <= 0 or sz <= 0:
            continue
        if px <= float(limit_px) + 1e-9:
            if requested_shares > 0 and shares >= requested_shares:
                break  # enough liquidity found for the requested order size
            take = sz
            if requested_shares > 0 and (shares + sz) > requested_shares:
                take = requested_shares - shares  # partial fill of this level
            shares += take
            notional += take * px
    vwap = (notional / shares) if shares > 0 else 0.0
    return shares, vwap


def fillable_bid_shares(book: dict | None, requested_shares: float, floor_px: float = 0.0) -> tuple[float, float]:
    """Shares and VWAP a sell of requested_shares would get walking down the real bid book (>= floor_px)."""
    if not book or requested_shares <= 0:
        return 0.0, 0.0
    levels = []
    for level in book.get("bids") or []:
        try:
            px = float(level.get("price") if isinstance(level, dict) else level)
            sz = float((level.get("size") if isinstance(level, dict) else 0.0) or 0.0)
        except (TypeError, ValueError, AttributeError):
            continue
        if 0.0 < px < 1.0 and sz > 0 and px >= floor_px:
            levels.append((px, sz))
    levels.sort(key=lambda x: -x[0])  # best (highest) bid first, regardless of book ordering
    shares = 0.0
    notional = 0.0
    for px, sz in levels:
        take = min(sz, requested_shares - shares)
        if take <= 0:
            break
        shares += take
        notional += take * px
    return shares, ((notional / shares) if shares > 0 else 0.0)


def extract_best_bid_ask(book: dict | None) -> tuple[float | None, float | None]:
    """Extract true best bid and true best ask from a Polymarket orderbook dict.

    Polymarket CLOB orderbooks may list bids ascending (0.01 -> highest)
    and asks descending (0.99 -> lowest).
    Best bid is the HIGHEST bid price.
    Best ask is the LOWEST ask price.
    """
    if not book:
        return None, None
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    best_bid: float | None = None
    best_ask: float | None = None
    for b in bids:
        try:
            p = float(b.get("price") if isinstance(b, dict) else b)
            if 0.0 < p < 1.0:
                best_bid = p if best_bid is None else max(best_bid, p)
        except (KeyError, TypeError, ValueError):
            continue
    for a in asks:
        try:
            p = float(a.get("price") if isinstance(a, dict) else a)
            if 0.0 < p < 1.0:
                best_ask = p if best_ask is None else min(best_ask, p)
        except (KeyError, TypeError, ValueError):
            continue
    return best_bid, best_ask


def extract_market_top(
    book_fav: dict | None, book_opp: dict | None = None
) -> tuple[float | None, float | None, float | None]:
    """Extract (bid, ask, spread) for a token with binary complement inference.

    If ask is missing from book_fav but book_opp has a bid,
    implied ask = round(1.0 - opp_bid, 4).
    If bid is missing from book_fav but book_opp has an ask,
    implied bid = round(1.0 - opp_ask, 4).
    """
    fav_bid, fav_ask = extract_best_bid_ask(book_fav)
    opp_bid, opp_ask = extract_best_bid_ask(book_opp)

    if fav_ask is None and opp_bid is not None:
        fav_ask = round(1.0 - opp_bid, 4)
    if fav_bid is None and opp_ask is not None:
        fav_bid = round(1.0 - opp_ask, 4)

    spread = round(fav_ask - fav_bid, 4) if (fav_bid is not None and fav_ask is not None) else None
    return fav_bid, fav_ask, spread


def parse_event_resolution(ev: dict | None, now_ts: float | None = None) -> dict | None:
    """Map a Gamma event to {side, source, final_price, price_to_beat} once the window is over.

    Does not treat in-window book prices as a result. After close, uses outcomePrices
    (>= RESOLUTION_PRICE_MIN) or eventMetadata.finalPrice vs priceToBeat.
    """
    if not isinstance(ev, dict):
        return None
    markets = ev.get("markets") or []
    m = markets[0] if markets else {}
    meta = ev.get("eventMetadata") if isinstance(ev.get("eventMetadata"), dict) else {}
    end_ts = None
    try:
        # slug ...-{unix}
        slug = str(ev.get("slug") or (m.get("slug") if isinstance(m, dict) else "") or "")
        tail = slug.rsplit("-", 1)[-1]
        end_ts = int(tail) + int(config.SLOT_SECONDS)
    except Exception:
        end_ts = None
    now = time.time() if now_ts is None else float(now_ts)
    window_over = True if end_ts is None else now >= float(end_ts) + 1.0

    outcomes = _json_list(m.get("outcomes") if isinstance(m, dict) else None)
    prices = _json_list(m.get("outcomePrices") if isinstance(m, dict) else None)
    closed = bool(m.get("closed") or ev.get("closed") or m.get("automaticallyResolved"))
    uma_done = str(m.get("umaResolutionStatus") or "").lower() == "resolved"

    def _side_from_label(label: str) -> str | None:
        o = str(label).strip().lower()
        if o in ("up", "yes", "higher", "above"):
            return "UP"
        if o in ("down", "no", "lower", "below"):
            return "DOWN"
        return None

    official = closed or uma_done
    if official or window_over:
        best_side = None
        best_px = -1.0
        for label, raw_px in zip(outcomes, prices):
            try:
                px = float(raw_px)
            except (TypeError, ValueError):
                continue
            side = _side_from_label(label)
            if side and px > best_px:
                best_px = px
                best_side = side
        threshold = 0.99 if official else float(config.RESOLUTION_PRICE_MIN)
        if best_side and best_px >= threshold:
            return {
                "side": best_side,
                "source": "polymarket_outcome_prices",
                "confidence": best_px,
                "final_price": meta.get("finalPrice"),
                "price_to_beat": meta.get("priceToBeat"),
                "official": official,
            }

    try:
        final_px = float(meta["finalPrice"]) if meta.get("finalPrice") is not None else None
        beat = float(meta["priceToBeat"]) if meta.get("priceToBeat") is not None else None
    except (TypeError, ValueError):
        final_px, beat = None, None
    if window_over and final_px is not None and beat is not None and beat > 0:
        side = "UP" if final_px >= beat else "DOWN"
        return {
            "side": side,
            "source": "polymarket_event_metadata",
            "confidence": 1.0,
            "final_price": final_px,
            "price_to_beat": beat,
            "official": official,
        }
    return None


def parse_clob_resolution(data: dict | None) -> dict | None:
    """Map a CLOB /markets/{condition_id} payload to {side, source, official} once a winner is flagged."""
    if not isinstance(data, dict):
        return None
    winners = []
    for tok in data.get("tokens") or []:
        if not isinstance(tok, dict) or tok.get("winner") is not True:
            continue
        o = str(tok.get("outcome") or "").strip().lower()
        if o in ("up", "yes", "higher", "above"):
            winners.append("UP")
        elif o in ("down", "no", "lower", "below"):
            winners.append("DOWN")
    if len(winners) != 1:
        return None
    return {
        "side": winners[0],
        "source": "polymarket_clob_winner",
        "confidence": 1.0,
        "final_price": None,
        "price_to_beat": None,
        "official": True,
    }


class PolymarketPublicClient:
    """Read-only client for Gamma market discovery and CLOB REST quotes."""

    def __init__(self, timeout: float = 10.0):
        self.session = requests.Session()
        self.timeout = timeout

    def find_active_5m_slot(self, now_ts: float | None = None, asset: str | None = None) -> dict | None:
        """Find active Polymarket 5-minute Up/Down market metadata by deterministic slug."""
        now = time.time() if now_ts is None else float(now_ts)
        slot = config.SLOT_SECONDS
        window_start = int(now // slot * slot)
        coin = str(asset or config.ASSET).strip().lower() or "btc"

        candidates = [window_start, window_start + slot, window_start - slot]
        for start_ts in candidates:
            slug = f"{coin}-updown-5m-{start_ts}"
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

                        price_to_beat = None
                        try:
                            meta = ev.get("eventMetadata") if isinstance(ev.get("eventMetadata"), dict) else {}
                            if meta.get("priceToBeat") is not None:
                                price_to_beat = float(meta["priceToBeat"])
                        except Exception:
                            price_to_beat = None

                        return {
                            "slug": slug,
                            "asset": coin,
                            "condition_id": m.get("conditionId", ""),
                            "slot_start_ts": start_ts,
                            "end_ts": start_ts + slot,
                            "tokens": tokens,
                            "fee_rate": fee_rate,
                            "price_to_beat": price_to_beat,
                            "question": m.get("question", f"{coin.upper()} 5-minute Up/Down"),
                        }
            except Exception:
                continue

        return None

    def get_event(self, slug: str) -> dict | None:
        try:
            r = self.session.get(f"{config.GAMMA_HOST}/events", params={"slug": slug}, timeout=self.timeout)
            data = r.json()
            ev = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
            return ev if isinstance(ev, dict) else None
        except Exception:
            return None

    def get_clob_resolution(self, condition_id: str) -> dict | None:
        """Official winner from the CLOB market (tokens[].winner). None until Polymarket marks a winner."""
        if not condition_id:
            return None
        try:
            r = self.session.get(f"{config.CLOB_HOST}/markets/{condition_id}", timeout=self.timeout)
            data = r.json()
        except Exception:
            return None
        return parse_clob_resolution(data)

    def get_resolution(
        self, slug: str, now_ts: float | None = None, condition_id: str | None = None
    ) -> dict | None:
        """Official Up/Down resolution: Gamma event first, then the CLOB winner flag. None if still open."""
        ev = self.get_event(slug)
        res = parse_event_resolution(ev, now_ts=now_ts)
        if res is not None:
            return res
        cid = condition_id
        if not cid and isinstance(ev, dict):
            markets = ev.get("markets") or []
            if markets and isinstance(markets[0], dict):
                cid = markets[0].get("conditionId")
        try:
            end_ts = int(str(slug).rsplit("-", 1)[-1]) + int(config.SLOT_SECONDS)
        except Exception:
            end_ts = None
        now = time.time() if now_ts is None else float(now_ts)
        if cid and (end_ts is None or now >= end_ts + 1.0):
            return self.get_clob_resolution(cid)
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
_CANCELED_STATUSES = {"canceled", "cancelled", "canceled_market_resolved", "invalid", "expired"}


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

    def __init__(self, client=None):
        if not config.PRIVATE_KEY or not config.FUNDER_ADDRESS:
            raise ValueError("MODE=live requires PRIVATE_KEY and FUNDER_ADDRESS")
        sig = config.SIGNATURE_TYPE
        if sig is None or int(sig) not in config.VALID_SIGNATURE_TYPES:
            raise ValueError(
                "MODE=live requires an explicit SIGNATURE_TYPE (0=EOA, 1=proxy, 2=Gnosis Safe, 3=deposit wallet); "
                f"got {sig!r}"
            )
        self.signature_type = int(sig)
        if client is not None:  # injected (tests)
            self.client = client
            self.clob_version = 2
            return
        self.client = self._build_client(self.signature_type)
        try:
            self.client.set_api_creds(self._derive_creds())
        except Exception as e:
            raise RuntimeError(f"Failed to initialize py_clob_client: {type(e).__name__}")

    def _clean_key(self) -> str:
        pk = str(config.PRIVATE_KEY).strip()
        if not pk.startswith("0x") and len(pk) == 64:
            pk = "0x" + pk
        return pk

    def _build_client(self, signature_type: int):
        """CLOB V2 client (py_clob_client_v2) when installed; V1 only for types 0-2 if V2 is missing."""
        try:
            from py_clob_client_v2.client import ClobClient
            self.clob_version = 2
        except ImportError:
            if int(signature_type) == 3:
                raise ValueError("SIGNATURE_TYPE=3 (deposit wallet) requires py-clob-client-v2")
            from py_clob_client.client import ClobClient
            self.clob_version = 1
        return ClobClient(
            host=config.CLOB_HOST,
            key=self._clean_key(),
            chain_id=config.CHAIN_ID,
            signature_type=signature_type,
            funder=config.FUNDER_ADDRESS,
        )

    def _derive_creds(self):
        c = self.client
        if hasattr(c, "create_or_derive_api_key"):
            return c.create_or_derive_api_key()
        return c.create_or_derive_api_creds()

    def _types(self):
        if getattr(self, "clob_version", 2) == 2:
            try:
                from py_clob_client_v2 import clob_types as ct
                return ct
            except ImportError:
                pass
        from py_clob_client import clob_types as ct
        return ct

    @staticmethod
    def _matched_from_post(resp: dict, side: str) -> tuple[float, float | None]:
        """(filled_shares, avg_price) from a post_order response. BUY: takingAmount = shares;
        SELL: makingAmount = shares. Falls back to size_matched."""
        data = _as_dict(resp)
        if isinstance(resp, dict):
            data = {**data, **resp}
        shares = extract_filled_shares(data)
        usd = None
        try:
            making = float(data.get("makingAmount") or 0.0)
            taking = float(data.get("takingAmount") or 0.0)
        except (TypeError, ValueError):
            making = taking = 0.0
        if side.upper() == "SELL" and making > 0:
            shares, usd = making, taking
        elif side.upper() == "BUY" and taking > 0:
            shares, usd = taking, making
        avg = (usd / shares) if (usd and shares) else None
        return float(shares), avg

    def place_order(self, token_id: str, side: str, price: float, size: float, order_type: str = "GTC",
                    max_usd: float | None = None) -> dict:
        """Place an order via CLOB API. Post success != fill: 'shares' is the MATCHED size only (0 if none).
        No signature-type fallback: a signing error is returned as a failure."""
        cap = float(max_usd if max_usd is not None else (getattr(config, "MAX_TRADE_USD", 0.0) or 0.0))
        if cap > 0 and side.upper() == "BUY" and round(price, 2) * round(size, 2) > cap + 1e-9:
            logger.error(f"BUY blocked: {size:.2f} sh @ {price:.2f} exceeds MAX_TRADE_USD ${cap:.2f}")
            return {"success": False, "filled": False, "error": "exceeds MAX_TRADE_USD", "order_id": None,
                    "status": "", "shares": 0.0, "avg_price": None, "response": None}
        ct = self._types()
        OrderArgs, OrderType = ct.OrderArgs, ct.OrderType
        order_args = OrderArgs(token_id=token_id, price=round(price, 2), size=round(size, 2), side=side.upper())
        ot = {"GTC": OrderType.GTC, "FAK": OrderType.FAK, "GTD": OrderType.GTD}.get(
            str(order_type).upper(), OrderType.FOK
        )
        try:
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order, ot)
        except Exception as e:
            logger.error(f"Live order placement failed ({side} {order_type}): {e}")
            return {"success": False, "filled": False, "error": str(e), "order_id": None,
                    "status": "", "shares": 0.0, "avg_price": None, "response": None}
        data = _as_dict(resp)
        if isinstance(resp, dict):
            data = {**data, **resp}
        order_id = data.get("orderID") or data.get("order_id") or data.get("id")
        status = extract_order_status(data)
        filled_shares, avg_price = self._matched_from_post(data, side)
        filled = filled_shares > 0 or status in _FILLED_STATUSES
        ok_flag = data.get("success")
        posted_ok = (ok_flag is not False) and (bool(order_id) or filled or status in _OPEN_STATUSES)
        if data.get("errorMsg"):
            logger.warning(f"Live order response errorMsg: {data.get('errorMsg')}")
        return {
            "success": bool(posted_ok),
            "filled": bool(filled and filled_shares > 0),
            "status": status or ("matched" if filled else "live"),
            "order_id": order_id,
            "shares": round(filled_shares, 6),
            "avg_price": avg_price,
            "response": data,
        }

    def get_order_state(self, order_id: str) -> dict | None:
        """Exchange view of one order: {status, size_matched, original_size, price}. None on error."""
        try:
            o = self.client.get_order(order_id)
        except Exception as e:
            logger.warning(f"get_order {order_id} failed: {e}")
            return None
        d = _as_dict(o)
        if isinstance(o, dict):
            d = {**d, **o}
        if not d:
            return None

        def _f(*keys):
            for k in keys:
                if d.get(k) is not None:
                    try:
                        return float(d[k])
                    except (TypeError, ValueError):
                        pass
            return None
        return {
            "status": extract_order_status(d),
            "size_matched": _f("size_matched", "sizeMatched") or 0.0,
            "original_size": _f("original_size", "originalSize", "size"),
            "price": _f("price"),
        }

    def cancel_order(self, order_id: str) -> dict:
        """Cancel a resting order. Returns the raw response; {} on error (use cancel_and_confirm)."""
        try:
            if hasattr(self.client, "cancel_order"):  # CLOB V2
                payload_cls = getattr(self._types(), "OrderPayload", None)
                if payload_cls is not None:
                    return self.client.cancel_order(payload_cls(orderID=str(order_id))) or {}
            return self.client.cancel(order_id) or {}
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return {}

    def cancel_and_confirm(self, order_id: str) -> dict:
        """Cancel, then read the order back. Returns {confirmed, size_matched, status}.
        confirmed=True only when the exchange shows the order is no longer resting
        (canceled / matched / filled); size_matched is what actually filled before the cancel."""
        resp = self.cancel_order(order_id)
        state = self.get_order_state(order_id)
        if state is None:
            canceled = order_id in ((resp or {}).get("canceled") or [])
            return {"confirmed": False, "size_matched": 0.0, "status": "cancel_ack" if canceled else "unknown"}
        status = state["status"]
        terminal = status in _FILLED_STATUSES or status in _CANCELED_STATUSES
        # Exchange read-back can lag the cancel ack by ~1s (observed live): re-poll briefly.
        tries = int(getattr(config, "CANCEL_CONFIRM_RETRIES", 6))
        while not terminal and tries > 0:
            tries -= 1
            time.sleep(float(getattr(config, "CANCEL_CONFIRM_POLL_S", 0.5)))
            s2 = self.get_order_state(order_id)
            if s2 is None:
                continue
            state, status = s2, s2["status"]
            terminal = status in _FILLED_STATUSES or status in _CANCELED_STATUSES
        return {"confirmed": bool(terminal), "size_matched": float(state["size_matched"] or 0.0), "status": status}

    def get_wallet_balance(self) -> float | None:
        """USDC collateral balance of the funder wallet as seen by the CLOB. None on error (never 0.0)."""
        try:
            ct = self._types()
            AssetType, BalanceAllowanceParams = ct.AssetType, ct.BalanceAllowanceParams
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=self.signature_type)
            resp = self.client.get_balance_allowance(params) or {}
            raw = resp.get("balance")
            if raw is None:
                return None
            return float(raw) / 1e6  # CLOB reports 6-decimal base units
        except Exception as e:
            logger.error(f"Error fetching wallet balance: {e}")
            return None

    def get_collateral_balance(self) -> float:
        bal = self.get_wallet_balance()
        return 0.0 if bal is None else bal
