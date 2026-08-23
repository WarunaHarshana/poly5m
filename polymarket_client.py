"""Polymarket access.

- PolymarketPublic: read-only, NO auth and NO wallet needed. Uses the public
  Gamma (market data) + CLOB (prices/book) REST endpoints. This is all that
  `paper_live` mode requires.
- PolymarketClient: authenticated CLOB V2 trading client for `live` mode
  (needs `py-clob-client-v2`, a funded wallet, and pUSD/POL).

Market discovery is deterministic: Polymarket's recurring 5-minute BTC up/down
markets are events whose slug encodes the slot's Unix start time (a multiple of
SLOT_SECONDS), e.g. "btc-updown-5m-1771582500". Every round we compute the
current slot start and fetch that event by slug, so the correct token IDs are
always found automatically with nothing to paste. A fuzzy /markets scan is used
only as a fallback.
"""
from __future__ import annotations
import json
import logging
import time

import config

UP_WORDS = {"up", "yes", "higher", "above"}
DOWN_WORDS = {"down", "no", "lower", "below"}

# py-clob-client-v2 logs every timeout as ERROR even when we handle retries.
# Keep the console clean; real failures still surface via our own messages.
logging.getLogger("py_clob_client_v2").setLevel(logging.CRITICAL)
logging.getLogger("py_clob_client_v2.http_helpers").setLevel(logging.CRITICAL)
logging.getLogger("py_clob_client_v2.http_helpers.helpers").setLevel(logging.CRITICAL)

_clob_http_patched = False
_last_balance_refresh_ts = 0.0


def _patch_clob_http_timeout(timeout_s: float = 30.0) -> None:
    """Give CLOB httpx client a longer timeout (library default is too short)."""
    global _clob_http_patched
    if _clob_http_patched:
        return
    try:
        import httpx
        from py_clob_client_v2.http_helpers import helpers as h
        # Replace shared client with explicit connect/read timeouts.
        try:
            old = getattr(h, "_http_client", None)
            if old is not None:
                try:
                    old.close()
                except Exception:
                    pass
        except Exception:
            pass
        h._http_client = httpx.Client(
            http2=True,
            timeout=httpx.Timeout(timeout_s, connect=min(15.0, timeout_s)),
        )
        _clob_http_patched = True
    except Exception:
        pass


class PolymarketPublic:
    """Read-only market data via public REST. No key required."""

    def __init__(self, timeout: float = 12.0):
        import requests
        self.requests = requests
        self.timeout = timeout

    # --- low-level helpers ---
    @staticmethod
    def _parse_json_field(v):
        """Gamma returns clobTokenIds/outcomes as JSON-encoded strings."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return None
        return v

    @classmethod
    def _map_tokens(cls, tokens_raw, outcomes):
        """Map [tokenId, tokenId] + [outcomeLabel, ...] to {UP, DOWN}."""
        if not tokens_raw or len(tokens_raw) < 2:
            return None
        up = down = None
        for i, oc in enumerate(outcomes or []):
            if i >= len(tokens_raw):
                break
            o = str(oc).strip().lower()
            if o in UP_WORDS:
                up = tokens_raw[i]
            elif o in DOWN_WORDS:
                down = tokens_raw[i]
        if up is None or down is None:
            up, down = tokens_raw[0], tokens_raw[1]  # positional fallback
        return {"UP": up, "DOWN": down}

    def _fetch_event_by_slug(self, slug: str):
        """Return the raw Gamma event dict for a slug, or None."""
        r = self.requests.get(f"{config.GAMMA_HOST}/events",
                              params={"slug": slug}, timeout=self.timeout)
        try:
            data = r.json()
        except Exception:
            return None
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict):
            arr = data.get("data") or data.get("events")
            if isinstance(arr, list) and arr:
                return arr[0]
            if data.get("markets") or data.get("slug"):
                return data
        return None

    def _pack(self, m: dict, slug: str, slot_start):
        tokens = self._map_tokens(self._parse_json_field(m.get("clobTokenIds")),
                                  self._parse_json_field(m.get("outcomes")))
        if not tokens:
            return None
        end_ts = (slot_start + config.SLOT_SECONDS) if slot_start else None
        # Taker fee rate from Gamma (crypto 5m markets charge
        # rate * min(p, 1-p) per share, taker-only). Fallback to config.
        fee_rate = None
        try:
            fs = m.get("feeSchedule")
            if isinstance(fs, str):
                fs = json.loads(fs)
            if m.get("feesEnabled") and isinstance(fs, dict):
                fee_rate = float(fs.get("rate"))
        except Exception:
            fee_rate = None
        if fee_rate is None:
            fee_rate = (float(getattr(config, "TAKER_FEE_RATE", 0.07))
                        if m.get("feesEnabled") in (True, None) else 0.0)
        # Venue order minimums, published per market. Reading them beats the
        # hard-coded config default: the cheapest valid order is exactly
        # orderMinSize shares, and if Polymarket ever changes it the bot
        # follows instead of silently over- or under-sizing.
        try:
            min_order_size = float(m.get("orderMinSize"))
        except (TypeError, ValueError):
            min_order_size = None
        try:
            tick_size = float(m.get("orderPriceMinTickSize"))
        except (TypeError, ValueError):
            tick_size = None
        return {
            "question": m.get("question") or "BTC 5-minute Up/Down",
            "tokens": tokens,
            "up": tokens["UP"],
            "down": tokens["DOWN"],
            "conditionId": m.get("conditionId") or "",
            "end_ts": end_ts,
            "slug": slug,
            "fee_rate": fee_rate,
            "min_order_size": min_order_size,
            "tick_size": tick_size,
        }

    def _open_market_from_event(self, ev: dict, slug: str, slot_start):
        """Pick the first tradeable market in an event and pack it."""
        if not ev:
            return None
        for m in (ev.get("markets") or []):
            if m.get("closed"):
                continue
            if m.get("acceptingOrders") is False:
                continue
            packed = self._pack(m, slug, slot_start)
            if packed:
                return packed
        return None

    def _scan_markets(self):
        """Fuzzy fallback: scan active markets for a BTC up/down 5-minute market."""
        r = self.requests.get(
            f"{config.GAMMA_HOST}/markets",
            params={"active": "true", "closed": "false", "limit": 500,
                    "order": "volume24hr", "ascending": "false"},
            timeout=self.timeout,
        )
        try:
            markets = r.json()
        except Exception:
            return None
        for m in markets or []:
            text = ((m.get("question") or "") + " " + (m.get("slug") or "")).lower()
            if config.BTC_MARKET_MATCH and config.BTC_MARKET_MATCH not in text:
                continue
            if not any(a in text for a in config.BTC_MARKET_ASSET_TERMS):
                continue
            if not any(u in text for u in config.BTC_MARKET_UPDOWN_TERMS):
                continue
            packed = self._pack(m, m.get("slug") or "", None)
            if packed:
                return packed
        return None

    def find_btc_5min_market(self, now: float | None = None) -> dict:
        """Return {question, tokens{UP,DOWN}, up, down, conditionId, end_ts, slug}
        for the current BTC 5-minute up/down slot.

        Fetches the event by deterministic slug for the current slot (and the
        neighbouring slots as a cushion), then falls back to a fuzzy scan.

        `now` should be Chainlink/stream wall time when available — a skewed PC
        clock will otherwise keep rediscovering an already-ended slot.
        """
        now = time.time() if now is None else float(now)
        slot = config.SLOT_SECONDS
        base = int(now // slot * slot)
        # If current window is already over (or nearly over), prefer the next slug.
        # IMPORTANT: do NOT use MIN_SLOT_SECONDS here — that is a *trade* gate.
        # Using 90s caused hybrid to abandon the current slot before late_scalp
        # (8–35s) could ever run.
        rem_cur = (base + slot) - now
        advance_below = int(getattr(config, "MARKET_ADVANCE_REMAINING", 5) or 5)
        advance_below = max(1, min(advance_below, 30))
        if rem_cur <= 0:
            candidates = (base + slot, base + 2 * slot, base)
        elif rem_cur < advance_below:
            candidates = (base + slot, base, base - slot)
        else:
            candidates = (base, base + slot, base - slot)

        for slot_start in candidates:
            packed = self.find_btc_5min_market_at(slot_start, now=now)
            if packed:
                return packed

        packed = self._scan_markets()
        if packed:
            end_ts = packed.get("end_ts")
            if end_ts is None or end_ts > now:
                return packed

        raise RuntimeError(
            "No open BTC 5-minute up/down market found. The series may be between "
            "slots (just retry), or Polymarket renamed it \u2014 run `python discover.py` "
            "to inspect, and set BTC_SLUG_PREFIX / BTC_MARKET_MATCH in .env if needed.")

    def find_btc_5min_market_at(
        self, slot_start_ts: int | float, now: float | None = None
    ) -> dict | None:
        """Fetch a specific 5m slot by start timestamp (for hybrid late pin)."""
        now = time.time() if now is None else float(now)
        slot_start = int(slot_start_ts)
        slug = f"{config.BTC_SLUG_PREFIX}{slot_start}"
        try:
            ev = self._fetch_event_by_slug(slug)
        except Exception:
            ev = None
        packed = self._open_market_from_event(ev, slug, slot_start)
        if not packed:
            return None
        end_ts = packed.get("end_ts")
        if end_ts is not None and end_ts <= now:
            return None
        return packed

    def get_official_btc_updown_outcome(self, slug: str) -> str | None:
        """Return 'UP' or 'DOWN' once Polymarket has resolved the event.

        Reads Gamma outcomePrices after automatic Chainlink Data Streams
        settlement. Returns None if the market is still open / unresolved.
        This is the only resolution source that matches the website for
        paper_live PnL — exchange candles and on-chain Aggregator feeds are
        *not* the same oracle Polymarket uses.
        """
        if not slug:
            return None
        ev = self._fetch_event_by_slug(slug)
        if not ev:
            return None
        for m in (ev.get("markets") or []):
            outcomes = self._parse_json_field(m.get("outcomes")) or []
            prices = self._parse_json_field(m.get("outcomePrices")) or []
            if len(outcomes) < 2 or len(prices) < len(outcomes):
                continue
            scored = []
            for oc, px in zip(outcomes, prices):
                try:
                    scored.append((str(oc).strip().upper(), float(px)))
                except (TypeError, ValueError):
                    continue
            if not scored:
                continue
            best_label, best_px = max(scored, key=lambda x: x[1])
            # After resolution the winning side is ~1.0; mid-market is ~0.5.
            resolved = (
                m.get("umaResolutionStatus") == "resolved"
                or m.get("closed") is True
                or m.get("automaticallyResolved") is True
            )
            if not resolved:
                return None
            if best_label in UP_WORDS or best_label == "UP":
                return "UP"
            if best_label in DOWN_WORDS or best_label == "DOWN":
                return "DOWN"
            # Positional fallback: first outcome is usually Up
            if best_label == str(outcomes[0]).strip().upper():
                return "UP"
            return "DOWN"
        return None

    @staticmethod
    def _best_level(levels, want_max: bool):
        px = []
        for lv in levels or []:
            try:
                px.append(float(lv.get("price")))
            except Exception:
                pass
        if not px:
            return None
        return max(px) if want_max else min(px)

    def _book_sides(self, token_id: str):
        """Return (best_bid, best_ask, bid_size, ask_size) or Nones if unusable."""
        try:
            b = self.book(token_id)
        except Exception:
            return None, None, None, None

        def _best_with_size(levels, want_max):
            best_px, best_sz = None, None
            for lv in levels or []:
                try:
                    px = float(lv.get("price"))
                    sz = float(lv.get("size") or lv.get("amount") or 0)
                except Exception:
                    continue
                if best_px is None:
                    best_px, best_sz = px, sz
                    continue
                if want_max and px > best_px:
                    best_px, best_sz = px, sz
                elif not want_max and px < best_px:
                    best_px, best_sz = px, sz
                elif px == best_px:
                    best_sz = (best_sz or 0) + sz
            return best_px, best_sz

        bb, bb_sz = _best_with_size(b.get("bids"), True)
        ba, ba_sz = _best_with_size(b.get("asks"), False)
        return bb, ba, bb_sz, ba_sz

    def _book_mid(self, token_id: str):
        """Fallback midpoint from the order book: (best_bid + best_ask) / 2.

        Requires BOTH sides; a one-sided book is treated as no quote."""
        bb, ba, _, _ = self._book_sides(token_id)
        if bb is not None and ba is not None:
            return (bb + ba) / 2.0
        return None

    def midpoint(self, token_id: str) -> float:
        """Real order-book midpoint (0..1) for an outcome token, validated.

        An empty or one-sided book can make the CLOB return a missing/zero
        midpoint. Trusting a ~0 price would imply buying contracts for almost
        nothing and booking an absurd payout, so we fall back to the book mid
        and finally raise if there is no sane (0,1) price -- the caller then
        skips the slot instead of recording a fake 2000x fill.
        """
        mid = None
        try:
            data = self.requests.get(
                f"{config.CLOB_HOST}/midpoint",
                params={"token_id": token_id}, timeout=self.timeout).json()
            if isinstance(data, dict) and data.get("mid") not in (None, ""):
                mid = float(data["mid"])
        except Exception:
            mid = None
        if mid is None or not (0.0 < mid < 1.0):
            mid = self._book_mid(token_id)
        if mid is None or not (0.0 < mid < 1.0):
            raise ValueError(
                f"no valid midpoint (got {mid!r}); order book is empty "
                "or one-sided for this token")
        return mid

    def buy_quote(self, token_id: str) -> dict:
        """Executable buy price for paper/live sizing.

        Prefer the best ask (what you actually pay to lift offers). Fall back to
        midpoint if the book is incomplete. Also returns mid/spread/depth.

        Returns: {price, mid, best_bid, best_ask, spread, ask_size, bid_size}
        Raises ValueError if no sane price in (0, 1).
        """
        bb, ba, bb_sz, ba_sz = self._book_sides(token_id)
        mid = None
        try:
            mid = self.midpoint(token_id)
        except ValueError:
            mid = None
        if mid is None and bb is not None and ba is not None:
            mid = (bb + ba) / 2.0

        # Executable price: best ask when available; otherwise mid (last resort).
        price = ba if ba is not None and 0.0 < ba < 1.0 else mid
        if price is None or not (0.0 < price < 1.0):
            raise ValueError(
                f"no valid buy quote (bid={bb!r} ask={ba!r} mid={mid!r}); "
                "order book is empty or one-sided for this token")

        spread = None
        if bb is not None and ba is not None and ba >= bb:
            spread = ba - bb
        return {
            "price": float(price),
            "mid": float(mid) if mid is not None else float(price),
            "best_bid": bb,
            "best_ask": ba,
            "spread": spread,
            "ask_size": ba_sz,
            "bid_size": bb_sz,
        }

    def book(self, token_id: str) -> dict:
        r = self.requests.get(f"{config.CLOB_HOST}/book",
                              params={"token_id": token_id}, timeout=self.timeout)
        return r.json()

    def get_order_book(self, token_id: str) -> dict:
        """Alias for book() to match Polymarket CLOB conventions."""
        return self.book(token_id)


class PolymarketClient:
    """Authenticated trading client (live mode only)."""

    # Process-wide cache so dashboard balance polls don't re-auth every few seconds
    # (re-auth hammers POST /auth/api-key and prints noisy 400s).
    _shared = None
    _shared_fp = None

    def __init__(self, use_shared: bool = True):
        self._clob = None
        self._use_shared = use_shared

    @staticmethod
    def _normalize_private_key(pk: str) -> str:
        pk = (pk or "").strip().strip('"').strip("'")
        if not pk:
            raise RuntimeError("PRIVATE_KEY is empty")
        if pk.startswith("0X"):
            pk = "0x" + pk[2:]
        if not pk.startswith("0x"):
            pk = "0x" + pk
        body = pk[2:]
        if len(body) != 64 or any(c not in "0123456789abcdefABCDEF" for c in body):
            raise RuntimeError(
                "PRIVATE_KEY must be 64 hex characters (optional 0x prefix)")
        return pk

    @staticmethod
    def _creds_path() -> str:
        import os
        root = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(root, "state", "poly_api_creds.json")

    @classmethod
    def _fingerprint(cls) -> str:
        pk = (config.PRIVATE_KEY or "")[-8:]
        funder = (config.FUNDER_ADDRESS or "").lower()
        sig = str(getattr(config, "SIGNATURE_TYPE", 0))
        return f"{pk}|{funder}|{sig}"

    @classmethod
    def _load_cached_creds(cls):
        import json
        import os
        path = cls._creds_path()
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("fingerprint") != cls._fingerprint():
                return None
            if not (data.get("api_key") and data.get("api_secret")
                    and data.get("api_passphrase")):
                return None
            return data
        except Exception:
            return None

    @classmethod
    def _save_cached_creds(cls, creds, signature_type: int | None = None) -> None:
        import json
        import os
        path = cls._creds_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                "fingerprint": cls._fingerprint(),
                "api_key": getattr(creds, "api_key", None),
                "api_secret": getattr(creds, "api_secret", None),
                "api_passphrase": getattr(creds, "api_passphrase", None),
                "signature_type": int(
                    signature_type if signature_type is not None
                    else getattr(config, "SIGNATURE_TYPE", 3)
                ),
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
        except Exception as e:
            print(f"  [warn] could not cache API creds: {e}")

    def _build_client(self):
        """Create authenticated ClobClient with funder + signature_type + server time.

        API-key notes:
        - L1 auth is signed by the EOA private key.
        - Prefer cached creds, then derive (GET). Avoid create when possible —
          existing keys return 400 "Could not create api key" (noisy, not fatal).
        - use_server_time=True avoids PC clock skew killing L1 auth.
        """
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds

        if not config.PRIVATE_KEY or not config.FUNDER_ADDRESS:
            raise RuntimeError(
                "PRIVATE_KEY and FUNDER_ADDRESS required for live client")

        pk = self._normalize_private_key(config.PRIVATE_KEY)
        funder = str(config.FUNDER_ADDRESS).strip()
        sig = int(config.SIGNATURE_TYPE)

        # py-clob-client-v2 SignatureTypeV2:
        #   0=EOA  1=POLY_PROXY  2=GNOSIS_SAFE  3=POLY_1271 (deposit wallet)
        # Polymarket.com deposit/proxy funders need type 3 — type 1 often returns
        # collateral balance "0" for the same funder (verified).
        sig, eoa = self._resolve_signature_type(pk, funder, sig)

        client = self._make_clob(pk, funder, sig)

        def _apply(creds_obj) -> bool:
            if not creds_obj or not getattr(creds_obj, "api_key", None):
                return False
            client.set_api_creds(creds_obj)
            return True

        # 1) Explicit .env API creds
        api_key = getattr(config, "POLY_API_KEY", None)
        api_secret = getattr(config, "POLY_API_SECRET", None)
        api_pass = getattr(config, "POLY_API_PASSPHRASE", None)
        if api_key and api_secret and api_pass:
            if _apply(ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_pass,
            )):
                return client

        # 2) Local cache from a previous successful derive (skip network auth)
        cached = self._load_cached_creds()
        if cached:
            # Prefer signature type that was used when balance last worked
            cached_sig = cached.get("signature_type")
            if cached_sig is not None and int(cached_sig) != int(sig):
                sig = int(cached_sig)
                config.SIGNATURE_TYPE = sig
                client = self._make_clob(pk, funder, sig)
            if _apply(ApiCreds(
                api_key=cached["api_key"],
                api_secret=cached["api_secret"],
                api_passphrase=cached["api_passphrase"],
            )):
                return client

        # 3) Derive with retries (timeouts are common; create is often 400 noise)
        derive_err = None
        creds = None
        for attempt in range(3):
            try:
                creds = client.derive_api_key()
                if creds and getattr(creds, "api_key", None):
                    break
            except Exception as e:
                derive_err = e
                time.sleep(0.8 * (attempt + 1))
                creds = None

        # 4) Create only if derive failed (may 400 if key already exists)
        create_err = None
        if not creds or not getattr(creds, "api_key", None):
            try:
                creds = client.create_api_key()
            except Exception as e:
                create_err = e
                creds = None

        if not creds or not getattr(creds, "api_key", None):
            raise RuntimeError(
                "Polymarket API key create/derive failed.\n"
                f"  derive error: {derive_err}\n"
                f"  create error: {create_err}\n"
                "Common fixes:\n"
                "  1) Sync Windows clock\n"
                "  2) Check PRIVATE_KEY / FUNDER / SIGNATURE_TYPE\n"
                "  3) Network timeout - retry; avoid spamming Refresh balance\n"
                "  4) After one success, creds are cached in state/poly_api_creds.json\n"
            )

        client.set_api_creds(creds)
        self._save_cached_creds(creds, signature_type=sig)
        # Print only when we actually derived/created (not on cache hits)
        print("  [auth] Polymarket API key ready (cached for this wallet) "
              f"sig_type={sig}"
              + (f" eoa={eoa[:8]}… funder={funder[:8]}…" if eoa else ""))
        return client

    @staticmethod
    def _make_clob(pk: str, funder: str, sig: int):
        from py_clob_client_v2 import ClobClient
        _patch_clob_http_timeout(
            float(getattr(config, "CLOB_HTTP_TIMEOUT", 30) or 30)
        )
        return ClobClient(
            config.CLOB_HOST,
            chain_id=int(config.CHAIN_ID),
            key=pk,
            signature_type=int(sig),
            funder=funder,
            use_server_time=True,
            retry_on_error=True,
        )

    @classmethod
    def _resolve_signature_type(cls, pk: str, funder: str, sig: int):
        """Map wallet layout to CLOB signature type. Returns (sig, eoa_or_None).

        Polymarket.com deposit wallets (funder != EOA) use POLY_1271 (type 3).
        Type 1 (POLY_PROXY) often returns collateral balance 0 for those wallets.
        """
        eoa = None
        try:
            from eth_account import Account
            eoa = Account.from_key(pk).address
        except Exception:
            return int(sig), None

        funder_l = (funder or "").lower()
        eoa_l = eoa.lower()
        if funder_l and eoa_l == funder_l:
            if sig != 0:
                if not getattr(cls, "_sig_warn_printed", False):
                    print("  [auth] funder matches key - using SIGNATURE_TYPE=0 (EOA)")
                    cls._sig_warn_printed = True
                sig = 0
                config.SIGNATURE_TYPE = 0
            return 0, eoa

        # funder != EOA → deposit / site proxy
        if sig == 2:
            return 2, eoa  # explicit Gnosis Safe
        if sig != 3:
            if not getattr(cls, "_sig_warn_printed", False):
                print(f"  [auth] funder != key address - using SIGNATURE_TYPE=3 "
                      f"(POLY_1271 deposit wallet); was {sig}")
                print(f"         EOA={eoa}  funder={funder}")
                print("         (type 1 returns $0 collateral for most site proxies)")
                cls._sig_warn_printed = True
            sig = 3
            config.SIGNATURE_TYPE = 3
        return 3, eoa

    def _client(self):
        if self._use_shared:
            # Resolve sig before fingerprint so env reloads don't bust the cache.
            try:
                pk = self._normalize_private_key(config.PRIVATE_KEY or "")
                funder = str(config.FUNDER_ADDRESS or "").strip()
                sig = int(getattr(config, "SIGNATURE_TYPE", 3) or 3)
                sig, _ = self._resolve_signature_type(pk, funder, sig)
            except Exception:
                pass
            fp = self._fingerprint()
            if (PolymarketClient._shared is not None
                    and PolymarketClient._shared_fp == fp):
                return PolymarketClient._shared
            clob = self._build_client()
            PolymarketClient._shared = clob
            PolymarketClient._shared_fp = fp
            self._clob = clob
            return clob
        if self._clob is None:
            self._clob = self._build_client()
        return self._clob

    def reset(self):
        """Drop cached client (e.g. after .env wallet change)."""
        self._clob = None
        PolymarketClient._shared = None
        PolymarketClient._shared_fp = None

    @staticmethod
    def _parse_usdc_amount(raw) -> float | None:
        """CLOB returns USDC collateral often as 6-decimal base units.

        Examples: \"50000000\" -> $50.00 · \"50.25\" -> $50.25
        """
        if raw is None:
            return None
        try:
            s = str(raw).strip().replace(",", "")
            if not s:
                return None
            if "." in s:
                return round(float(s), 6)
            # Integer string / int: treat as 6-decimal fixed-point
            v = float(int(s)) / 1e6
            return round(v, 6)
        except (TypeError, ValueError):
            return None

    def get_usdc_balance(self, refresh: bool = True) -> dict:
        """Fetch collateral (USDC) balance + allowance for the funder wallet.

        Returns:
          {ok, balance_usd, allowance_usd, funder, raw, error?}

        Note: update_balance_allowance is rate-limited to once per
        CLOB_BALANCE_REFRESH_S seconds — dashboard polls must not hammer CLOB.
        """
        global _last_balance_refresh_ts
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        _patch_clob_http_timeout(
            float(getattr(config, "CLOB_HTTP_TIMEOUT", 30) or 30)
        )
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        try:
            c = self._client()
        except Exception as e:
            return {
                "ok": False,
                "balance_usd": None,
                "allowance_usd": None,
                "funder": config.FUNDER_ADDRESS,
                "raw": None,
                "error": str(e),
            }
        # Full on-chain refresh is expensive and often times out on Windows.
        refresh_every = float(getattr(config, "CLOB_BALANCE_REFRESH_S", 60) or 60)
        now = time.time()
        do_refresh = bool(refresh) and (
            now - _last_balance_refresh_ts >= refresh_every
        )
        if do_refresh:
            try:
                c.update_balance_allowance(params)
                _last_balance_refresh_ts = now
            except Exception:
                pass  # still try cached get
        try:
            raw = c.get_balance_allowance(params)
        except Exception as e:
            err = str(e)
            if "Request exception" in err or "timeout" in err.lower():
                err = "CLOB timed out (network/API busy) — will retry"
            return {
                "ok": False,
                "balance_usd": None,
                "allowance_usd": None,
                "funder": config.FUNDER_ADDRESS,
                "raw": None,
                "error": err,
            }
        bal = allow = None
        if isinstance(raw, dict):
            bal = self._parse_usdc_amount(
                raw.get("balance") if raw.get("balance") is not None
                else raw.get("balance_usd") or raw.get("collateral"))
            allow = self._parse_usdc_amount(
                raw.get("allowance") if raw.get("allowance") is not None
                else raw.get("allowance_usd"))
            # Newer CLOB shape: allowances: { spender: amount, ... }
            if allow is None and isinstance(raw.get("allowances"), dict):
                try:
                    allow = max(
                        (self._parse_usdc_amount(v) or 0.0)
                        for v in raw["allowances"].values()
                    ) if raw["allowances"] else None
                except Exception:
                    allow = None
            # nested shapes
            if bal is None:
                for k in ("balances", "data", "result"):
                    sub = raw.get(k)
                    if isinstance(sub, dict):
                        bal = self._parse_usdc_amount(
                            sub.get("balance") or sub.get("COLLATERAL"))
                        if bal is not None:
                            break
        elif isinstance(raw, (int, float, str)):
            bal = self._parse_usdc_amount(raw)
        # Max-uint allowances are "unlimited" — don't show scientific notation in UI
        if allow is not None and allow > 1e12:
            allow = None
        return {
            "ok": bal is not None,
            "balance_usd": bal,
            "allowance_usd": allow,
            "funder": config.FUNDER_ADDRESS,
            "signature_type": config.SIGNATURE_TYPE,
            "raw": raw,
            "error": None if bal is not None else "Could not parse balance response",
        }

    def buy(self, token_id: str, price: float, size: float, condition_id: str,
            order_type: str = "GTC"):
        """Post a limit buy at `price` (never a market order).

        Limit orders cap slippage: we only pay up to `price` (best ask at
        decision time, rounded to tick). Market orders are intentionally
        avoided — they can fill far above fair value in thin 5m books.

        ``order_type``:
          GTC  rests on the book until filled or cancelled (the default).
          FOK  fills the WHOLE size immediately or is killed. This is what
               makes a two-leg arb safe: a partial fill on one leg leaves a
               naked directional position, which is exactly the leg risk that
               kept arb paper-only.
          FAK  fills what it can immediately and kills the rest (used to
               unwind a stranded leg).

        Size is in shares. Resting leftovers are cancelled at slot end.
        """
        from py_clob_client_v2 import (OrderArgs, OrderType,
                                       PartialCreateOrderOptions)
        from py_clob_client_v2.order_builder.constants import BUY

        ot = getattr(OrderType, str(order_type).upper(), OrderType.GTC)

        def _post(c):
            market = c.get_market(condition_id)
            opts = PartialCreateOrderOptions(
                tick_size=str(market["minimum_tick_size"]),
                neg_risk=market["neg_risk"],
            )
            # Round price down to tick so we never overpay past the displayed ask
            # more than one tick (taker-friendly but bounded).
            px = float(price)
            try:
                tick = float(market["minimum_tick_size"])
            except Exception:
                tick = 0.01
            if tick > 0:
                px = max(tick, round(round(px / tick) * tick, 6))
            # Limit — NOT market. Protects against adverse fill/slippage.
            return c.create_and_post_order(
                OrderArgs(token_id=token_id, price=px, size=float(size),
                          side=BUY),
                options=opts, order_type=ot,
            )

        c = self._client()
        try:
            return _post(c)
        except Exception as e:
            msg = str(e).lower()
            if "signer address" in msg and "api key" in msg:
                # Stale cache or wrong signature_type — wipe and rebuild once
                print("  [auth] signer/API-key mismatch - clearing cache and "
                      "re-authenticating once...")
                try:
                    import os
                    path = self._creds_path()
                    if os.path.isfile(path):
                        os.remove(path)
                except Exception:
                    pass
                self.reset()
                c2 = self._client()
                return _post(c2)
            raise

    def sell(self, token_id: str, price: float, size: float, condition_id: str,
             order_type: str = "FAK"):
        """Post a limit SELL — used to unwind a stranded arb leg.

        Defaults to FAK: take whatever liquidity is there right now and kill
        the rest. A resting sell is no use here, because the position being
        unwound is naked directional in a market that resolves in minutes;
        getting most of it off immediately beats getting all of it off later.
        """
        from py_clob_client_v2 import (OrderArgs, OrderType,
                                       PartialCreateOrderOptions)
        from py_clob_client_v2.order_builder.constants import SELL

        ot = getattr(OrderType, str(order_type).upper(), OrderType.FAK)

        def _post(c):
            market = c.get_market(condition_id)
            opts = PartialCreateOrderOptions(
                tick_size=str(market["minimum_tick_size"]),
                neg_risk=market["neg_risk"],
            )
            px = float(price)
            try:
                tick = float(market["minimum_tick_size"])
            except Exception:
                tick = 0.01
            if tick > 0:
                px = max(tick, round(round(px / tick) * tick, 6))
            return c.create_and_post_order(
                OrderArgs(token_id=token_id, price=px, size=float(size),
                          side=SELL),
                options=opts, order_type=ot,
            )

        return _post(self._client())

    def cancel_all(self):
        """Cancel open orders so a missed fill doesn't rest into the next slot."""
        c = self._client()
        for name in ("cancel_all", "cancel_market_orders", "cancel_orders"):
            fn = getattr(c, name, None)
            if callable(fn):
                try:
                    return fn()
                except TypeError:
                    try:
                        return fn(None)
                    except Exception:
                        continue
                except Exception as e:
                    print(f"  cancel via {name} failed: {e}")
        return None

    def cancel_order(self, order_id: str):
        c = self._client()
        try:
            return c.cancel_orders([order_id])
        except Exception as e:
            print(f"  cancel_orders failed: {e}")
        for name in ("cancel", "cancel_order"):
            fn = getattr(c, name, None)
            if callable(fn):
                try:
                    return fn(order_id)
                except Exception as e:
                    print(f"  cancel_order failed: {e}")
        return None

