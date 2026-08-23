"""Central configuration for the Polymarket BTC 5-minute up/down bot.

All live-trading secrets come from environment variables (.env). Nothing
sensitive is hard-coded. See .env.example.

MODES
-----
- paper       : offline random-walk data + simulated fills (no network, no money)
- paper_live  : REAL BTC prices + REAL Polymarket order-book quotes, but
                simulated fills (no wallet/private key needed, no money at risk)
- live        : real data + REAL orders on Polymarket (needs funded wallet)

The target market's token IDs are discovered from the Gamma API at the start of
every round, so the bot always trades the current 5-minute slot automatically.

LIVE SAFETY
-----------
Mode=live automatically applies a conservative risk profile (tiny fixed stakes,
strict edge/EV, Chainlink-first prices, trade-window limits) unless you set
LIVE_AGGRESSIVE=1. You must also set LIVE_CONFIRM=yes.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # python-dotenv optional for the offline simulator
    pass


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# --- Mode ---
MODE = os.getenv("MODE", "paper")  # paper | paper_live | live
# Strategy:
#   mid         = mid-slot value band (default; edge vs strike + model)
#   late_scalp  = selective late favorites (stricter gates)
#   late_every  = late scalp EVERY 5m slot (relaxed; more activity/risk)
#   hybrid      = late preferred; mid only if exceptional, else late same slot
STRATEGY = (os.getenv("STRATEGY") or "mid").strip().lower()

# --- Polymarket / chain ---
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon mainnet
# Authenticated CLOB httpx timeout (py-clob-client-v2 default is too tight).
CLOB_HTTP_TIMEOUT = float(os.getenv("CLOB_HTTP_TIMEOUT", "30"))
# Min seconds between update_balance_allowance calls (dashboard polls often).
CLOB_BALANCE_REFRESH_S = float(os.getenv("CLOB_BALANCE_REFRESH_S", "60"))
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS")
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "3"))  # 0=EOA, 1=POLY_PROXY, 2=Safe, 3=deposit/EIP-1271
# Optional: pre-created CLOB API creds (skips create/derive if all three set).
POLY_API_KEY = (os.getenv("POLY_API_KEY") or "").strip() or None
POLY_API_SECRET = (os.getenv("POLY_API_SECRET") or "").strip() or None
POLY_API_PASSPHRASE = (os.getenv("POLY_API_PASSPHRASE") or "").strip() or None
# Hard gate for real-money mode: must be "yes" in .env
LIVE_CONFIRM = (os.getenv("LIVE_CONFIRM") or "").strip().lower()
# If 1, skip auto-tightening of live risk defaults (not recommended).
LIVE_AGGRESSIVE = _env_bool("LIVE_AGGRESSIVE", False)

# --- Market discovery (deterministic) ---
# Polymarket's 5-minute BTC markets are events whose slug encodes the slot's
# Unix start time, e.g. "btc-updown-5m-1771582500" (always a multiple of 300).
# The bot computes the current slot as floor(now/SLOT_SECONDS)*SLOT_SECONDS and
# fetches that event by slug. Override the prefix only if Polymarket renames it.
ASSET = os.getenv("ASSET", "btc").strip().lower()

# Which recurring series to trade. Verified live 2026-07-19: only 5m, 15m
# and 4h exist (1h / 30m / 10m / 3m / 1m do NOT). Longer series are far
# thinner: 5m ~$81k/24h vs 15m ~$23k, so expect worse fills on 15m.
# The late-window logic itself transfers cleanly between series — the fair
# value model keys off REMAINING seconds, not window length.
TIMEFRAME = (os.getenv("TIMEFRAME", "5m") or "5m").strip().lower()
_TF_SECONDS = {"5m": 300, "15m": 900, "4h": 14400}
if TIMEFRAME not in _TF_SECONDS:
    print(f"[config] unknown TIMEFRAME '{TIMEFRAME}' -> 5m")
    TIMEFRAME = "5m"
SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", str(_TF_SECONDS[TIMEFRAME])))

BTC_SLUG_PREFIX = ""
BTC_MARKET_ASSET_TERMS = []
BTC_MARKET_UPDOWN_TERMS = []
BTC_MARKET_MATCH = ""
BTC_PRICE_SOURCES = []

def reapply_asset_config():
    global BTC_SLUG_PREFIX, BTC_MARKET_ASSET_TERMS, BTC_MARKET_UPDOWN_TERMS, BTC_MARKET_MATCH, BTC_PRICE_SOURCES
    
    # Target slug prefix mapped to ASSET *and* TIMEFRAME
    BTC_SLUG_PREFIX = (os.getenv("BTC_SLUG_PREFIX")
                       or f"{ASSET}-updown-{TIMEFRAME}-")
    
    # Asset terms dynamically mapped
    default_asset_terms = f"{ASSET},bitcoin" if ASSET == "btc" else (f"{ASSET},ethereum" if ASSET == "eth" else f"{ASSET},solana")
    BTC_MARKET_ASSET_TERMS = [s.strip().lower() for s in os.getenv(
        "BTC_MARKET_ASSET_TERMS", default_asset_terms).split(",") if s.strip()]
        
    BTC_MARKET_UPDOWN_TERMS = [s.strip().lower() for s in os.getenv(
        "BTC_MARKET_UPDOWN_TERMS",
        "up or down,up-or-down,updown,5 min,5-min,5min,next 5").split(",") if s.strip()]
        
    BTC_MARKET_MATCH = os.getenv("BTC_MARKET_MATCH", "").strip().lower()
    
    BTC_PRICE_SOURCES = [s.strip() for s in os.getenv(
        "BTC_PRICE_SOURCES",
        "chainlink_stream,chainlink,pyth,coinbase,binance,kraken").split(",") if s.strip()]

reapply_asset_config()

# --- Market discovery (fuzzy fallback only) ---
SLOT_MIN_SECONDS = int(os.getenv("SLOT_MIN_SECONDS", "180"))
SLOT_MAX_SECONDS = int(os.getenv("SLOT_MAX_SECONDS", "420"))
# Skip slots with too little time left (need room to decide + place).
MIN_SLOT_SECONDS = int(os.getenv("MIN_SLOT_SECONDS", "90"))
# Also skip if TOO much time left (opening books are often empty/noisy).
# Trade only when remaining is in [MIN_SLOT_SECONDS, MAX_TRADE_REMAINING].
MAX_TRADE_REMAINING = int(os.getenv("MAX_TRADE_REMAINING", "240"))
# Discovery: only prefer the *next* slot when current remaining is below this.
# Must stay LOW (≤ late window) or hybrid never reaches late_scalp (was wrongly
# tied to MIN_SLOT_SECONDS=90, which abandoned slots ~100s early).
MARKET_ADVANCE_REMAINING = int(os.getenv("MARKET_ADVANCE_REMAINING", "5"))


# --- Strategy (stricter mid defaults — fewer, higher-conviction mid entries) ---
# Must follow the series length: it caps how long the loop may sleep waiting
# for a slot to end. Hardcoded 300 made a 15m instance wake ~600s early every
# slot and re-poll discovery pointlessly.
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", str(SLOT_SECONDS)))
# Chop is measured over a FIXED recent window, not the whole slot: crossing
# counts grow with elapsed time, so a 15m slot would show far more crossings
# than a 5m one and the same MAX_CHOP threshold would mean something
# different on each series. 300s = identical behaviour on 5m (no regression).
CHOP_LOOKBACK_S = float(os.getenv("CHOP_LOOKBACK_S", "300"))
MOMENTUM_LOOKBACK = int(os.getenv("MOMENTUM_LOOKBACK", "12"))
SAMPLE_SECONDS = float(os.getenv("SAMPLE_SECONDS", "5"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.04"))
MIN_EV = float(os.getenv("MIN_EV", "0.02"))
VOL_BLEND = float(os.getenv("VOL_BLEND", "0.5"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.08"))
# Assumed / buffer for fees + adverse selection (subtracted from model edge).
FEE_BUFFER = float(os.getenv("FEE_BUFFER", "0.01"))
# Only buy contracts in this ask band (avoid expensive favorites / lottery tickets).
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "0.22"))
MAX_ENTRY_PRICE = float(os.getenv("MAX_ENTRY_PRICE", "0.72"))
# Require signal confidence and a clear move vs strike (in $) before trading.
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.40"))
MIN_MOVE_USD = float(os.getenv("MIN_MOVE_USD", "5.0"))  # |spot - strike|
# Minimum size sitting on the best ask (shares); 0 = disabled (paper-friendly).
MIN_ASK_SIZE = float(os.getenv("MIN_ASK_SIZE", "0"))

# Mid-only quality guards (STRATEGY=mid). Archive mid was ~44% WR / flat PnL;
# skip overconfident cheap tickets and soft edges.
MID_MIN_MODEL = float(os.getenv("MID_MIN_MODEL", "0.52"))
MID_MAX_MODEL = float(os.getenv("MID_MAX_MODEL", "0.95"))
# If ask is this cheap AND model above this -> skip (0 / 1 = disabled).
MID_CHEAP_ASK = float(os.getenv("MID_CHEAP_ASK", "0.0"))
MID_CHEAP_MAX_MODEL = float(os.getenv("MID_CHEAP_MAX_MODEL", "1.0"))
MID_REQUOTE = _env_bool("MID_REQUOTE", True)
MID_REQUOTE_SLEEP_S = float(os.getenv("MID_REQUOTE_SLEEP_S", "0.40"))
MID_MAX_ASK_JUMP = float(os.getenv("MID_MAX_ASK_JUMP", "0.02"))
MID_PAUSE_SLOTS_AFTER_LOSS = int(os.getenv("MID_PAUSE_SLOTS_AFTER_LOSS", "1"))
MID_MAX_STAKE_USD = float(os.getenv("MID_MAX_STAKE_USD", "4.0"))
# Fair-value: used for edge sizing/logging. Noise floor off when CAP=0.
MID_USE_FAIR_VALUE = _env_bool("MID_USE_FAIR_VALUE", True)
MID_Z_MIN = float(os.getenv("MID_Z_MIN", "0.15"))
# Cap on Z*sigma floor (USD). 0 = do not raise min move above MIN_MOVE_USD.
MID_NOISE_MOVE_CAP = float(os.getenv("MID_NOISE_MOVE_CAP", "0"))
MID_REQUIRE_SIGNIFICANT = _env_bool("MID_REQUIRE_SIGNIFICANT", False)
MID_MAX_CHOP_CROSSES = int(os.getenv("MID_MAX_CHOP_CROSSES", "6"))
# Paper default: allow proxy strike (Chainlink open often missing if bot joins late)
MID_REQUIRE_CHAINLINK_STRIKE = _env_bool("MID_REQUIRE_CHAINLINK_STRIKE", False)
# Pessimistic paper fills: add this to ask for simulated entry (Tier 1 realism).
PAPER_FILL_SLIPPAGE = float(os.getenv("PAPER_FILL_SLIPPAGE", "0.01"))
# Book quality (Tier 2)
MID_MIN_BID_ASK_RATIO = float(os.getenv("MID_MIN_BID_ASK_RATIO", "0.0"))  # 0=off
# Both-side arb scanner: log when ask_UP + ask_DOWN < 1 - fee (Tier 2)
ARB_SCAN_ENABLED = _env_bool("ARB_SCAN_ENABLED", True)
ARB_MIN_EDGE = float(os.getenv("ARB_MIN_EDGE", "0.02"))  # 1 - sum asks
# Shadow journal: log every mid decision even when not trading
DECISION_JOURNAL = _env_bool("DECISION_JOURNAL", True)

# --- Late-scalp strategy (STRATEGY=late_scalp / hybrid late path) ---
# Tighter defaults: harder to lose (stronger favorites, later window) + better
# payoff than ultra-rich 0.97+ asks. R13-style 0.89 losses are filtered out.
# Enter only when remaining seconds in [LATE_SCALP_MIN_REMAINING, LATE_SCALP_MAX_REMAINING]
# and the favorite ask is in [LATE_SCALP_MIN_ENTRY, LATE_SCALP_MAX_ENTRY].
LATE_SCALP_MIN_REMAINING = int(os.getenv("LATE_SCALP_MIN_REMAINING", "8"))
LATE_SCALP_MAX_REMAINING = int(os.getenv("LATE_SCALP_MAX_REMAINING", "50"))
# Default band wide enough for every-slot mode (clamp if needed).
# Min favorite ask — below this is near coin-flip (R5 lost @ 0.57).
LATE_SCALP_MIN_ENTRY = float(os.getenv("LATE_SCALP_MIN_ENTRY", "0.88"))
LATE_SCALP_MAX_ENTRY = float(os.getenv("LATE_SCALP_MAX_ENTRY", "0.93"))
# Prefer entries at/under this ask (ignored when LATE_SCALP_EVERY_SLOT=1).
LATE_SCALP_PREFER_MAX = float(os.getenv("LATE_SCALP_PREFER_MAX", "0.93"))
# Minimum gap between favorite ask and underdog ask (favorite clearly leading).
LATE_SCALP_MIN_FAVORITE_LEAD = float(os.getenv("LATE_SCALP_MIN_FAVORITE_LEAD", "0.05"))
# Underdog ask must be this low (0 = off in every-slot).
LATE_SCALP_MAX_UNDERDOG_ASK = float(os.getenv("LATE_SCALP_MAX_UNDERDOG_ASK", "0.42"))
# Require spot vs strike to agree with the favorite side (skip book/spot fights).
LATE_SCALP_REQUIRE_SPOT_ALIGN = _env_bool("LATE_SCALP_REQUIRE_SPOT_ALIGN", False)
LATE_SCALP_SPOT_ALIGN_EPS_USD = float(os.getenv("LATE_SCALP_SPOT_ALIGN_EPS_USD", "8.0"))
# Re-check book after a short pause; skip if favorite flips or ask jumps.
LATE_SCALP_REQUOTE = _env_bool("LATE_SCALP_REQUOTE", False)
LATE_SCALP_REQUOTE_SLEEP_S = float(os.getenv("LATE_SCALP_REQUOTE_SLEEP_S", "0.25"))
LATE_SCALP_MAX_ASK_JUMP = float(os.getenv("LATE_SCALP_MAX_ASK_JUMP", "0.10"))
# If requote spikes higher, keep original limit when it is still in the sweet band.
LATE_SCALP_KEEP_FIRST_ASK = _env_bool("LATE_SCALP_KEEP_FIRST_ASK", True)
# After a late loss, skip this many subsequent late windows (cooldown).
LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS = int(os.getenv("LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", "0"))
# Cap stake even after min-share bump (5 * 0.99 = 4.95).
LATE_SCALP_MAX_STAKE_USD = float(os.getenv("LATE_SCALP_MAX_STAKE_USD", "5.00"))
# Prefer slightly wider late max-spread than mid (books thin near end).
LATE_SCALP_MAX_SPREAD = float(os.getenv("LATE_SCALP_MAX_SPREAD", "0.15"))
# 1 = try to place a late trade every 5m slot (relaxed gates; more risk).
LATE_SCALP_EVERY_SLOT = _env_bool("LATE_SCALP_EVERY_SLOT", True)

# --- Late-scalp v2 improvements (smart filters) ---
# Minimum |spot − strike| move in USD before entering. Skip coin-flip slots.
LATE_SCALP_MIN_MOVE_USD = float(os.getenv("LATE_SCALP_MIN_MOVE_USD", "8.0"))
# Require Chainlink Data Stream tick for strike (refuse exchange-proxy strikes).
LATE_SCALP_REQUIRE_CHAINLINK = _env_bool("LATE_SCALP_REQUIRE_CHAINLINK", True)
# Minimum model edge (model_prob − market_ask) to enter. 0 = off (legacy).
LATE_SCALP_MIN_EDGE = float(os.getenv("LATE_SCALP_MIN_EDGE", "0.02"))
# Enable edge-scaled/Kelly sizing for late-scalping.
LATE_SCALP_USE_EDGE_SIZING = _env_bool("LATE_SCALP_USE_EDGE_SIZING", True)

# --- Dedicated Terminal Diffusion Momentum Engine (H-001 - H-005) ---
STRATEGY = "diffusion_momentum"
# H-001: Standardized diffusion Z-score hurdle: Z = |spot - strike| / sigma_tau
DIFFUSION_Z_MIN = float(os.getenv("DIFFUSION_Z_MIN", "1.80"))
# Breakout Z threshold where chop no longer degrades directional hit rate
DIFFUSION_Z_BREAKOUT = float(os.getenv("DIFFUSION_Z_BREAKOUT", "2.20"))
# H-002: Max strike crossings allowed when Z is marginal (DIFFUSION_Z_MIN <= Z < DIFFUSION_Z_BREAKOUT)
DIFFUSION_MAX_CHOP_MARGINAL = int(os.getenv("DIFFUSION_MAX_CHOP_MARGINAL", "3"))
# Calibrated logistic S-curve steepness alpha for P(Z) = 1 / (1 + exp(-alpha * Z))
DIFFUSION_ALPHA = float(os.getenv("DIFFUSION_ALPHA", "1.75"))
# Floor on terminal standard deviation in USD over tau to prevent division by zero
DIFFUSION_MIN_SIGMA_USD = float(os.getenv("DIFFUSION_MIN_SIGMA_USD", "2.50"))
# Terminal execution window (remaining seconds before slot resolution)
TERMINAL_WINDOW_MAX_S = int(os.getenv("TERMINAL_WINDOW_MAX_S", "20"))
TERMINAL_WINDOW_MIN_S = int(os.getenv("TERMINAL_WINDOW_MIN_S", "5"))
# H-003: Dynamic stake multiplier based on previous slot (T-1) displacement
MOMENTUM_USE_REGIME_SIZING = _env_bool("MOMENTUM_USE_REGIME_SIZING", True)
# H-005: Require Chainlink RTDS tick for strike (reject proxy strikes to prevent settlement skew)
MOMENTUM_REQUIRE_VERIFIED_STRIKE = _env_bool("MOMENTUM_REQUIRE_VERIFIED_STRIKE", True)
# Momentum trading enable toggle
MOMENTUM_TRADE_ENABLED = _env_bool("MOMENTUM_TRADE_ENABLED", True)
MOMENTUM_OBSERVE_ONLY = _env_bool("MOMENTUM_OBSERVE_ONLY", False)

# Entry price band: [0.50, 0.99]
LATE_MOMENTUM_MIN_ENTRY = float(os.getenv("LATE_MOMENTUM_MIN_ENTRY", "0.50"))
LATE_MOMENTUM_MAX_ENTRY = float(os.getenv("LATE_MOMENTUM_MAX_ENTRY", "0.99"))
LATE_MOMENTUM_MIN_EDGE = float(os.getenv("LATE_MOMENTUM_MIN_EDGE", "0.00"))
LATE_MOMENTUM_MAX_EDGE = None  # No ceiling: aggressively capture large mispricings on volatile moves
# Evaluate and record momentum slots, but place no momentum trades.
#
# The tuner learns from counterfactuals: every evaluated slot carries an ask,
# a model edge and the outcome that actually occurred, so a parameter set can
# be scored over slots the bot never took. Real trades add fill realism and
# nothing else to the learning. That makes trading a strategy with no
# demonstrated edge purely a cost.
#
# DEFAULT ON as of 2026-08-11. Momentum is retired from the trade path and kept
# as a research stream only. The evidence, from this bot's own data:
#
#   * 41 venue-confirmed trades: -$16.57, 75.6% against an 81.1% breakeven
#   * across 740+ priced, labeled slots the book prices the signal correctly —
#     win rate matches the ask to within ~1pp BEFORE fees, in every move bucket
#   * no entry band is significantly positive net of fees; the largest (0.95+,
#     n=621) is +0.15pp at t=+0.52, i.e. nothing
#   * the tuner has never found a configuration clearing its guardrails, and
#     with under 1% of slots qualifying it is structurally starved — which is
#     the same finding, not a separate problem
#
# A taker on an efficiently-priced signal simply pays the fee. There is nothing
# to tune, so trading it is a cost with no expected return. Observation costs
# nothing and keeps the counterfactual dataset growing at full rate, which is
# what would let the tuner change this verdict if the market ever changes.
#
# Set MOMENTUM_OBSERVE_ONLY=0 to trade it again — but only on the back of a
# validated tuner result, not a hunch.
MOMENTUM_OBSERVE_ONLY = _env_bool("MOMENTUM_OBSERVE_ONLY", True)

# --- Sum-to-one arbitrage (runs alongside late_momentum) ---
# When ask_UP + ask_DOWN < 1 − ARB_MIN_EDGE, buying BOTH sides pays $1/share
# whichever way the slot resolves — structural edge, no prediction needed.
# Execution is PAPER-ONLY: live needs both legs to fill (leg risk unhandled).
ARB_TRADE_ENABLED = _env_bool("ARB_TRADE_ENABLED", True)
ARB_MAX_STAKE_USD = float(os.getenv("ARB_MAX_STAKE_USD", "10.0"))
# Minimum LOCKED profit per pair after simulated slippage AND taker fees
# (real fees eat razor-thin pairs; don't let paper count them).
ARB_MIN_LOCKED_EDGE = float(os.getenv("ARB_MIN_LOCKED_EDGE", "0.02"))
# Raw edges above this are makers pulling quotes (dust levels), not fillable
# arbs — skip as too-good-to-be-true. 0 = off.
ARB_MAX_EDGE = float(os.getenv("ARB_MAX_EDGE", "0.15"))
# Only scan when at least this many seconds remain (books settle, time to fill).
ARB_MIN_REMAINING = int(os.getenv("ARB_MIN_REMAINING", "20"))
# Execute arbs with real money. OFF by default and independent of MODE=live,
# so live momentum and live arb are separate decisions.
#
# Arb is the only strategy here with a demonstrated edge (15/15, +4.57% ROI,
# t=+5.58) and the only one that predicts nothing. What is NOT yet known is the
# real second-leg fill rate: paper assumes both legs fill, so it reports a
# perfect lock rate by construction and cannot measure this. Turning this on is
# how that number gets measured — treat the first few hundred attempts as
# buying information at the venue minimum, not as an investment.
# arb_executor.stats() reports the lock rate as it accumulates.
ARB_LIVE_ENABLED = _env_bool("ARB_LIVE_ENABLED", False)
# Total USD that may sit in open arb PAIRS at once. Separate from
# MAX_EXPOSURE_USD, which sizes a single directional leg and drops to $1 under
# the live profile — the cheapest legal pair is 5 shares a side, roughly $4.50,
# so sharing that cap silently rejected every live arb. A hedged pair cannot
# lose the way a naked leg can, so it gets its own limit; two concurrent pairs
# at the live stake cap is the intent.
ARB_MAX_EXPOSURE_USD = float(os.getenv("ARB_MAX_EXPOSURE_USD", "20.0"))

# Venue maintenance blackouts: "HH:MM-HH:MM" UTC windows (comma separated) in
# which NO live order may be sent.
#
# This is not caution for its own sake. Polymarket's maintenance notice says
# users may get HTTP 503 when placing OR cancelling orders, and when viewing
# order status. The placing failure is harmless here — a killed first leg costs
# nothing. The dangerous one is the combination: leg one fills, leg two 503s,
# and the unwind 503s too. That is the single path in this design that leaves a
# naked directional position. Worse, with order status also unavailable,
# _verify_filled cannot tell us what we are holding, so we would not even know.
#
# Paper is unaffected — it sends nothing, so it keeps collecting data.
LIVE_BLACKOUT_WINDOWS_UTC = (os.getenv("LIVE_BLACKOUT_WINDOWS_UTC", "") or "").strip()


def in_live_blackout(now=None) -> tuple[bool, str]:
    """Is `now` inside a configured no-live-orders window? -> (bool, label)."""
    import datetime as _dt
    if not LIVE_BLACKOUT_WINDOWS_UTC:
        return False, ""
    t = (now or _dt.datetime.now(_dt.timezone.utc))
    if t.tzinfo is None:
        t = t.replace(tzinfo=_dt.timezone.utc)
    mins = t.hour * 60 + t.minute
    for win in LIVE_BLACKOUT_WINDOWS_UTC.split(","):
        win = win.strip()
        if not win or "-" not in win:
            continue
        try:
            a, b = win.split("-", 1)
            ah, am = (int(x) for x in a.strip().split(":"))
            bh, bm = (int(x) for x in b.strip().split(":"))
        except (ValueError, TypeError):
            continue
        start, end = ah * 60 + am, bh * 60 + bm
        # A window may wrap midnight (e.g. 23:30-00:30).
        inside = (start <= mins < end) if start <= end else (mins >= start or mins < end)
        if inside:
            return True, win
    return False, ""

# Pushed order book over the venue's market WebSocket. Falls back to REST per
# scan when the socket is down or the book is stale, so an outage degrades the
# bot instead of stopping it.
BOOK_FEED_ENABLED = _env_bool("BOOK_FEED_ENABLED", True)
# Refuse to price an arb from a book older than this. Quotes move in
# milliseconds on these markets and the 500ms taker buffer was removed in
# Feb 2026, so a two-second-old book is already a guess.
BOOK_FEED_MAX_AGE_S = float(os.getenv("BOOK_FEED_MAX_AGE_S", "2.0"))
# How often to re-check for an arb while waiting out a slot. This was 15s,
# which on a market that moves in milliseconds meant nearly every gap was
# missed and the ones caught were stale by construction. With a pushed book the
# check is a cheap in-memory read, so it can run far more often.
ARB_SCAN_INTERVAL_S = float(os.getenv("ARB_SCAN_INTERVAL_S", "1.0"))
# Only trade a pair both of whose legs were priced from ONE instant.
#
# The REST fallback reads UP and then DOWN in separate round trips. The price
# moves between them, and when it moves the right way the two quotes describe a
# pair that never simultaneously existed — a gap that looks exactly like an arb
# and is not one. This is not theoretical: across the whole logged history,
# 236 of 236 sum-to-one signals came from REST and ZERO from the socket, while
# the socket did the large majority of the scanning. The book is tight; the
# "opportunities" were our own measurement error.
#
# The fallback still runs, because knowing the socket is down is worth having.
# It just may not authorise a trade. Turning this off re-enables trading on
# quotes that cannot be trusted to have coexisted.
ARB_REQUIRE_ATOMIC_BOOK = _env_bool("ARB_REQUIRE_ATOMIC_BOOK", True)

# How many timestamped backup folders to keep under archive/copies/ (0 = none).
# Each one holds a full copy of the trade history, written on every settled
# trade, so leaving this unbounded grows the volume quadratically — it filled
# the Railway disk and crash-looped the bot. The master JSONL/CSV are the real
# archive; these are only a convenience.
ARCHIVE_COPIES_KEEP = int(os.getenv("ARCHIVE_COPIES_KEEP", "3"))

# --- Hybrid (STRATEGY=hybrid): prefer late; mid only if exceptional ---
# When true (default), mid must clear HYBRID_MID_* bars on top of normal mid filters;
# otherwise the slot is queued for late_scalp. Set HYBRID_PREFER_LATE=0 for old
# mid-first behavior (still uses normal EDGE_THRESHOLD etc. for mid).
HYBRID_PREFER_LATE = _env_bool("HYBRID_PREFER_LATE", True)
HYBRID_MID_MIN_EDGE = float(os.getenv("HYBRID_MID_MIN_EDGE", "0.10"))
HYBRID_MID_MIN_MOVE_USD = float(os.getenv("HYBRID_MID_MIN_MOVE_USD", "15.0"))
HYBRID_MID_MIN_MODEL = float(os.getenv("HYBRID_MID_MIN_MODEL", "0.65"))
HYBRID_MID_MIN_CONFIDENCE = float(os.getenv("HYBRID_MID_MIN_CONFIDENCE", "0.55"))

# CLOB limit orders usually need ~5 shares. Stake is bumped to max(bet, shares*price).
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5"))
ENFORCE_MIN_SHARES = _env_bool("ENFORCE_MIN_SHARES", True)
# Buy the venue minimum (orderMinSize shares) rather than a fixed dollar stake.
#
# A fixed $5 stake buys 8.5 shares at a 0.59 ask and 5.6 at 0.87 — so the cost
# of a trade is constant while the position size swings with price. Sizing at
# exactly the minimum inverts that: every trade is the smallest order the venue
# will accept, costing shares*ask (e.g. $2.95 at 0.59, $4.35 at 0.87).
#
# The point is trades-per-dollar, not edge. PnL scales linearly with size, so
# this shrinks wins as well as losses — it does NOT make a losing strategy
# profitable. What it buys is runway: on the first 28 live trades it would have
# deployed 18% less capital for the same 28 labeled outcomes. While the tuner
# is still gathering data, maximising labeled trades per dollar of bankroll is
# the right trade-off. Turn off once the strategy is proven and you want size.
MIN_SHARES_SIZING = _env_bool("MIN_SHARES_SIZING", True)

# --- Risk controls ---
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "100.0"))
BASE_TRADE_USD = float(os.getenv("BASE_TRADE_USD", "1.0"))
MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "3.0"))
MAX_EXPOSURE_USD = float(os.getenv("MAX_EXPOSURE_USD", "3.0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "5.0"))
# Halt at the TIGHTER of absolute $ and this % of starting bankroll (0 = ignore %).
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.05"))
# 0 = unlimited (paper). Live safety profile forces a small cap.
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "0"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.10"))
# If > 0, every qualifying trade uses exactly this USD stake (live profile sets $1).
FIXED_STAKE_USD = float(os.getenv("FIXED_STAKE_USD", "0"))

MIN_QUOTE = float(os.getenv("MIN_QUOTE", "0.02"))
MAX_QUOTE = float(os.getenv("MAX_QUOTE", "0.98"))

# --- Maker-first execution (verified: feeSchedule.takerOnly=True) ---
# Takers pay rate*min(p,1-p)/share; MAKERS PAY ZERO and additionally receive
# a share of the taker-fee pool (rebateRate 0.2). On this venue the fee is
# several times the typical 1c spread, so the prize for resting an order is
# not spread capture — it is fee avoidance.
# Flow: post 1 tick under the ask, wait briefly; if the market trades down to
# us we filled as a maker (no fee). If not, fall back to crossing at the ask
# exactly as before — so this can only improve the entry, never lose a trade.
MAKER_FIRST_ENABLED = _env_bool("MAKER_FIRST_ENABLED", True)
# Ticks below the best ask to rest at (1 tick = 0.01 here).
MAKER_IMPROVE_TICKS = int(os.getenv("MAKER_IMPROVE_TICKS", "1"))
# Seconds to wait for the market to come to our price.
MAKER_WAIT_SECONDS = float(os.getenv("MAKER_WAIT_SECONDS", "3.0"))
# Never start a maker attempt without this much slot time left afterwards.
MAKER_MIN_REMAINING = float(os.getenv("MAKER_MIN_REMAINING", "6.0"))
# Live maker-first is OFF by default: it needs cancel/fill race handling that
# has not been validated against the real exchange. Paper proves it first.
MAKER_FIRST_LIVE = _env_bool("MAKER_FIRST_LIVE", False)
# Maker rebates are a proportional daily pool — real, but NOT credited to
# PnL here. Counting speculative rebates would be the same optimistic
# accounting the fee bug already taught us to avoid.

# --- Trading fees (verified live 2026-07-18 via Gamma feeSchedule) ---
# Polymarket 5m crypto markets charge TAKERS: rate * min(p, 1-p) per share.
# rate=0.07 → ~3.5c/share at p=0.5, ~1c/share at p=0.85. Makers get rebates.
# The per-market rate from Gamma overrides this fallback when available.
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.07"))


def taker_fee_per_share(price: float, rate: float | None = None,
                        exponent: float = 1.0) -> float:
    """Fee in $ per share for a taker fill at `price`.

    Polymarket's documented formula is

        fee = shares * rate * price * (1 - price)

    which the venue's own feeSchedule corroborates: {"rate": 0.07,
    "exponent": 1, "takerOnly": true} — exponent 1 meaning price^1 * (1-p)^1.
    Their worked example is 100 shares at $0.50 on a crypto market = $1.75,
    and 100 * 0.07 * 0.5 * 0.5 reproduces it exactly.

    This previously used `rate * min(p, 1-p)`, a different curve that charged
    DOUBLE the real fee at 50/50 and ~15% too much at 0.87. It was wrong in the
    conservative direction — trades were rejected as unprofitable when they were
    not, and booked PnL was understated — but wrong is wrong: an arb gate is
    only as good as the cost model inside it.
    """
    r = TAKER_FEE_RATE if rate is None else float(rate)
    p = min(max(float(price), 0.0), 1.0)
    if exponent == 1.0:
        return r * p * (1.0 - p)
    return r * (p ** exponent) * ((1.0 - p) ** exponent)


# Makers pay no taker fee AND receive a share of taker fees back as a daily
# USDC rebate (crypto: 20%, per the venue feeSchedule's rebateRate). Not
# credited to PnL anywhere — it is a proportional daily pool, not a guaranteed
# per-trade payment, so counting it would inflate results.
MAKER_REBATE_SHARE = float(os.getenv("MAKER_REBATE_SHARE", "0.20"))

VOLATILITY_DAILY = float(os.getenv("VOLATILITY_DAILY", "0.045"))


def apply_live_safety_profile() -> list[str]:
    """Tighten risk for MODE=live. Returns list of applied overrides for logging.

    Does nothing if LIVE_AGGRESSIVE=1. Env values that are already stricter than
    the profile floor are kept (we only tighten, never loosen).
    """
    if LIVE_AGGRESSIVE:
        return ["LIVE_AGGRESSIVE=1 (profile skipped)"]

    notes = []
    global EDGE_THRESHOLD, MIN_EV, MAX_SPREAD, FEE_BUFFER
    global MIN_ENTRY_PRICE, MAX_ENTRY_PRICE, MIN_CONFIDENCE, MIN_MOVE_USD
    global BASE_TRADE_USD, MAX_TRADE_USD, MAX_EXPOSURE_USD, FIXED_STAKE_USD
    global ARB_MAX_STAKE_USD, ARB_MAX_EXPOSURE_USD
    global DAILY_LOSS_LIMIT_USD, DAILY_LOSS_LIMIT_PCT, MAX_TRADES_PER_DAY
    global MAX_CONSECUTIVE_LOSSES, KELLY_FRACTION, BTC_PRICE_SOURCES
    global MIN_SLOT_SECONDS, MAX_TRADE_REMAINING, MIN_ASK_SIZE

    def tighten_max(name, current, ceiling):
        """Values where lower is safer (trade size, loss limit, etc.)."""
        if current > ceiling:
            notes.append(f"{name}: {current} -> {ceiling}")
            return ceiling
        return current

    def tighten_min(name, current, floor):
        """Values where higher is safer (edge, min confidence, etc.)."""
        if current < floor:
            notes.append(f"{name}: {current} -> {floor}")
            return floor
        return current

    # Live mid floors align with stricter mid defaults (hybrid late path separate).
    EDGE_THRESHOLD = tighten_min("EDGE_THRESHOLD", EDGE_THRESHOLD, 0.08)
    MIN_EV = tighten_min("MIN_EV", MIN_EV, 0.05)
    MAX_SPREAD = tighten_max("MAX_SPREAD", MAX_SPREAD, 0.05)
    FEE_BUFFER = tighten_min("FEE_BUFFER", FEE_BUFFER, 0.015)
    MAX_ENTRY_PRICE = tighten_max("MAX_ENTRY_PRICE", MAX_ENTRY_PRICE, 0.58)
    MIN_ENTRY_PRICE = tighten_min("MIN_ENTRY_PRICE", MIN_ENTRY_PRICE, 0.25)
    MIN_CONFIDENCE = tighten_min("MIN_CONFIDENCE", MIN_CONFIDENCE, 0.50)
    MIN_MOVE_USD = tighten_min("MIN_MOVE_USD", MIN_MOVE_USD, 12.0)
    MIN_ASK_SIZE = tighten_min("MIN_ASK_SIZE", MIN_ASK_SIZE, 5.0)

    FIXED_STAKE_USD = tighten_max("FIXED_STAKE_USD", FIXED_STAKE_USD or 1.0, 1.0)
    if FIXED_STAKE_USD <= 0:
        FIXED_STAKE_USD = 1.0
        notes.append("FIXED_STAKE_USD -> 1.0")
    BASE_TRADE_USD = min(BASE_TRADE_USD, FIXED_STAKE_USD)
    MAX_TRADE_USD = min(MAX_TRADE_USD, FIXED_STAKE_USD)
    MAX_EXPOSURE_USD = min(MAX_EXPOSURE_USD, FIXED_STAKE_USD)
    notes.append(f"fixed stake ${FIXED_STAKE_USD:.2f}, max exposure ${MAX_EXPOSURE_USD:.2f}")

    # Arb needs its own cap. This whole profile was written when momentum was
    # the live strategy, and it tightens momentum sizing only — so arb, which
    # is now the ONLY strategy that trades live, sailed past it at $10 a pair
    # while the profile believed exposure was capped at $1.
    _arb_cap = float(os.getenv("ARB_LIVE_MAX_STAKE_USD", "5.0"))
    if ARB_MAX_STAKE_USD > _arb_cap:
        notes.append(f"ARB_MAX_STAKE_USD: {ARB_MAX_STAKE_USD} -> {_arb_cap}")
        ARB_MAX_STAKE_USD = _arb_cap
    _arb_exp = float(os.getenv("ARB_LIVE_MAX_EXPOSURE_USD", "10.0"))
    if ARB_MAX_EXPOSURE_USD > _arb_exp:
        notes.append(f"ARB_MAX_EXPOSURE_USD: {ARB_MAX_EXPOSURE_USD} -> {_arb_exp}")
        ARB_MAX_EXPOSURE_USD = _arb_exp

    DAILY_LOSS_LIMIT_USD = tighten_max("DAILY_LOSS_LIMIT_USD", DAILY_LOSS_LIMIT_USD, 5.0)
    # PCT=0 means "use absolute $ limit only" — do NOT force 5% back on
    # (that made daily_loss=$0.10 on a $2 start balance and halted after one $1 loss).
    if DAILY_LOSS_LIMIT_PCT > 0.05:
        notes.append(f"DAILY_LOSS_LIMIT_PCT: {DAILY_LOSS_LIMIT_PCT} -> 0.05")
        DAILY_LOSS_LIMIT_PCT = 0.05
    elif DAILY_LOSS_LIMIT_PCT <= 0:
        notes.append("DAILY_LOSS_LIMIT_PCT: 0 (absolute $ cap only)")
    MAX_TRADES_PER_DAY = int(tighten_max("MAX_TRADES_PER_DAY",
                                         float(MAX_TRADES_PER_DAY or 12), 10))
    if MAX_TRADES_PER_DAY <= 0:
        MAX_TRADES_PER_DAY = 10
        notes.append("MAX_TRADES_PER_DAY -> 10")
    MAX_CONSECUTIVE_LOSSES = int(tighten_max("MAX_CONSECUTIVE_LOSSES",
                                            float(MAX_CONSECUTIVE_LOSSES or 3), 3))
    KELLY_FRACTION = tighten_max("KELLY_FRACTION", KELLY_FRACTION, 0.10)

    MIN_SLOT_SECONDS = int(tighten_min("MIN_SLOT_SECONDS", float(MIN_SLOT_SECONDS), 90))
    MAX_TRADE_REMAINING = int(tighten_max("MAX_TRADE_REMAINING",
                                          float(MAX_TRADE_REMAINING), 220))

    # Prefer Chainlink first (resolution source).
    # Prefer Polymarket RTDS Chainlink Data Streams (matches 5m market rules).
    preferred = "chainlink_stream"
    if preferred not in BTC_PRICE_SOURCES:
        # Legacy: Arbitrum aggregator named "chainlink" is only a fallback.
        if BTC_PRICE_SOURCES and BTC_PRICE_SOURCES[0] != "chainlink":
            rest = [s for s in BTC_PRICE_SOURCES if s != "chainlink"]
            BTC_PRICE_SOURCES = ["chainlink"] + rest
            notes.append("BTC_PRICE_SOURCES: chainlink first (legacy aggregator)")
    elif BTC_PRICE_SOURCES[0] != preferred:
        rest = [s for s in BTC_PRICE_SOURCES if s != preferred]
        BTC_PRICE_SOURCES = [preferred] + rest
        notes.append("BTC_PRICE_SOURCES: chainlink_stream first (Data Streams)")

    return notes


def apply_late_scalp_profile() -> list[str]:
    """Override timing + entry band for late-window favorite scalping.

    Designed for paper_live testing of Bone Reaper–style convergence:
    trade only in the last ~35s–8s when one side is already ~90%+ priced.
    Still uses limit buys (never market). Does not loosen live stake caps.
    """
    global MIN_SLOT_SECONDS, MAX_TRADE_REMAINING
    global MIN_ENTRY_PRICE, MAX_ENTRY_PRICE
    global EDGE_THRESHOLD, MIN_EV, MIN_CONFIDENCE, MIN_MOVE_USD
    global FEE_BUFFER, MAX_SPREAD, MIN_ASK_SIZE
    global LATE_SCALP_MIN_REMAINING, LATE_SCALP_MAX_REMAINING
    global LATE_SCALP_MIN_ENTRY, LATE_SCALP_MAX_ENTRY

    notes = []
    # Window: last 35s down to 8s remaining
    lo = max(5, int(LATE_SCALP_MIN_REMAINING))
    hi = max(lo + 1, int(LATE_SCALP_MAX_REMAINING))
    LATE_SCALP_MIN_REMAINING = lo
    LATE_SCALP_MAX_REMAINING = hi
    if MIN_SLOT_SECONDS != lo:
        notes.append(f"MIN_SLOT_SECONDS: {MIN_SLOT_SECONDS} -> {lo} (late_scalp)")
        MIN_SLOT_SECONDS = lo
    if MAX_TRADE_REMAINING != hi:
        notes.append(f"MAX_TRADE_REMAINING: {MAX_TRADE_REMAINING} -> {hi} (late_scalp)")
        MAX_TRADE_REMAINING = hi

    # High-price favorites only (strict band for lower loss rate)
    MIN_ENTRY_PRICE = float(LATE_SCALP_MIN_ENTRY)
    MAX_ENTRY_PRICE = float(LATE_SCALP_MAX_ENTRY)
    notes.append(
        f"entry band -> [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}] "
        f"(late favorites from ~0.90; skip micro-payout 0.97+)"
    )

    # Book/signal gates: late scalp is book-driven, not model-edge driven
    EDGE_THRESHOLD = 0.0
    MIN_EV = 0.0
    MIN_CONFIDENCE = 0.0
    MIN_MOVE_USD = max(MIN_MOVE_USD, LATE_SCALP_MIN_MOVE_USD)
    FEE_BUFFER = min(FEE_BUFFER, 0.01)
    MAX_SPREAD = max(MAX_SPREAD, 0.08) if MAX_SPREAD > 0 else 0.08
    # Paper-friendly depth; live safety may have raised MIN_ASK_SIZE already
    notes.append(
        f"window {lo}-{hi}s left; edge/EV/move gates off (favorite-convergence mode)"
    )
    notes.append(
        "WARNING: late_scalp buys expensive favorites - high win-rate, brutal "
        "loss size when wrong. Prefer paper_live first."
    )
    return notes


def is_late_momentum() -> bool:
    """Always true — late_momentum is the only trading strategy.

    mid / late_scalp / late_every / hybrid were retired 2026-07-17: the trade
    archive showed negative live expectancy (rich-favorite buying can't clear
    its ~92% breakeven) or paper results inflated by simulation bugs. The
    sum-to-one arb runs alongside momentum when ARB_TRADE_ENABLED.
    """
    return True


def apply_strategy_flags() -> None:
    """Coerce any legacy STRATEGY value to late_momentum (single strategy)."""
    global STRATEGY
    s = (STRATEGY or "").strip().lower()
    if s and s not in ("late_momentum", "momentum_late", "late_min_move",
                       "momentum"):
        print(f"[config] STRATEGY '{s}' was retired -> using late_momentum")
    STRATEGY = "late_momentum"


def strategy_name() -> str:
    return "diffusion_momentum"


# ---------------------------------------------------------------------------
# Learned parameters
# ---------------------------------------------------------------------------
# The nightly tuner writes state/tuned_params.json after validating a change on
# data it did not fit to. Applying it here — last, after every env default —
# means the running bot picks the learned values up on its next restart without
# anything else in the codebase needing to know the tuner exists.
#
# Precedence, highest first:
#   1. TUNER_FROZEN_KEYS   (comma-separated) — pin a key, tuner may not move it
#   2. tuned_params.json   (when TUNER_ENABLED is on, the default)
#   3. .env / environment
#   4. the hard-coded defaults above
TUNER_ENABLED = _env_bool("TUNER_ENABLED", True)
TUNER_FROZEN_KEYS = {
    k.strip().upper()
    for k in (os.getenv("TUNER_FROZEN_KEYS") or "").split(",") if k.strip()
}
TUNED_APPLIED: dict = {}


def apply_tuned_params(verbose: bool = True) -> dict:
    """Overlay learned parameters onto this module. Returns what changed."""
    global TUNED_APPLIED
    TUNED_APPLIED = {}
    if not TUNER_ENABLED:
        return TUNED_APPLIED
    try:
        import tuner
        learned = (tuner.load_tuned().get("params") or {})
        allowed = set(tuner.LADDERS)
    except Exception as e:  # tuner is optional; never block startup on it
        if verbose:
            print(f"[config] tuned params unavailable ({e}); using defaults")
        return TUNED_APPLIED
    for key, value in learned.items():
        if key not in allowed or key in TUNER_FROZEN_KEYS:
            continue
        old = globals().get(key)
        if old is None or old == value:
            continue
        globals()[key] = type(old)(value) if isinstance(old, (int, float)) else value
        TUNED_APPLIED[key] = {"from": old, "to": globals()[key]}
    if TUNED_APPLIED and verbose:
        print("[config] learned parameters applied (state/tuned_params.json):")
        for key, ch in TUNED_APPLIED.items():
            print(f"  {key}: {ch['from']} -> {ch['to']}")
    return TUNED_APPLIED


apply_tuned_params()


def get_time_suitability(ts: float | None = None) -> tuple[str, str]:
    """Check trading session regime and expected move characteristics (H-004)."""
    import datetime
    import time
    if ts is None:
        ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    hour = dt.hour
    
    slst_dt = dt + datetime.timedelta(hours=5, minutes=30)
    slst_hour = slst_dt.hour
    slst_minute = slst_dt.minute
    slst_str = f"{slst_hour:02d}:{slst_minute:02d} SLST"

    if 8 <= hour < 12:
        return "OPTIMAL", f"London Opening (95.5% win rate baseline) · {slst_str}"
    elif 12 <= hour < 16:
        return "HIGH-VOLATILITY", f"US-Europe Overlap ($44.70 avg move breakout window) · {slst_str}"
    elif 16 <= hour < 21:
        return "ACTIVE", f"US Afternoon Session · {slst_str}"
    elif 0 <= hour < 8:
        return "NORMAL", f"Asian Session · {slst_str}"
    return "QUIET", f"Pacific / Late US Session · {slst_str}"

