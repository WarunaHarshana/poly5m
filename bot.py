"""Main trading loop.

Every round: sample the BTC price, compute the up/down signal, get the market
price for the predicted side, size the trade through the risk manager, place it,
then resolve when the round closes. Writes each trade to state/trades.json and a
live snapshot to state/status.json for the dashboard.

Modes (set MODE in .env or --mode):
  paper       offline random-walk data + simulated fills (no network/money)
  paper_live  REAL BTC prices + REAL Polymarket quotes, simulated fills
              (no wallet/key needed, nothing at risk)
  live        real data + REAL orders (funded wallet required)

For paper_live/live the current 5-minute market's token IDs are fetched fresh
from Gamma at the start of every round and the hold is aligned to that slot's
real end time.

NOTE on resolution: Polymarket settles BTC 5m markets on Chainlink Data Streams
(btc/usd), NOT Binance/Coinbase candles. paper_live PnL prefers the official
Gamma outcome after the slot closes so WON/LOST matches the website.
"""
from __future__ import annotations
import json
import os
import time

import config
from price_feed import SimulatedPriceFeed, LivePriceFeed, ChainlinkDataStream
from signal_engine import compute_signal
from risk import RiskManager
from executor import PaperExecutor, Trade, stamp_trade_times

STATE_DIR = os.path.join(os.path.dirname(__file__), "state")
os.makedirs(STATE_DIR, exist_ok=True)
TRADES_PATH = os.path.join(STATE_DIR, "trades.json")
STATS_PATH = os.path.join(STATE_DIR, "stats.json")
STATUS_PATH = os.path.join(STATE_DIR, "status.json")
# Permanent history — NOT deleted by --reset
ARCHIVE_DIR = os.path.join(STATE_DIR, "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
HISTORY_JSONL = os.path.join(ARCHIVE_DIR, "trades_history.jsonl")
HISTORY_CSV = os.path.join(ARCHIVE_DIR, "trades_history.csv")
SESSIONS_DIR = os.path.join(ARCHIVE_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)

_session_id: str | None = None
_archived_keys: set[str] = set()


def _ensure_session_id() -> str:
    global _session_id
    if not _session_id:
        from datetime import datetime, timezone
        _session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return _session_id


def reset_state():
    """Delete previous-run artifacts so a fresh run starts clean.

    Does NOT delete state/archive/ (permanent trade history).
    """
    removed = False
    for _p in (TRADES_PATH, STATS_PATH, STATUS_PATH):
        try:
            os.remove(_p)
            removed = True
        except FileNotFoundError:
            pass
    print("[reset] cleared previous trades/stats/status." if removed
          else "[reset] no previous state to clear.")
    print(f"[archive] permanent history kept in {ARCHIVE_DIR}")


_status: dict = {}


def write_status(**kw):
    """Merge + persist a live snapshot for the dashboard status bar."""
    _status.update(kw)
    _status["mode"] = config.MODE
    _status["updated_at"] = time.time()
    try:
        with open(STATUS_PATH, "w") as f:
            json.dump(_status, f, indent=2)
    except Exception:
        pass


def market_price_from_model(model_prob_up: float, side: str, noise_rng) -> float:
    """Offline-only synthetic mid: partial reaction to fair value + noise so an
    edge only sometimes exists. Live/paper_live use the real book ask."""
    fair = model_prob_up if side == "UP" else (1 - model_prob_up)
    # Markets underreact to some degree; stickiness in [0.3, 0.8] leaves room
    # for the stricter edge/EV filters to still fire on a subset of rounds.
    stickiness = noise_rng.uniform(0.30, 0.80)
    quote = 0.5 + (fair - 0.5) * stickiness + noise_rng.gauss(0, 0.04)
    return min(0.95, max(0.05, quote))


def _trade_archive_key(d: dict) -> str:
    return "|".join([
        str(d.get("ts", "")),
        str(d.get("round_id", "")),
        str(d.get("slug", "")),
        str(d.get("side", "")),
        str(d.get("entry_price", "")),
        str(d.get("pnl", "")),
        str(d.get("won", "")),
    ])


def _load_archived_keys() -> set[str]:
    keys: set[str] = set()
    if not os.path.isfile(HISTORY_JSONL):
        return keys
    try:
        with open(HISTORY_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    keys.add(_trade_archive_key(d))
                except Exception:
                    continue
    except Exception:
        pass
    return keys


def _rewrite_history_csv() -> None:
    """Rebuild CSV from JSONL for Excel / analysis."""
    import csv
    rows = []
    if os.path.isfile(HISTORY_JSONL):
        with open(HISTORY_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    if not rows:
        return
    # Stable column order; include common fields first
    preferred = [
        "session_id", "ts", "time_utc", "time_et", "mode", "strategy",
        "round_id", "side", "entry_price", "shares", "stake_usd",
        "model_prob", "edge", "outcome", "won", "pnl", "bankroll_after",
        "slug", "question", "event_url", "resolve_source",
        "resolved_ts", "resolved_time_utc", "resolved_time_et",
        "slot_start_ts", "end_ts",
    ]
    fieldnames = list(preferred)
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with open(HISTORY_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def archive_records(trades, stats: dict) -> int:
    """Append settled trades to permanent history. Returns count newly written."""
    global _archived_keys
    if not _archived_keys:
        _archived_keys = _load_archived_keys()
    sid = _ensure_session_id()
    strat = getattr(config, "STRATEGY", "mid")
    try:
        strat = config.strategy_name()
    except Exception:
        pass
    new_n = 0
    try:
        with open(HISTORY_JSONL, "a", encoding="utf-8") as f:
            for t in trades:
                if getattr(t, "won", None) is None:
                    continue
                d = t.to_dict() if hasattr(t, "to_dict") else dict(t)
                d["session_id"] = sid
                d["mode"] = config.MODE
                d["strategy"] = strat
                d["archived_at"] = time.time()
                key = _trade_archive_key(d)
                if key in _archived_keys:
                    continue
                f.write(json.dumps(d, default=str) + "\n")
                _archived_keys.add(key)
                new_n += 1
        if new_n:
            _rewrite_history_csv()
            # Per-session snapshot (full current run)
            snap = {
                "session_id": sid,
                "saved_at": time.time(),
                "mode": config.MODE,
                "strategy": strat,
                "stats": stats,
                "trades": [
                    (t.to_dict() if hasattr(t, "to_dict") else t)
                    for t in trades if getattr(t, "won", None) is not None
                ],
            }
            snap_path = os.path.join(SESSIONS_DIR, f"session_{sid}.json")
            with open(snap_path, "w", encoding="utf-8") as f:
                json.dump(snap, f, indent=2, default=str)
            print(f"  [archive] +{new_n} trade(s) -> {HISTORY_JSONL}")
            print(f"  [archive] session snapshot -> {snap_path}")
            print(f"  [archive] CSV updated -> {HISTORY_CSV}")
    except Exception as e:
        print(f"  [archive] WARNING failed to save history: {e}")
    return new_n


def persist(trades, risk, skip_counts: dict | None = None):
    """Write live dashboard files + permanent archive (survives --reset)."""
    with open(TRADES_PATH, "w") as f:
        json.dump([t.to_dict() for t in trades], f, indent=2)
    settled = [t for t in trades if t.won is not None]
    wins = sum(1 for t in settled if t.won)
    avg_edge = (sum(t.edge for t in settled) / len(settled)) if settled else 0.0
    avg_entry = (sum(t.entry_price for t in settled) / len(settled)) if settled else 0.0
    avg_stake = (sum(t.stake_usd for t in settled) / len(settled)) if settled else 0.0
    # Expectancy: average PnL per trade (the number that actually matters).
    expectancy = (sum(t.pnl for t in settled) / len(settled)) if settled else 0.0
    try:
        strat = config.strategy_name()
    except Exception:
        strat = getattr(config, "STRATEGY", "mid")
    stats = {
        "generated_at": time.time(),
        "session_id": _ensure_session_id(),
        "mode": config.MODE,
        "strategy": strat,
        "starting_bankroll": config.STARTING_BANKROLL,
        "bankroll": round(risk.bankroll, 2),
        "realized_pnl": round(risk.bankroll - config.STARTING_BANKROLL, 2),
        "trades": len(settled),
        "wins": wins,
        "losses": len(settled) - wins,
        "win_rate": round(100 * wins / len(settled), 1) if settled else 0.0,
        "avg_edge": round(avg_edge, 4),
        "avg_entry": round(avg_entry, 4),
        "avg_stake": round(avg_stake, 2),
        "expectancy": round(expectancy, 4),
        "halted": risk.halted,
        "halt_reason": risk.halt_reason,
        "skips": skip_counts or {},
        "archive_dir": ARCHIVE_DIR,
        "history_jsonl": HISTORY_JSONL,
        "history_csv": HISTORY_CSV,
    }
    with open(STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    archive_records(trades, stats)
    return stats


def run_sim(rounds: int = 200, sleep_between: float = 0.0, seed: int = 42):
    """Offline paper simulation (MODE=paper)."""
    import random
    noise = random.Random(seed + 1)
    feed = SimulatedPriceFeed(seed=seed)
    risk = RiskManager(config.STARTING_BANKROLL)
    executor = PaperExecutor()
    trades: list[Trade] = []
    skips = {"no_edge": 0}
    write_status(phase="running", message="offline simulation")

    for rid in range(1, rounds + 1):
        if risk.halted:
            print(f"[HALT] {risk.halt_reason}; stopping.")
            break
        for _ in range(config.MOMENTUM_LOOKBACK):
            feed.step()
        prices = feed.recent(config.MOMENTUM_LOOKBACK)
        price_before = prices[-1]
        sig = compute_signal(
            prices,
            sample_seconds=config.SAMPLE_SECONDS,
            volatility_daily=config.VOLATILITY_DAILY,
            vol_blend=config.VOL_BLEND,
        )
        # Synthetic ask: mid + half-spread so offline sim is not filled at mid.
        mid = market_price_from_model(sig.model_prob_up, sig.side, noise)
        half_spread = max(0.01, abs(noise.gauss(0.015, 0.008)))
        mkt = min(0.97, mid + half_spread)
        model_side_prob = sig.model_prob_up if sig.side == "UP" else (1 - sig.model_prob_up)
        stake = risk.size_trade(model_side_prob, mkt, confidence=sig.confidence)
        if stake <= 0:
            skips["no_edge"] += 1
            continue
        shares = round(stake / mkt, 4)
        trade = Trade(ts=time.time(), round_id=rid, side=sig.side,
                      entry_price=round(mkt, 4), shares=shares, stake_usd=stake,
                      model_prob=round(model_side_prob, 4),
                      edge=round(model_side_prob - mkt, 4))
        stamp_trade_times(trade)
        risk.on_open(stake)
        for _ in range(config.MOMENTUM_LOOKBACK):
            feed.step()
        price_after = feed.price()
        trade = executor.open_and_resolve(trade, price_after >= price_before)
        risk.on_close(stake, trade.pnl)
        trade.bankroll_after = round(risk.bankroll, 2)
        trades.append(trade)
        flag = "WON " if trade.won else "LOST"
        print(f"R{rid:>3} {sig.side:<4} entry={mkt:.2f} stake=${stake:>5.2f} "
              f"edge={trade.edge:+.2f} conf={sig.confidence:.2f} -> {flag} "
              f"pnl={trade.pnl:+.2f} bankroll=${risk.bankroll:.2f} "
              f"@ {trade.time_et or trade.time_utc}")
        if sleep_between:
            time.sleep(sleep_between)

    stats = persist(trades, risk, skips)
    write_status(phase="stopped", message="simulation complete")
    print("\n=== SUMMARY ===")
    print(json.dumps(stats, indent=2))
    return stats


def _warmup_prices(feed, remaining):
    """Collect a short momentum window of real prices, bounded so it fits inside
    the current slot (leave room to place the trade before the slot ends)."""
    n = config.MOMENTUM_LOOKBACK
    spacing = config.SAMPLE_SECONDS
    if remaining is not None and n > 1:
        allowed = max(0.0, remaining * 0.4)  # use at most 40% of the slot warming up
        if spacing * (n - 1) > allowed:
            spacing = max(0.5, allowed / (n - 1)) if allowed > 0 else 0.0
    prices = []
    for i in range(n):
        try:
            prices.append(feed.price())
        except Exception as e:
            print(f"  price sample failed: {e}")
        if i < n - 1 and spacing > 0:
            time.sleep(spacing)
    return prices


def _now(cl_stream: ChainlinkDataStream | None = None) -> float:
    """Wall time for slot math — prefer Chainlink stream clock (PC often drifts)."""
    if cl_stream is not None and cl_stream.ready:
        return cl_stream.market_now()
    return time.time()


def _sleep_until_slot_end(end_ts, cl_stream: ChainlinkDataStream | None = None):
    """Wait out the current slot so the next fetch returns a fresh one.

    Honors ROUND_SECONDS<=0 as a 'fast/test' setting (no artificial wait), which
    keeps offline self-tests instant while still pacing real runs.

    If end_ts is already in the past (common with PC clock skew), sleep a short
    fixed interval so we never busy-spin on the same expired slot.
    """
    if config.ROUND_SECONDS <= 0:
        return  # fast/test mode: don't stall
    if end_ts:
        wait = end_ts - _now(cl_stream)
        if wait > 0:
            cap = config.ROUND_SECONDS + 1
            time.sleep(min(wait + 1.0, cap))
        else:
            # Past end already — don't tight-loop; give Gamma/discovery a moment
            # to roll to the next slug (and let stream clock advance).
            time.sleep(min(5.0, max(2.0, config.ROUND_SECONDS * 0.02)))
        return
    time.sleep(min(5.0, config.ROUND_SECONDS))


def _backoff(seconds):
    """Bounded retry sleep that is skipped in fast/test mode (ROUND_SECONDS<=0)."""
    if config.ROUND_SECONDS <= 0:
        return
    time.sleep(seconds)


def _quote_side(pub, market, side: str) -> dict | None:
    """Best-ask buy quote for UP or DOWN, or None on failure."""
    try:
        token_id = market["tokens"][side]
        if hasattr(pub, "buy_quote"):
            return pub.buy_quote(token_id)
        mid = pub.midpoint(token_id)
        return {"price": mid, "mid": mid, "best_bid": None,
                "best_ask": None, "spread": None, "ask_size": None}
    except Exception:
        return None


def _pick_late_scalp_favorite(pub, market) -> tuple[str, dict] | None:
    """Pick the expensive side (market favorite) if clearly leading.

    Returns (side, quote) or None if book is balanced / unusable.
    """
    q_up = _quote_side(pub, market, "UP")
    q_dn = _quote_side(pub, market, "DOWN")
    if not q_up or not q_dn:
        return None
    try:
        p_up = float(q_up["price"])
        p_dn = float(q_dn["price"])
    except (TypeError, ValueError, KeyError):
        return None
    if not (0.0 < p_up < 1.0 and 0.0 < p_dn < 1.0):
        return None
    lead = getattr(config, "LATE_SCALP_MIN_FAVORITE_LEAD", 0.50) or 0.50
    # Favorite = higher ask (market prices it more likely to pay $1).
    if p_up >= p_dn and (p_up - p_dn) >= lead:
        return "UP", q_up
    if p_dn > p_up and (p_dn - p_up) >= lead:
        return "DOWN", q_dn
    return None


def run_realtime(rounds: int = 100, paper: bool = True):
    """Real BTC data + real Polymarket quotes, discovering the current slot's
    token IDs from Gamma at the start of every round.

    paper=True  -> simulated fills (MODE=paper_live), no wallet/money.
    paper=False -> REAL orders (MODE=live), funded wallet required.

    Resolution (matches Polymarket rules):
      Up iff Chainlink Data Streams BTC/USD at end >= price at start of range.
      Source: https://data.chain.link/streams/btc-usd (via Polymarket RTDS).
      Paper PnL prefers Gamma's official outcome; stream used for strike/signal.
    """
    from polymarket_client import PolymarketPublic
    pub = PolymarketPublic()
    cl_stream = ChainlinkDataStream()
    cl_stream.start()
    feed = LivePriceFeed(sources=list(config.BTC_PRICE_SOURCES),
                         chainlink_stream=cl_stream)
    risk = RiskManager(config.STARTING_BANKROLL)
    executor = PaperExecutor()

    live_client = None
    if not paper:
        from polymarket_client import PolymarketClient
        if not config.PRIVATE_KEY or not config.FUNDER_ADDRESS:
            raise SystemExit(
                "MODE=live needs PRIVATE_KEY + FUNDER_ADDRESS in .env")
        if config.LIVE_CONFIRM not in ("yes", "y", "true", "1"):
            raise SystemExit(
                "MODE=live is blocked. Set LIVE_CONFIRM=yes in .env only after\n"
                "you have run paper_live successfully and accept real capital risk.\n"
                "There is NO profit guarantee — this bot only limits downside size.")
        notes = config.apply_live_safety_profile()
        print("[LIVE SAFETY PROFILE]")
        for n in notes:
            print(f"  - {n}")
        print(f"  stake=${config.FIXED_STAKE_USD or config.BASE_TRADE_USD:.2f}  "
              f"daily_loss<=${risk._daily_loss_limit():.2f}  "
              f"max_trades={config.MAX_TRADES_PER_DAY}  "
              f"edge>={config.EDGE_THRESHOLD}  fee_buffer={config.FEE_BUFFER}")
        # Re-bind bankroll after profile (sizes may have changed, bankroll same).
        risk = RiskManager(config.STARTING_BANKROLL)
        live_client = PolymarketClient()

    # Strategy profile (after live safety so late_scalp can override the window)
    hybrid = config.is_hybrid()
    late_only = config.is_late_scalp() and not hybrid
    # Snapshot mid-window params before any late profile mutates them
    mid_min_slot = int(config.MIN_SLOT_SECONDS)
    mid_max_rem = int(config.MAX_TRADE_REMAINING)
    mid_min_entry = float(config.MIN_ENTRY_PRICE)
    mid_max_entry = float(config.MAX_ENTRY_PRICE)
    mid_min_move = float(config.MIN_MOVE_USD)
    mid_edge = float(config.EDGE_THRESHOLD)
    mid_min_ev = float(getattr(config, "MIN_EV", 0) or 0)
    mid_min_conf = float(getattr(config, "MIN_CONFIDENCE", 0) or 0)

    late_min = int(getattr(config, "LATE_SCALP_MIN_REMAINING", 8))
    late_max = int(getattr(config, "LATE_SCALP_MAX_REMAINING", 35))
    late_min_entry = float(getattr(config, "LATE_SCALP_MIN_ENTRY", 0.88))
    late_max_entry = float(getattr(config, "LATE_SCALP_MAX_ENTRY", 0.97))

    if late_only:
        ls_notes = config.apply_late_scalp_profile()
        print("[STRATEGY: late_scalp] late-window favorites only")
        for n in ls_notes:
            print(f"  - {n}")
        if not paper:
            print("  [warn] late_scalp on LIVE money — prefer paper_live first")
        risk = RiskManager(config.STARTING_BANKROLL)
        late_min = config.MIN_SLOT_SECONDS
        late_max = config.MAX_TRADE_REMAINING
    elif hybrid:
        print("[STRATEGY: hybrid] mid-edge first, then late_scalp on same slot if no fill")
        print(f"  mid window {mid_min_slot}–{mid_max_rem}s  "
              f"band [{mid_min_entry:.2f},{mid_max_entry:.2f}]")
        print(f"  late window {late_min}–{late_max}s  "
              f"band [{late_min_entry:.2f},{late_max_entry:.2f}]")
        print(f"  min shares {getattr(config, 'MIN_ORDER_SHARES', 5):.0f} "
              f"(stake auto-bumped for CLOB limits)")
        if not paper:
            print("  [warn] hybrid on LIVE — paper_live recommended first")
    else:
        print(f"[STRATEGY: mid] mid-slot edge band "
              f"[{config.MIN_ENTRY_PRICE:.2f}, {config.MAX_ENTRY_PRICE:.2f}]  "
              f"window {config.MIN_SLOT_SECONDS}–{config.MAX_TRADE_REMAINING}s left")
        print(f"  min shares {getattr(config, 'MIN_ORDER_SHARES', 5):.0f} "
              f"(ENFORCE={getattr(config, 'ENFORCE_MIN_SHARES', True)})")

    label = "PAPER-LIVE (real data, simulated fills)" if paper else "LIVE (REAL MONEY)"
    strat_tag = config.strategy_name()
    print(f"[{label} | {strat_tag}] warming up Chainlink Data Stream "
          f"(Polymarket RTDS btc/usd)...")
    write_status(phase="starting", message="warming up Chainlink Data Stream",
                 strategy=strat_tag)

    # Hybrid: after mid skip on a slot, remember to try late on same end_ts
    pending_late_end_ts = None
    if not cl_stream.wait_ready(timeout=20.0):
        print("  [warn] Chainlink Data Stream slow/unavailable; "
              "falling back to other feeds (NOT official resolution source)")
    try:
        spot = feed.price()
        src = "chainlink-data-stream" if cl_stream.ready else "fallback"
        print(f"  BTC spot now: ${spot:,.2f}  [{src}]")
        if cl_stream.ready:
            off = cl_stream.clock_offset_s()
            print(f"  resolution source: Chainlink Data Streams BTC/USD "
                  f"(Polymarket RTDS)  PC clock offset vs stream: {off:+.1f}s")
            if abs(off) >= 5:
                print("  [warn] Windows clock is skewed vs Chainlink; bot uses "
                      "stream time for slot countdowns. Sync OS time if you can.")
        write_status(phase="idle", btc_price=round(spot, 2))
    except Exception as e:
        write_status(phase="error", message=f"no BTC price source: {e}")
        cl_stream.stop()
        raise SystemExit(f"Could not reach a BTC price source: {e}")

    # Live: show real Polymarket USDC balance at startup
    if not paper and live_client is not None:
        try:
            bal = live_client.get_usdc_balance(refresh=True)
            if bal.get("ok") and bal.get("balance_usd") is not None:
                print(f"  Polymarket wallet USDC: ${bal['balance_usd']:,.2f}  "
                      f"(funder {bal.get('funder')})")
                write_status(wallet_balance_usd=bal["balance_usd"],
                             wallet_funder=bal.get("funder"),
                             wallet_ok=True)
            else:
                print(f"  [warn] could not read wallet balance: "
                      f"{bal.get('error') or bal}")
                write_status(wallet_ok=False,
                             wallet_error=str(bal.get("error") or "unknown"))
        except Exception as e:
            print(f"  [warn] wallet balance lookup failed: {e}")

    trades: list[Trade] = []
    placed = 0
    lookup_fails = 0
    slots_seen = 0
    skips = {
        "slot_closing": 0,
        "slot_too_early": 0,
        "bad_samples": 0,
        "thin_move": 0,
        "quote_fail": 0,
        "quote_range": 0,
        "wide_spread": 0,
        "thin_book": 0,
        "no_edge": 0,
        "order_fail": 0,
        "unfilled": 0,
    }
    max_slots = max(rounds * 30, 300)  # safety cap so a dead market can't spin forever

    while placed < rounds and slots_seen < max_slots:
        slots_seen += 1
        if risk.halted:
            print(f"[HALT] {risk.halt_reason}; stopping.")
            write_status(phase="halted", message=risk.halt_reason)
            break

        # Fetch the freshest current 5-minute slot's token IDs.
        # Pass stream clock so a slow PC clock doesn't re-select an ended slot.
        try:
            market = pub.find_btc_5min_market(now=_now(cl_stream))
            lookup_fails = 0
        except Exception as e:
            lookup_fails += 1
            print(f"[market lookup failed] {e}; retrying in 15s")
            write_status(phase="searching", message=str(e))
            if lookup_fails >= 20:
                print("[giving up] market not found after 20 tries. Run discover.py.")
                write_status(phase="error", message="market not found after 20 tries")
                break
            _backoff(15)
            continue

        question = market.get("question") or "BTC 5-minute Up/Down"
        end_ts = market.get("end_ts")
        remaining = (end_ts - _now(cl_stream)) if end_ts else None
        rem_txt = f"{remaining:.0f}s left" if remaining is not None else "no end time"
        print(f"\nslot: {question}  ({rem_txt})")
        write_status(phase="analyzing", question=question, slug=market.get("slug"),
                     end_ts=end_ts, side=None, edge=None, mkt=None)

        # Decide phase for this iteration: mid | late
        force_late = (
            hybrid and pending_late_end_ts is not None
            and end_ts is not None and pending_late_end_ts == end_ts
        )
        use_late = late_only or force_late

        if hybrid and not force_late and remaining is not None:
            # Past mid window with nothing pending → go straight toward late
            if remaining < mid_min_slot and remaining > late_max:
                pending_late_end_ts = end_ts
                force_late = True
                use_late = True
                print(f"  [hybrid] past mid window ({remaining:.0f}s); "
                      f"queuing late_scalp for this slot")

        if use_late:
            min_slot = late_min
            max_rem = late_max
            phase_name = "late_scalp"
        else:
            min_slot = mid_min_slot if hybrid else int(config.MIN_SLOT_SECONDS)
            max_rem = mid_max_rem if hybrid else (
                getattr(config, "MAX_TRADE_REMAINING", 0) or 0)
            phase_name = "mid"

        if remaining is not None and remaining < min_slot:
            if hybrid and pending_late_end_ts == end_ts:
                pending_late_end_ts = None
            print(f"  slot closing ({remaining:.0f}s); waiting for the next slot...")
            skips["slot_closing"] += 1
            write_status(phase="waiting", message="slot closing")
            _sleep_until_slot_end(end_ts, cl_stream)
            continue
        if remaining is not None and max_rem > 0 and remaining > max_rem:
            wait = remaining - max_rem
            print(f"  slot too early ({remaining:.0f}s left); waiting {wait:.0f}s "
                  f"for {phase_name} window (max rem {max_rem}s)...")
            skips["slot_too_early"] += 1
            write_status(phase="waiting", message=f"waiting for {phase_name}")
            if config.ROUND_SECONDS > 0 and wait > 0:
                time.sleep(min(wait, 15.0 if use_late else 30.0))
            continue

        remaining = (end_ts - _now(cl_stream)) if end_ts else remaining
        if remaining is not None and remaining < min_slot:
            if hybrid and pending_late_end_ts == end_ts:
                pending_late_end_ts = None
            print(f"  window missed ({remaining:.0f}s left); next slot...")
            skips["slot_closing"] += 1
            _sleep_until_slot_end(end_ts, cl_stream)
            continue

        # ---- LATE SCALP PATH: book favorite, no model edge ----
        if use_late:
            # Restore late band for sizing (hybrid keeps mid band in globals)
            config.MIN_ENTRY_PRICE = late_min_entry
            config.MAX_ENTRY_PRICE = late_max_entry
            config.MIN_SLOT_SECONDS = late_min
            config.MAX_TRADE_REMAINING = late_max
            slot_start_time = (end_ts - config.SLOT_SECONDS) if end_ts else None
            slot_open_ref = None
            strike_src = "unknown"
            if slot_start_time:
                try:
                    slot_open_ref = feed.get_slot_start_price(slot_start_time)
                    if slot_open_ref is not None:
                        has_cl = bool(
                            cl_stream.ready and (
                                cl_stream.price_at_or_before(
                                    float(slot_start_time), max_age_s=30.0)
                                or cl_stream.price_at(
                                    float(slot_start_time), max_skew_s=15.0)
                            )
                        )
                        strike_src = ("chainlink-data-stream" if has_cl
                                      else "exchange-proxy")
                except Exception as e:
                    print(f"  failed to fetch slot start price: {e}")
            try:
                price_before = feed.price()
            except Exception:
                price_before = slot_open_ref or 0.0
            if slot_open_ref is None:
                slot_open_ref = price_before
                strike_src = "warmup-fallback"
            move_usd = abs(price_before - slot_open_ref) if slot_open_ref else 0.0
            print(f"  [late_scalp] strike=${slot_open_ref:,.2f} [{strike_src}]  "
                  f"btc=${price_before:,.2f} move=${move_usd:.1f}  "
                  f"{remaining:.0f}s left")

            picked = _pick_late_scalp_favorite(pub, market)
            if not picked:
                print("  [late_scalp] no clear favorite (book balanced / quote fail); skip")
                skips["quote_fail"] += 1
                write_status(phase="waiting", message="no clear favorite")
                if remaining is not None and remaining > late_min + 2:
                    time.sleep(min(2.0, max(0.5, remaining - late_min)))
                    continue
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                continue
            side, q = picked
            mkt = float(q["price"])
            ask_sz = q.get("ask_size")
            min_entry = late_min_entry
            max_entry = late_max_entry
            if mkt < min_entry or mkt > max_entry:
                print(f"  [late_scalp] favorite ask {mkt:.3f} outside "
                      f"[{min_entry:.2f}, {max_entry:.2f}]; skip")
                skips["quote_range"] += 1
                if remaining is not None and remaining > late_min + 2:
                    time.sleep(1.0)
                    continue
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                continue
            max_spread = getattr(config, "MAX_SPREAD", 0) or 0
            if max_spread > 0 and q.get("spread") is not None and q["spread"] > max_spread:
                print(f"  [late_scalp] spread {q['spread']:.3f} too wide; skip")
                skips["wide_spread"] += 1
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                continue
            min_ask_sz = getattr(config, "MIN_ASK_SIZE", 0) or 0
            if min_ask_sz > 0 and ask_sz is not None and ask_sz < min_ask_sz:
                print(f"  [late_scalp] ask size {ask_sz:.2f} < {min_ask_sz:.2f}; skip")
                skips["thin_book"] += 1
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                continue

            model_side_prob = mkt
            fee = getattr(config, "FEE_BUFFER", 0.0) or 0.0
            stake = risk.size_late_scalp(mkt)
            edge = 0.0
            print(f"  [late_scalp] side={side} ask={mkt:.3f} stake=${stake:.2f}  "
                  f"(favorite convergence; limit buy; min shares enforced)")
            write_status(phase="analyzing", btc_price=round(price_before, 2),
                         side=side, model=round(model_side_prob, 4),
                         mkt=round(mkt, 4), edge=0.0, stake=stake,
                         confidence=1.0, end_ts=end_ts, question=question,
                         strategy="late_scalp")
            if stake <= 0:
                why = getattr(risk, "last_skip_reason", "") or "no stake"
                print(f"  size skip ({why})")
                skips["no_edge"] += 1
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                continue
            pending_late_end_ts = None
            token_id = market["tokens"][side]
            class _LSSig:
                side = side
                confidence = 1.0
                model_prob_up = mkt if side == "UP" else (1.0 - mkt)
                realized_vol_daily = None
            sig = _LSSig()
        else:
            # ---- MID-SLOT PATH (original) ----
            config.MIN_ENTRY_PRICE = mid_min_entry
            config.MAX_ENTRY_PRICE = mid_max_entry
            config.MIN_MOVE_USD = mid_min_move
            config.EDGE_THRESHOLD = mid_edge
            config.MIN_EV = mid_min_ev
            config.MIN_CONFIDENCE = mid_min_conf
            prices = _warmup_prices(feed, remaining)
            if len(prices) < 3:
                print("  not enough price samples; retrying in 5s")
                skips["bad_samples"] += 1
                write_status(phase="searching", message="price feed unstable")
                if hybrid:
                    pending_late_end_ts = end_ts
                    print("  [hybrid] mid samples weak; will try late_scalp later")
                    rem_now = (end_ts - _now(cl_stream)) if end_ts else None
                    if rem_now is not None and rem_now > late_max and config.ROUND_SECONDS > 0:
                        time.sleep(min(rem_now - late_max, 25.0))
                    continue
                _backoff(5)
                continue

            # Strike = BTC price at beginning of the range (Polymarket rules).
            slot_start_time = (end_ts - config.SLOT_SECONDS) if end_ts else None
            slot_open_ref = None
            strike_src = "unknown"
            if slot_start_time:
                try:
                    slot_open_ref = feed.get_slot_start_price(slot_start_time)
                    if slot_open_ref is not None:
                        has_cl = bool(
                            cl_stream.ready and (
                                cl_stream.price_at_or_before(
                                    float(slot_start_time), max_age_s=30.0)
                                or cl_stream.price_at(
                                    float(slot_start_time), max_skew_s=15.0)
                            )
                        )
                        strike_src = ("chainlink-data-stream" if has_cl
                                      else "exchange-proxy")
                except Exception as e:
                    print(f"  failed to fetch slot start price: {e}")

            if slot_open_ref is None:
                slot_open_ref = prices[0]
                strike_src = "warmup-fallback"
                print(f"  [bot] using fallback start price: ${slot_open_ref:,.2f} "
                      f"[{strike_src}]")
            else:
                print(f"  [bot] using strike price: ${slot_open_ref:,.2f} "
                      f"[{strike_src}]")
            if strike_src in ("exchange-proxy", "warmup-fallback"):
                print("  [warn] strike is NOT Chainlink Data Stream — close-call "
                      "local PnL can disagree with Polymarket. Will wait longer "
                      "for official Gamma resolution.")

            price_before = prices[-1]
            remaining_secs = (end_ts - _now(cl_stream)) if end_ts else None

            n = len(prices)
            spacing = config.SAMPLE_SECONDS
            if remaining is not None and n > 1:
                allowed = max(0.0, remaining * 0.4)
                if spacing * (n - 1) > allowed:
                    spacing = max(0.5, allowed / (n - 1)) if allowed > 0 else 0.5

            try:
                oracle_now = feed.price()
                if oracle_now and oracle_now > 0:
                    price_before = oracle_now
                    prices = prices[:-1] + [oracle_now]
            except Exception:
                pass

            def _mid_no_trade(skip_key: str, msg: str):
                """Skip mid; in hybrid, queue late_scalp on same slot instead of burning it."""
                nonlocal pending_late_end_ts
                skips[skip_key] = skips.get(skip_key, 0) + 1
                write_status(phase="waiting", message=msg)
                if hybrid:
                    pending_late_end_ts = end_ts
                    print(f"  [hybrid] mid skip ({msg}); will try late_scalp "
                          f"when ~{late_max}s left")
                    rem_now = (end_ts - _now(cl_stream)) if end_ts else None
                    if rem_now is not None and rem_now > late_max and config.ROUND_SECONDS > 0:
                        time.sleep(min(rem_now - late_max, 25.0))
                    return "hybrid_late"
                _sleep_until_slot_end(end_ts, cl_stream)
                return "done"

            min_move = getattr(config, "MIN_MOVE_USD", 0.0) or 0.0
            move_usd = abs(price_before - slot_open_ref)
            if min_move > 0 and move_usd < min_move:
                print(f"  move ${move_usd:.2f} < MIN_MOVE_USD ${min_move:.2f}; skip")
                if _mid_no_trade("thin_move", "move too small") == "hybrid_late":
                    continue
                continue

            sig = compute_signal(
                prices,
                slot_start_price=slot_open_ref,
                remaining_seconds=remaining_secs,
                volatility_daily=config.VOLATILITY_DAILY,
                sample_seconds=spacing if spacing > 0 else config.SAMPLE_SECONDS,
                vol_blend=config.VOL_BLEND,
            )

            token_id = market["tokens"][sig.side]
            try:
                if hasattr(pub, "buy_quote"):
                    q = pub.buy_quote(token_id)
                else:
                    mid = pub.midpoint(token_id)
                    q = {"price": mid, "mid": mid, "best_bid": None,
                         "best_ask": None, "spread": None, "ask_size": None}
                mkt = q["price"]
            except Exception as e:
                print(f"  quote fetch failed ({e}); skipping slot")
                if _mid_no_trade("quote_fail", "quote fetch failed") == "hybrid_late":
                    continue
                continue
            if not (config.MIN_QUOTE <= mkt <= config.MAX_QUOTE):
                print(f"  quote {mkt:.4f} outside tradeable range "
                      f"[{config.MIN_QUOTE:.2f}, {config.MAX_QUOTE:.2f}] "
                      "(empty/illiquid book); skipping slot")
                if _mid_no_trade("quote_range", f"quote {mkt:.3f} out of range") == "hybrid_late":
                    continue
                continue
            min_entry = getattr(config, "MIN_ENTRY_PRICE", 0.0) or 0.0
            max_entry = getattr(config, "MAX_ENTRY_PRICE", 1.0) or 1.0
            if mkt < min_entry or mkt > max_entry:
                print(f"  ask {mkt:.3f} outside entry band "
                      f"[{min_entry:.2f}, {max_entry:.2f}]; skip (avoid bad payoff odds)")
                if _mid_no_trade("quote_range", f"ask {mkt:.3f} outside entry band") == "hybrid_late":
                    continue
                continue
            max_spread = getattr(config, "MAX_SPREAD", 0) or 0
            if max_spread > 0 and q.get("spread") is not None and q["spread"] > max_spread:
                print(f"  spread {q['spread']:.3f} > MAX_SPREAD {max_spread:.3f}; skipping")
                if _mid_no_trade("wide_spread", f"spread {q['spread']:.3f} too wide") == "hybrid_late":
                    continue
                continue
            min_ask_sz = getattr(config, "MIN_ASK_SIZE", 0) or 0
            ask_sz = q.get("ask_size")
            if min_ask_sz > 0 and ask_sz is not None and ask_sz < min_ask_sz:
                print(f"  ask size {ask_sz:.2f} < MIN_ASK_SIZE {min_ask_sz:.2f}; thin book")
                if _mid_no_trade("thin_book", "thin ask") == "hybrid_late":
                    continue
                continue

            model_side_prob = (
                sig.model_prob_up if sig.side == "UP" else (1 - sig.model_prob_up)
            )
            fee = getattr(config, "FEE_BUFFER", 0.0) or 0.0
            stake = risk.size_trade(model_side_prob, mkt, confidence=sig.confidence)
            edge = model_side_prob - mkt - fee
            rv_txt = (f" rv={sig.realized_vol_daily:.3f}"
                      if sig.realized_vol_daily is not None else "")
            print(f"  btc=${price_before:,.2f} strike=${slot_open_ref:,.2f} "
                  f"move=${move_usd:.1f} side={sig.side} model={model_side_prob:.2f} "
                  f"ask={mkt:.2f} edge={edge:+.2f} conf={sig.confidence:.2f} "
                  f"stake=${stake:.2f}{rv_txt}")
            write_status(phase="analyzing", btc_price=round(price_before, 2),
                         side=sig.side, model=round(model_side_prob, 4),
                         mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                         confidence=round(sig.confidence, 3),
                         end_ts=end_ts, question=question, strategy="mid")
            if stake <= 0:
                why = getattr(risk, "last_skip_reason", "") or "no qualifying edge"
                print(f"  size skip ({why}) -> skip this slot")
                if _mid_no_trade("no_edge", why) == "hybrid_late":
                    continue
                continue
            token_id = market["tokens"][sig.side]

        shares = round(stake / mkt, 4)
        # Ensure we don't request more shares than visible ask size when known.
        if ask_sz is not None and ask_sz > 0:
            shares = min(shares, round(ask_sz * 0.5, 4))  # take at most half the level
            stake = round(shares * mkt, 2)
            if stake < 0.5:
                print("  sized below dust after depth cap; skip")
                skips["thin_book"] += 1
                _sleep_until_slot_end(end_ts, cl_stream)
                continue

        trade = Trade(
            ts=time.time(),
            round_id=placed + 1,
            side=sig.side,
            entry_price=round(mkt, 4),
            shares=shares,
            stake_usd=stake,
            model_prob=round(model_side_prob, 4),
            edge=round(edge, 4),
            slug=market.get("slug") or "",
            question=question,
            slot_start_ts=slot_start_time,
            end_ts=end_ts,
        )
        stamp_trade_times(trade)
        risk.on_open(stake)
        # Notify dashboard immediately (before multi-minute hold).
        trade_event_id = f"placed-{trade.round_id}-{int(trade.ts)}"
        strat_lbl = "late_scalp" if late_scalp else "mid"
        print(f"  TRADE PLACED [{strat_lbl}] R{trade.round_id} {trade.side} "
              f"@ {trade.entry_price:.2f} ${trade.stake_usd:.2f}  "
              f"{trade.time_et or trade.time_utc}")
        write_status(
            phase="holding",
            trade_event="placed",
            trade_event_id=trade_event_id,
            end_ts=end_ts,
            side=sig.side,
            mkt=round(mkt, 4),
            edge=round(edge, 4),
            stake=stake,
            question=question,
            slug=trade.slug,
            event_url=trade.event_url,
            time_et=trade.time_et,
            time_utc=trade.time_utc,
            strategy=strat_lbl,
            message=f"TRADE PLACED [{strat_lbl}] {sig.side} @ {mkt:.2f}",
        )

        order_id = None
        if not paper:
            try:
                # Re-check quote right before order (latency / adverse selection).
                try:
                    q2 = pub.buy_quote(token_id)
                    if q2["price"] > mkt + 0.02:
                        raise RuntimeError(
                            f"ask moved {mkt:.3f} -> {q2['price']:.3f} before order")
                    if q2["price"] > mkt:
                        mkt = q2["price"]
                        trade.entry_price = round(mkt, 4)
                        shares = round(stake / mkt, 4)
                        trade.shares = shares
                except RuntimeError:
                    raise
                except Exception:
                    pass
                resp = live_client.buy(token_id, float(mkt),
                                       shares, market["conditionId"])
                print(f"  ORDER PLACED: {resp}")
                if isinstance(resp, dict):
                    order_id = (resp.get("orderID") or resp.get("order_id")
                                or resp.get("id"))
            except Exception as e:
                print(f"  order failed: {e}")
                skips["order_fail"] += 1
                risk.on_close(stake, 0.0)
                _sleep_until_slot_end(end_ts, cl_stream)
                continue

        # Hold until this slot actually resolves.
        hold = (end_ts - _now(cl_stream)) if end_ts else config.ROUND_SECONDS
        print(f"  holding {max(0, hold):.0f}s until the slot resolves...")
        write_status(phase="holding", end_ts=end_ts, side=sig.side,
                     mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                     question=question, slug=trade.slug,
                     message=f"holding {sig.side} @ {mkt:.2f}")
        if hold and hold > 0:
            time.sleep(hold)

        # Live: cancel any resting leftovers so they don't fill next slot.
        if not paper and live_client is not None:
            try:
                if order_id:
                    live_client.cancel_order(str(order_id))
                live_client.cancel_all()
            except Exception as e:
                print(f"  cancel cleanup: {e}")

        # --- Resolution (Polymarket rules) ---
        # Up iff end_price >= start_price on Chainlink Data Streams BTC/USD.
        # ALWAYS prefer official Gamma outcome (matches website). Local compare
        # is only a last resort — and is UNRELIABLE if strike was exchange-proxy
        # (mid-slot restart missing Chainlink open tick).
        price_after = None
        if slot_start_time:
            try:
                price_after = feed.get_slot_close_price(slot_start_time)
            except Exception as e:
                print(f"  failed to fetch slot close price: {e}")
        if price_after is None:
            try:
                price_after = feed.price()
            except Exception:
                price_after = price_before
        local_up = price_after >= slot_open_ref

        official = None
        slug = market.get("slug") or ""
        # Proxy strike / late on-chain settlement: poll longer (~3 min).
        # Healthy-book wait on the *next* slot still leaves room for this.
        proxy_strike = strike_src in ("exchange-proxy", "warmup-fallback")
        poll_attempts = 90 if proxy_strike else 60  # 270s or 180s @ 3s
        if slug:
            write_status(phase="resolving", message="waiting for Polymarket outcome",
                         end_ts=end_ts, question=question, side=sig.side)
            # Brief grace — Gamma often lags end_ts by 10–30s+
            if config.ROUND_SECONDS > 0:
                time.sleep(12.0)
            print(f"  [resolve] polling official outcome up to "
                  f"~{poll_attempts * 3}s (strike={strike_src})...")
            for attempt in range(poll_attempts):
                try:
                    official = pub.get_official_btc_updown_outcome(slug)
                except Exception as e:
                    if attempt == 0:
                        print(f"  [resolve] official lookup error: {e}")
                    official = None
                if official in ("UP", "DOWN"):
                    break
                if config.ROUND_SECONDS <= 0:
                    break
                if attempt > 0 and attempt % 10 == 0:
                    print(f"  [resolve] still waiting for Gamma "
                          f"({attempt * 3}s)...")
                time.sleep(3.0)

        if official in ("UP", "DOWN"):
            resolved_up = official == "UP"
            src = "polymarket-official"
            if (official == "UP") != local_up:
                print(f"  [resolve] mismatch: official={official} vs "
                      f"stream/local={'UP' if local_up else 'DOWN'} "
                      f"(end=${price_after:,.2f} start=${slot_open_ref:,.2f} "
                      f"strike={strike_src}) - using official (website truth)")
            else:
                print(f"  [resolve] official={official} "
                      f"(local agrees; end=${price_after:,.2f} "
                      f"start=${slot_open_ref:,.2f})")
        else:
            resolved_up = local_up
            if proxy_strike:
                src = "local-UNRELIABLE-proxy-strike"
                print(f"  [resolve] WARNING: no official outcome after "
                      f"~{poll_attempts * 3}s AND strike was {strike_src}. "
                      f"Local estimate {'UP' if local_up else 'DOWN'} "
                      f"end=${price_after:,.2f} vs start=${slot_open_ref:,.2f} "
                      f"— may NOT match Polymarket. Prefer restarting bot "
                      f"before next slots so Chainlink open is recorded.")
            else:
                src = "chainlink-stream" if cl_stream.ready else "local-estimate"
                print(f"  [resolve] no Gamma outcome yet; {src} "
                      f"{'UP' if local_up else 'DOWN'} "
                      f"end=${price_after:,.2f} vs start=${slot_open_ref:,.2f}")

        trade = executor.open_and_resolve(trade, resolved_up)
        trade.resolve_source = src
        risk.on_close(stake, trade.pnl)
        trade.bankroll_after = round(risk.bankroll, 2)
        trades.append(trade)
        placed += 1
        flag = "WON " if trade.won else "LOST"
        print(f"  resolved {'UP' if resolved_up else 'DOWN'} [{src}] "
              f"-> {flag} pnl={trade.pnl:+.2f} "
              f"bankroll=${risk.bankroll:.2f} "
              f"(consec_loss={risk.consecutive_losses})")
        print(f"  trade time: {trade.time_et or trade.time_utc}  "
              f"slot: {trade.question or trade.slug}")
        if trade.event_url:
            print(f"  verify: {trade.event_url}")
        wallet_usd = None
        if not paper:
            print("  note: live wallet PnL is on Polymarket; local bankroll is estimate")
            if live_client is not None:
                try:
                    bal = live_client.get_usdc_balance(refresh=True)
                    if bal.get("ok") and bal.get("balance_usd") is not None:
                        wallet_usd = bal["balance_usd"]
                        print(f"  Polymarket wallet USDC now: ${wallet_usd:,.2f}")
                except Exception as e:
                    print(f"  [warn] wallet balance refresh: {e}")
        persist(trades, risk, skips)
        write_status(
            phase="resolved",
            btc_price=round(price_after, 2),
            last_result=("WON" if trade.won else "LOST"),
            last_pnl=trade.pnl,
            bankroll=round(risk.bankroll, 2),
            side=trade.side,
            question=question,
            end_ts=None,
            trade_event="resolved",
            trade_event_id=f"resolved-{trade.round_id}-{int(trade.resolved_ts or time.time())}",
            message=f"resolved {official or ('UP' if resolved_up else 'DOWN')} via {src}",
            last_trade_time=trade.time_et or trade.time_utc,
            last_slug=trade.slug,
            last_event_url=trade.event_url,
            time_et=trade.time_et,
            slug=trade.slug,
            event_url=trade.event_url,
            stake=trade.stake_usd,
            mkt=trade.entry_price,
            wallet_balance_usd=wallet_usd,
            wallet_ok=wallet_usd is not None if not paper else None,
        )

    cl_stream.stop()
    stats = persist(trades, risk, skips)
    if not risk.halted:
        write_status(phase="stopped", message="run complete")
    print("\n=== SUMMARY ===")
    print(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["paper", "paper_live", "live"],
                    default=config.MODE, help="overrides MODE from .env")
    ap.add_argument("--strategy", choices=["mid", "late_scalp", "hybrid"],
                    default=None,
                    help="mid | late_scalp | hybrid (mid first, then late same slot)")
    ap.add_argument("--rounds", type=int, default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--bankroll", type=float, default=None,
                    help="override starting paper balance for this run")
    ap.add_argument("--bet", type=float, default=None,
                    help="fixed USD amount to stake per trade "
                         "(sets both the base and max trade size)")
    ap.add_argument("--reset", action="store_true",
                    help="clear previous trades/stats/status before running")
    args = ap.parse_args()
    config.MODE = args.mode
    if args.strategy:
        config.STRATEGY = args.strategy
    if args.bankroll is not None:
        config.STARTING_BANKROLL = args.bankroll
    if args.bet is not None:
        config.BASE_TRADE_USD = args.bet
        config.MAX_TRADE_USD = args.bet
        config.FIXED_STAKE_USD = args.bet
        if config.MAX_EXPOSURE_USD < args.bet:
            config.MAX_EXPOSURE_USD = args.bet
    if args.reset:
        reset_state()

    if args.mode == "paper":
        run_sim(rounds=args.rounds or 200, sleep_between=args.sleep, seed=args.seed)
    elif args.mode == "paper_live":
        run_realtime(rounds=args.rounds or 100, paper=True)
    elif args.mode == "live":
        # Default to very few live rounds; override with --rounds if needed.
        run_realtime(rounds=args.rounds or 5, paper=False)
