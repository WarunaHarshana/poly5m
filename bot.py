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
from types import SimpleNamespace

import config
from price_feed import SimulatedPriceFeed, LivePriceFeed, ChainlinkDataStream
from signal_engine import compute_signal, count_strike_crossings, fair_value
from risk import RiskManager
from executor import PaperExecutor, Trade, stamp_trade_times
from decision_journal import log_decision

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


def _copy_file_safe(src: str, dst: str) -> None:
    import shutil
    try:
        if os.path.isfile(src):
            os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
            shutil.copy2(src, dst)
    except Exception as e:
        print(f"  [archive] copy failed {src} -> {dst}: {e}")


def archive_records(trades, stats: dict) -> int:
    """Append settled trades to permanent history + timestamped copies.

    Returns count newly written to the master JSONL.
    """
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

        # Always refresh CSV + session snapshot (even if no new rows — stats update)
        _rewrite_history_csv()
        settled_dicts = [
            (t.to_dict() if hasattr(t, "to_dict") else t)
            for t in trades if getattr(t, "won", None) is not None
        ]
        snap = {
            "session_id": sid,
            "saved_at": time.time(),
            "mode": config.MODE,
            "strategy": strat,
            "stats": stats,
            "trades": settled_dicts,
        }
        snap_path = os.path.join(SESSIONS_DIR, f"session_{sid}.json")
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, default=str)

        # Live dashboard copies (easy to open without hunting paths)
        _copy_file_safe(TRADES_PATH, os.path.join(ARCHIVE_DIR, "latest_trades.json"))
        _copy_file_safe(STATS_PATH, os.path.join(ARCHIVE_DIR, "latest_stats.json"))
        _copy_file_safe(HISTORY_CSV, os.path.join(ARCHIVE_DIR, "latest_trades.csv"))

        # Timestamped backup folder (never overwritten)
        from datetime import datetime, timezone
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        copy_dir = os.path.join(ARCHIVE_DIR, "copies", f"{sid}_{stamp}")
        os.makedirs(copy_dir, exist_ok=True)
        _copy_file_safe(TRADES_PATH, os.path.join(copy_dir, "trades.json"))
        _copy_file_safe(STATS_PATH, os.path.join(copy_dir, "stats.json"))
        _copy_file_safe(HISTORY_JSONL, os.path.join(copy_dir, "trades_history.jsonl"))
        _copy_file_safe(HISTORY_CSV, os.path.join(copy_dir, "trades_history.csv"))
        _copy_file_safe(snap_path, os.path.join(copy_dir, "session.json"))
        with open(os.path.join(copy_dir, "readme.txt"), "w", encoding="utf-8") as f:
            f.write(
                f"Session {sid}\nSaved {stamp} UTC\nMode={config.MODE} strategy={strat}\n"
                f"Master history: {HISTORY_JSONL}\nMaster CSV: {HISTORY_CSV}\n"
            )

        if new_n:
            print(f"  [archive] +{new_n} trade(s) -> {HISTORY_JSONL}")
        print(f"  [archive] session -> {snap_path}")
        print(f"  [archive] CSV -> {HISTORY_CSV}")
        print(f"  [archive] copy folder -> {copy_dir}")
    except Exception as e:
        print(f"  [archive] WARNING failed to save history: {e}")
        import traceback
        traceback.print_exc()
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


def _scan_sum_to_one_arb(pub, market) -> dict | None:
    """Tier 2: detect ask_UP + ask_DOWN < 1 - fee (structural mispricing)."""
    if not getattr(config, "ARB_SCAN_ENABLED", True):
        return None
    try:
        q_up = _quote_side(pub, market, "UP")
        q_dn = _quote_side(pub, market, "DOWN")
        if not q_up or not q_dn:
            return None
        p_up = float(q_up["price"])
        p_dn = float(q_dn["price"])
        if not (0.0 < p_up < 1.0 and 0.0 < p_dn < 1.0):
            return None
        s = p_up + p_dn
        min_edge = float(getattr(config, "ARB_MIN_EDGE", 0.02) or 0.02)
        edge = 1.0 - s
        if edge >= min_edge:
            return {
                "ask_up": p_up,
                "ask_down": p_dn,
                "sum": s,
                "edge": edge,
                "up_size": q_up.get("ask_size"),
                "down_size": q_dn.get("ask_size"),
            }
    except Exception:
        return None
    return None


def _journal(session_id: str, mode: str, strategy: str, **kw):
    if not getattr(config, "DECISION_JOURNAL", True):
        return
    row = {
        "session_id": session_id,
        "mode": mode,
        "strategy": strategy,
        **kw,
    }
    log_decision(row)


def _pick_late_scalp_favorite(
    pub, market, always: bool = False, spot: float | None = None,
    strike: float | None = None,
) -> tuple[str, dict, dict] | None:
    """Pick the expensive side (market favorite).

    Strict mode: require lead + underdog gates.
    always=True (every-slot mode): pick higher ask, or spot vs strike if tied.
    Returns (side, fav_quote, underdog_quote) or None if book unusable.
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

    always = always or getattr(config, "LATE_SCALP_EVERY_SLOT", False)
    lead = float(getattr(config, "LATE_SCALP_MIN_FAVORITE_LEAD", 0.50) or 0.0)
    max_dog = float(getattr(config, "LATE_SCALP_MAX_UNDERDOG_ASK", 0.14) or 0.0)
    if always:
        lead = min(lead, 0.02)  # tiny lead enough; else use spot
        max_dog = 0.0  # disable underdog gate

    # Favorite = higher ask (market prices it more likely to pay $1).
    if p_up > p_dn and (p_up - p_dn) >= lead:
        if max_dog > 0 and p_dn > max_dog and not always:
            return None
        return "UP", q_up, q_dn
    if p_dn > p_up and (p_dn - p_up) >= lead:
        if max_dog > 0 and p_up > max_dog and not always:
            return None
        return "DOWN", q_dn, q_up

    # Balanced book
    if always:
        # Prefer spot vs strike when available; else slight UP if equal
        if spot is not None and strike is not None and strike > 0:
            if float(spot) >= float(strike):
                return "UP", q_up, q_dn
            return "DOWN", q_dn, q_up
        if p_up >= p_dn:
            return "UP", q_up, q_dn
        return "DOWN", q_dn, q_up
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
                "There is NO profit guarantee - this bot only limits downside size.")
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
    if hasattr(config, "apply_strategy_flags"):
        config.apply_strategy_flags()
    hybrid = config.is_hybrid()
    late_only = config.is_late_scalp() and not hybrid
    prefer_late = bool(hybrid and config.hybrid_prefer_late())
    # Snapshot mid-window params before any late profile mutates them
    mid_min_slot = int(config.MIN_SLOT_SECONDS)
    mid_max_rem = int(config.MAX_TRADE_REMAINING)
    mid_min_entry = float(config.MIN_ENTRY_PRICE)
    mid_max_entry = float(config.MAX_ENTRY_PRICE)
    mid_min_move = float(config.MIN_MOVE_USD)
    mid_edge = float(config.EDGE_THRESHOLD)
    mid_min_ev = float(getattr(config, "MIN_EV", 0) or 0)
    mid_min_conf = float(getattr(config, "MIN_CONFIDENCE", 0) or 0)
    hybrid_mid_edge = float(getattr(config, "HYBRID_MID_MIN_EDGE", 0.10))
    hybrid_mid_move = float(getattr(config, "HYBRID_MID_MIN_MOVE_USD", 15.0))
    hybrid_mid_model = float(getattr(config, "HYBRID_MID_MIN_MODEL", 0.65))
    hybrid_mid_conf = float(getattr(config, "HYBRID_MID_MIN_CONFIDENCE", 0.55))

    late_min = int(getattr(config, "LATE_SCALP_MIN_REMAINING", 10))
    late_max = int(getattr(config, "LATE_SCALP_MAX_REMAINING", 22))
    late_min_entry = float(getattr(config, "LATE_SCALP_MIN_ENTRY", 0.93))
    late_max_entry = float(getattr(config, "LATE_SCALP_MAX_ENTRY", 0.96))
    late_pause_slots = 0  # cooldown after a late loss
    late_pause_n = int(getattr(config, "LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", 3) or 0)
    mid_pause_slots = 0  # cooldown after a mid loss
    mid_pause_n = int(getattr(config, "MID_PAUSE_SLOTS_AFTER_LOSS", 2) or 0)

    if late_only:
        ls_notes = config.apply_late_scalp_profile()
        every0 = bool(getattr(config, "LATE_SCALP_EVERY_SLOT", False))
        print("[STRATEGY: late_scalp] "
              f"{'EVERY 5m SLOT' if every0 else 'selective favorites'} "
              f"(band [{late_min_entry:.2f},{late_max_entry:.2f}]  "
              f"window {late_min}-{late_max}s)")
        for n in ls_notes:
            print(f"  - {n}")
        print(f"  guards: every_slot={every0}  lead>="
              f"{float(getattr(config,'LATE_SCALP_MIN_FAVORITE_LEAD',0.02)):.2f}  "
              f"spot_align="
              f"{bool(getattr(config,'LATE_SCALP_REQUIRE_SPOT_ALIGN',False))}  "
              f"requote={bool(getattr(config,'LATE_SCALP_REQUOTE',False))}  "
              f"pause_after_loss={late_pause_n}  "
              f"max_stake=$"
              f"{float(getattr(config,'LATE_SCALP_MAX_STAKE_USD',5.0)):.2f}")
        if every0:
            print("  [warn] EVERY_SLOT mode: tries to bet almost every window — "
                  "higher risk, paper_live first")
        if not paper:
            print("  [warn] late_scalp on LIVE money - prefer paper_live first")
        risk = RiskManager(config.STARTING_BANKROLL)
        late_min = config.MIN_SLOT_SECONDS
        late_max = config.MAX_TRADE_REMAINING
        late_min_entry = float(config.MIN_ENTRY_PRICE)
        late_max_entry = float(config.MAX_ENTRY_PRICE)
    elif hybrid:
        if prefer_late:
            print("[STRATEGY: hybrid] late preferred; mid only if exceptional "
                  "(else late_scalp same slot)")
            print(f"  mid exceptional bar: edge>={hybrid_mid_edge:.2f}  "
                  f"move>=${hybrid_mid_move:.0f}  "
                  f"model>={hybrid_mid_model:.2f}  conf>={hybrid_mid_conf:.2f}")
        else:
            print("[STRATEGY: hybrid] mid-edge first, then late_scalp on same slot "
                  "if no fill (HYBRID_PREFER_LATE=0)")
        print(f"  mid window {mid_min_slot}-{mid_max_rem}s  "
              f"band [{mid_min_entry:.2f},{mid_max_entry:.2f}]  "
              f"edge>={mid_edge:.2f} move>=${mid_min_move:.0f}")
        if getattr(config, "MID_REQUIRE_CHAINLINK_STRIKE", True):
            print("  mid requires Chainlink Data Stream strike "
                  "(exchange-proxy -> skip / defer late)")
        every = bool(getattr(config, "LATE_SCALP_EVERY_SLOT", False))
        print(f"  late window {late_min}-{late_max}s  "
              f"band [{late_min_entry:.2f},{late_max_entry:.2f}]  "
              f"{'EVERY_SLOT mode' if every else 'selective favorites'}")
        print(f"  late guards: every_slot={every}  lead>="
              f"{float(getattr(config,'LATE_SCALP_MIN_FAVORITE_LEAD',0.02)):.2f}  "
              f"spot_align="
              f"{bool(getattr(config,'LATE_SCALP_REQUIRE_SPOT_ALIGN',False))}  "
              f"requote={bool(getattr(config,'LATE_SCALP_REQUOTE',False))}  "
              f"pause_after_loss={late_pause_n}  "
              f"max_stake=$"
              f"{float(getattr(config,'LATE_SCALP_MAX_STAKE_USD',5.0)):.2f}")
        if every:
            print("  [warn] EVERY_SLOT bets most windows — high activity, "
                  "higher loss risk; paper_live recommended")
        print(f"  discovery stays on slot until "
              f"~{int(getattr(config, 'MARKET_ADVANCE_REMAINING', 5))}s left "
              f"(late handoff safe)")
        print(f"  min shares {getattr(config, 'MIN_ORDER_SHARES', 5):.0f} "
              f"(stake auto-bumped for CLOB limits)")
        if not paper:
            print("  [warn] hybrid on LIVE - paper_live recommended first")
    else:
        print(f"[STRATEGY: mid] quality mid-slot edge band "
              f"[{config.MIN_ENTRY_PRICE:.2f}, {config.MAX_ENTRY_PRICE:.2f}]  "
              f"window {config.MIN_SLOT_SECONDS}-{config.MAX_TRADE_REMAINING}s left")
        print(f"  edge>={config.EDGE_THRESHOLD:.2f}  min_ev>={getattr(config,'MIN_EV',0):.2f}  "
              f"move>=${config.MIN_MOVE_USD:.0f}  conf>={getattr(config,'MIN_CONFIDENCE',0):.2f}")
        print(f"  model in "
              f"[{float(getattr(config,'MID_MIN_MODEL',0.60)):.2f},"
              f"{float(getattr(config,'MID_MAX_MODEL',0.88)):.2f}]  "
              f"fair-value Z>={float(getattr(config,'MID_Z_MIN',0.30)):.2f}  "
              f"noise_cap=$"
              f"{float(getattr(config,'MID_NOISE_MOVE_CAP',40)):.0f}  "
              f"chop_max={int(getattr(config,'MID_MAX_CHOP_CROSSES',3))}  "
              f"paper_slip={float(getattr(config,'PAPER_FILL_SLIPPAGE',0.01)):.2f}")
        print(f"  requote={bool(getattr(config,'MID_REQUOTE',True))}  "
              f"pause_after_loss={mid_pause_n}  "
              f"max_stake=$"
              f"{float(getattr(config,'MID_MAX_STAKE_USD',3.5)):.2f}  "
              f"journal={bool(getattr(config,'DECISION_JOURNAL',True))}  "
              f"arb_scan={bool(getattr(config,'ARB_SCAN_ENABLED',True))}")
        if getattr(config, "MID_REQUIRE_CHAINLINK_STRIKE", True):
            print("  mid requires Chainlink Data Stream strike (no exchange-proxy)")
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
        "proxy_strike": 0,
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
        # When hybrid queued late_scalp for a slot, PIN that end_ts so discovery
        # cannot skip to the next market before the late window (8–35s).
        now_clk = _now(cl_stream)
        try:
            if hybrid and pending_late_end_ts is not None:
                pin_start = int(pending_late_end_ts) - int(config.SLOT_SECONDS)
                market = pub.find_btc_5min_market_at(pin_start, now=now_clk)
                if market is None:
                    # Pinned slot ended or unfetchable — clear and rediscover.
                    print(f"  [hybrid] pinned late slot end={int(pending_late_end_ts)} "
                          f"gone; clearing late queue")
                    pending_late_end_ts = None
                    market = pub.find_btc_5min_market(now=now_clk)
            else:
                market = pub.find_btc_5min_market(now=now_clk)
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
            elif remaining <= late_max and remaining >= late_min:
                # Already inside late window without a mid fill this slot
                pending_late_end_ts = end_ts
                force_late = True
                use_late = True
                print(f"  [hybrid] in late window ({remaining:.0f}s); "
                      f"running late_scalp")

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
                # Late handoff: sleep almost until the window opens so we don't
                # thrash discovery and miss 8–35s. Mid still polls every ~30s.
                if use_late:
                    time.sleep(min(wait, max(1.0, wait - 1.5)))
                else:
                    time.sleep(min(wait, 30.0))
            continue

        remaining = (end_ts - _now(cl_stream)) if end_ts else remaining
        if remaining is not None and remaining < min_slot:
            if hybrid and pending_late_end_ts == end_ts:
                pending_late_end_ts = None
            print(f"  window missed ({remaining:.0f}s left); next slot...")
            skips["slot_closing"] += 1
            _sleep_until_slot_end(end_ts, cl_stream)
            continue

        # ---- LATE SCALP PATH: book favorite + hard quality filters ----
        if use_late:
            # Restore late band for sizing (hybrid keeps mid band in globals)
            config.MIN_ENTRY_PRICE = late_min_entry
            config.MAX_ENTRY_PRICE = late_max_entry
            config.MIN_SLOT_SECONDS = late_min
            config.MAX_TRADE_REMAINING = late_max

            def _late_skip(skip_key: str, msg: str, retry: bool = True):
                """Skip this late attempt; optionally retry while window open."""
                nonlocal pending_late_end_ts
                skips[skip_key] = skips.get(skip_key, 0) + 1
                write_status(phase="waiting", message=msg, strategy="late_scalp")
                print(f"  [late_scalp] {msg}")
                rem_now = (end_ts - _now(cl_stream)) if end_ts else remaining
                if retry and rem_now is not None and rem_now > late_min + 2:
                    time.sleep(min(1.2, max(0.4, rem_now - late_min)))
                    return "retry"
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                _sleep_until_slot_end(end_ts, cl_stream)
                return "done"

            # Cooldown after a late loss (harder to chain blowups).
            if late_pause_slots > 0:
                print(f"  [late_scalp] pause after loss "
                      f"({late_pause_slots} late window(s) left); skip")
                skips["no_edge"] = skips.get("no_edge", 0) + 1
                late_pause_slots -= 1
                if hybrid and pending_late_end_ts == end_ts:
                    pending_late_end_ts = None
                write_status(phase="waiting", message="late pause after loss",
                             strategy="late_scalp")
                _sleep_until_slot_end(end_ts, cl_stream)
                continue

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

            # --- V2 IMPROVEMENT: Skip when Chainlink stream unavailable ---
            require_cl = getattr(config, "LATE_SCALP_REQUIRE_CHAINLINK", True)
            if require_cl and strike_src == "exchange-proxy":
                if _late_skip(
                    "proxy_strike",
                    f"no Chainlink tick for slot open — resolution risk "
                    f"too high (strike from {strike_src})",
                    retry=False,
                ) == "retry":
                    continue
                continue

            # --- V2 IMPROVEMENT: Minimum move threshold ---
            min_move = float(getattr(config, "LATE_SCALP_MIN_MOVE_USD", 15.0) or 0)
            if min_move > 0 and move_usd < min_move:
                if _late_skip(
                    "thin_move",
                    f"move ${move_usd:.1f} < min ${min_move:.1f} "
                    f"(too flat to call direction)",
                    retry=True,
                ) == "retry":
                    continue
                continue

            every = bool(getattr(config, "LATE_SCALP_EVERY_SLOT", False))
            picked = _pick_late_scalp_favorite(
                pub, market, always=every,
                spot=price_before, strike=slot_open_ref,
            )
            if not picked:
                if _late_skip("quote_fail",
                              "no clear favorite (lead/underdog gate)",
                              retry=True) == "retry":
                    continue
                continue
            side, q, q_dog = picked
            mkt = float(q["price"])
            dog_px = float(q_dog["price"]) if q_dog else None
            ask_sz = q.get("ask_size")
            min_entry = late_min_entry
            max_entry = late_max_entry
            dog_txt = f" dog={dog_px:.3f}" if dog_px is not None else ""
            if mkt < min_entry or mkt > max_entry:
                # Do NOT clamp below min_entry — that forced coin-flip bets (e.g. 0.57 loss).
                # Soft favorites: wait/retry; only clamp if slightly above max_entry.
                if every and mkt > max_entry:
                    old = mkt
                    mkt = max_entry
                    print(f"  [late_scalp] EVERY_SLOT clamp ask {old:.3f}->{mkt:.3f} "
                          f"(cap max {max_entry:.2f})")
                else:
                    if _late_skip(
                        "quote_range",
                        f"favorite {side} ask {mkt:.3f}{dog_txt} outside "
                        f"[{min_entry:.2f}, {max_entry:.2f}] "
                        f"(need favorite >= {min_entry:.2f})",
                        retry=True,
                    ) == "retry":
                        continue
                    continue
            print(f"  [late_scalp] candidate {side} ask={mkt:.3f}{dog_txt} "
                  f"in band [{min_entry:.2f},{max_entry:.2f}]"
                  f"{' EVERY_SLOT' if every else ''}")

            # Spot must agree with favorite side (book vs reality filter).
            # V2: ALWAYS check alignment — never bet against actual price direction.
            require_align = getattr(config, "LATE_SCALP_REQUIRE_SPOT_ALIGN", True)
            eps = float(getattr(config, "LATE_SCALP_SPOT_ALIGN_EPS_USD", 3.0) or 0.0)
            if require_align and slot_open_ref and price_before:
                signed = float(price_before) - float(slot_open_ref)
                if side == "UP" and signed < -eps:
                    if _late_skip(
                        "thin_move",
                        f"spot vs strike DOWN (${signed:.1f}) but book favorite UP",
                        retry=True,
                    ) == "retry":
                        continue
                    continue
                if side == "DOWN" and signed > eps:
                    if _late_skip(
                        "thin_move",
                        f"spot vs strike UP (+${signed:.1f}) but book favorite DOWN",
                        retry=True,
                    ) == "retry":
                        continue
                    continue

            # Prefer better payout: ideally fill at/under PREFER_MAX (~0.95).
            # EVERY_SLOT: prefer_max = hard max so we never skip on richness.
            prefer_max = float(getattr(config, "LATE_SCALP_PREFER_MAX", 0.95)
                               or max_entry)
            if every:
                prefer_max = max_entry
            prefer_max = min(prefer_max, max_entry)

            # Re-quote: V2 always re-checks the book before committing.
            do_rq = getattr(config, "LATE_SCALP_REQUOTE", True)
            if do_rq:
                rq_sleep = float(getattr(config, "LATE_SCALP_REQUOTE_SLEEP_S", 0.35)
                                 or 0.35)
                if config.ROUND_SECONDS > 0 and rq_sleep > 0:
                    time.sleep(min(rq_sleep, 1.0))
                picked2 = _pick_late_scalp_favorite(
                    pub, market, always=every,
                    spot=price_before, strike=slot_open_ref,
                )
                if not picked2:
                    if _late_skip("quote_fail", "requote lost favorite",
                                 retry=True) == "retry":
                        continue
                    continue
                side2, q2, _dog2 = picked2
                mkt2 = float(q2["price"])
                max_jump = float(getattr(config, "LATE_SCALP_MAX_ASK_JUMP", 0.04)
                                 or 0.04)
                keep_first = getattr(config, "LATE_SCALP_KEEP_FIRST_ASK", True)
                if side2 != side:
                    if _late_skip("quote_fail",
                                  f"requote side flip {side}->{side2}",
                                  retry=True) == "retry":
                        continue
                    continue
                if mkt2 > mkt + max_jump:
                    if keep_first and min_entry <= mkt <= prefer_max:
                        print(f"  [late_scalp] requote spiked {mkt:.3f}->{mkt2:.3f}; "
                              f"keeping limit @ {mkt:.3f} (better profit)")
                    else:
                        if _late_skip(
                            "wide_spread",
                            f"requote ask jump {mkt:.3f}->{mkt2:.3f} "
                            f"> {max_jump:.3f}",
                            retry=True,
                        ) == "retry":
                            continue
                        continue
                elif mkt2 < mkt:
                    print(f"  [late_scalp] requote improved {mkt:.3f}->{mkt2:.3f}")
                    mkt = mkt2
                    q = q2
                    ask_sz = q2.get("ask_size")
                elif mkt2 > mkt and mkt2 <= prefer_max:
                    mkt = mkt2
                    q = q2
                    ask_sz = q2.get("ask_size")
                if mkt < min_entry or mkt > max_entry:
                    if every:
                        mkt = min(max_entry, max(min_entry, mkt))
                    else:
                        if _late_skip(
                            "quote_range",
                            f"after requote ask {mkt:.3f} outside band",
                            retry=True,
                        ) == "retry":
                            continue
                        continue

            # Skip ultra-rich favorites when we want earlier/cheaper edge
            if (not every) and mkt > prefer_max + 1e-9:
                if _late_skip(
                    "quote_range",
                    f"ask {mkt:.3f} > prefer_max {prefer_max:.2f} "
                    f"(wait for ~0.90-0.95 for better profit)",
                    retry=True,
                ) == "retry":
                    continue
                continue

            max_spread = float(getattr(config, "LATE_SCALP_MAX_SPREAD", 0)
                               or getattr(config, "MAX_SPREAD", 0) or 0)
            # V2: always enforce spread filter (was skipped in EVERY_SLOT).
            if (max_spread > 0 and q.get("spread") is not None
                    and q["spread"] > max_spread):
                if _late_skip("wide_spread",
                              f"spread {q['spread']:.3f} too wide",
                              retry=False) == "retry":
                    continue
                continue
            min_ask_sz = getattr(config, "MIN_ASK_SIZE", 0) or 0
            if min_ask_sz > 0 and ask_sz is not None and ask_sz < min_ask_sz:
                if _late_skip("thin_book",
                              f"ask size {ask_sz:.2f} < {min_ask_sz:.2f}",
                              retry=False) == "retry":
                    continue
                continue

            # --- V2 IMPROVEMENT: Use signal engine for independent edge ---
            fv = fair_value(
                spot=float(price_before),
                strike=float(slot_open_ref),
                remaining_seconds=float(remaining),
                volatility_daily=float(getattr(config, "VOLATILITY_DAILY", 0.045)),
            )
            # Model probability for the chosen side
            model_side_prob = fv.fair_prob_up if side == "UP" else (1.0 - fv.fair_prob_up)
            edge = model_side_prob - mkt  # positive = model thinks we're underpaying
            min_edge = float(getattr(config, "LATE_SCALP_MIN_EDGE", 0.02) or 0)
            print(f"  [late_scalp] model={model_side_prob:.3f} vs ask={mkt:.3f} "
                  f"edge={edge:.3f} (Z={fv.z_score:.2f}, sigma=${fv.sigma_usd:.1f})")
            if min_edge > 0 and edge < min_edge:
                if _late_skip(
                    "no_edge",
                    f"edge {edge:.3f} < min {min_edge:.3f} "
                    f"(model={model_side_prob:.3f} vs ask={mkt:.3f})",
                    retry=True,
                ) == "retry":
                    continue
                continue

            fee = getattr(config, "FEE_BUFFER", 0.0) or 0.0
            stake = risk.size_late_scalp(mkt, edge=edge)
            # EVERY_SLOT: if stake blocked only by max stake cap, allow venue min
            if stake <= 0 and every:
                why0 = getattr(risk, "last_skip_reason", "") or ""
                if "LATE_SCALP_MAX_STAKE" in why0 or "max stake" in why0.lower():
                    min_sh = float(getattr(config, "MIN_ORDER_SHARES", 5) or 5)
                    need = round(min_sh * float(mkt), 2)
                    if need <= risk.bankroll and need > 0:
                        stake = need
                        print(f"  [late_scalp] EVERY_SLOT override stake "
                              f"to min shares ${stake:.2f}")
            dog_txt = f" dog={dog_px:.3f}" if dog_px is not None else ""
            mode_txt = "EVERY_SLOT" if every else "strict favorite"
            print(f"  [late_scalp] side={side} ask={mkt:.3f}{dog_txt} "
                  f"edge={edge:.3f} stake=${stake:.2f}  "
                  f"({mode_txt}; limit buy; model-gated)")
            write_status(phase="analyzing", btc_price=round(price_before, 2),
                         side=side, model=round(model_side_prob, 4),
                         mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                         confidence=round(fv.fair_prob_up, 4),
                         end_ts=end_ts, question=question,
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
            sig = SimpleNamespace(
                side=side,
                confidence=round(fv.fair_prob_up, 4),
                model_prob_up=fv.fair_prob_up,
                realized_vol_daily=None,
                z_score=fv.z_score,
                sigma_usd=fv.sigma_usd,
            )
        else:
            # ---- MID-SLOT PATH (original + quality guards) ----
            config.MIN_ENTRY_PRICE = mid_min_entry
            config.MAX_ENTRY_PRICE = mid_max_entry
            config.MIN_MOVE_USD = mid_min_move
            config.EDGE_THRESHOLD = mid_edge
            config.MIN_EV = mid_min_ev
            config.MIN_CONFIDENCE = mid_min_conf

            # Cooldown after mid loss (pure mid or hybrid mid fill).
            if mid_pause_slots > 0 and not hybrid:
                print(f"  [mid] pause after loss "
                      f"({mid_pause_slots} slot(s) left); skip")
                skips["no_edge"] = skips.get("no_edge", 0) + 1
                mid_pause_slots -= 1
                write_status(phase="waiting", message="mid pause after loss",
                             strategy="mid")
                _sleep_until_slot_end(end_ts, cl_stream)
                continue

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
                        time.sleep(min(rem_now - late_max,
                                       max(1.0, rem_now - late_max - 1.5)))
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
                print("  [warn] strike is NOT Chainlink Data Stream - close-call "
                      "local PnL can disagree with Polymarket. Will wait longer "
                      "for official Gamma resolution.")

            price_before = prices[-1] if prices else None
            journal_extra: dict = {}

            def _mid_no_trade(
                skip_key: str,
                msg: str,
                extra: dict | None = None,
                hard: bool = False,
            ):
                """Skip mid attempt.

                hybrid: queue late_scalp.
                pure mid soft skips (move/edge): recheck later in SAME slot —
                previously we slept until slot end and missed later moves.
                hard=True abandons the slot (proxy strike, etc.).
                """
                nonlocal pending_late_end_ts
                skips[skip_key] = skips.get(skip_key, 0) + 1
                write_status(phase="waiting", message=msg)
                j = {
                    "phase": "mid",
                    "action": "skip",
                    "reason": msg,
                    "slug": market.get("slug") or "",
                    "question": question,
                    "spot": price_before,
                    "strike": slot_open_ref,
                    "strike_src": strike_src,
                    "remaining_s": remaining,
                    "shadow": True,
                }
                j.update(journal_extra)
                if extra:
                    j.update(extra)
                try:
                    _journal(_ensure_session_id(),
                             "paper_live" if paper else "live",
                             "hybrid" if hybrid else "mid", **j)
                except Exception:
                    pass
                if hybrid:
                    pending_late_end_ts = end_ts
                    print(f"  [hybrid] mid skip ({msg}); will try late_scalp "
                          f"when ~{late_max}s left")
                    rem_now = (end_ts - _now(cl_stream)) if end_ts else None
                    # Sleep almost to late window so pin+discovery can't drift
                    if rem_now is not None and rem_now > late_max and config.ROUND_SECONDS > 0:
                        time.sleep(min(rem_now - late_max, max(1.0, rem_now - late_max - 1.5)))
                    return "hybrid_late"
                # Pure mid: recheck often until ~40s left (old logic stopped at
                # mid_min_slot+12 and only got 1-2 peeks per slot).
                rem_now = (end_ts - _now(cl_stream)) if end_ts else None
                recheck_floor = 40  # seconds remaining — keep looking
                if (not hard and rem_now is not None
                        and rem_now > recheck_floor
                        and config.ROUND_SECONDS > 0):
                    wait = min(12.0, max(6.0, rem_now - recheck_floor - 5))
                    print(f"  [mid] soft skip ({msg}); recheck in {wait:.0f}s "
                          f"({rem_now:.0f}s left)")
                    time.sleep(wait)
                    return "retry"
                print(f"  [mid] slot done ({msg})")
                _sleep_until_slot_end(end_ts, cl_stream)
                return "done"

            # Mid signal is strike-relative; exchange-proxy strike produced
            # fake "exceptional" edges that lost to official Gamma (session logs).
            require_cl = getattr(config, "MID_REQUIRE_CHAINLINK_STRIKE", True)
            if require_cl and strike_src != "chainlink-data-stream":
                print(f"  mid blocked: strike source [{strike_src}] "
                      f"(need chainlink-data-stream)")
                # Hard skip: no amount of waiting fixes a missing open tick mid-slot
                _mid_no_trade("proxy_strike", f"strike={strike_src}", hard=True)
                continue

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

            # Tier 2: both-side arb scan (log only — capital-light research path)
            arb = _scan_sum_to_one_arb(pub, market)
            if arb:
                print(f"  [arb] sum-to-one edge={arb['edge']:+.3f} "
                      f"UP@{arb['ask_up']:.3f}+DOWN@{arb['ask_down']:.3f}="
                      f"{arb['sum']:.3f}")
                journal_extra["arb_sum"] = arb["sum"]
                try:
                    _journal(
                        _ensure_session_id(),
                        "paper_live" if paper else "live",
                        "hybrid" if hybrid else "mid",
                        phase="arb",
                        action="signal",
                        reason="sum_to_one",
                        slug=market.get("slug") or "",
                        question=question,
                        ask=arb["sum"],
                        edge=arb["edge"],
                        arb_sum=arb["sum"],
                        remaining_s=remaining,
                        shadow=True,
                    )
                except Exception:
                    pass

            # Tier 1: chop filter — price thrashing the strike kills mid edge
            chop_n = count_strike_crossings(prices, float(slot_open_ref))
            journal_extra["chop_crosses"] = chop_n
            max_chop = int(getattr(config, "MID_MAX_CHOP_CROSSES", 3) or 0)
            if max_chop > 0 and chop_n > max_chop:
                print(f"  chop filter: {chop_n} strike crosses > {max_chop}; skip")
                if _mid_no_trade("thin_move", f"chop crosses={chop_n}") == "hybrid_late":
                    continue
                continue

            move_usd = abs(price_before - slot_open_ref)
            journal_extra["move_usd"] = move_usd
            min_move = getattr(config, "MIN_MOVE_USD", 0.0) or 0.0

            # Fair-value relative move (capped — raw Z*sigma is ~$110 at 200s left
            # and blocked every quiet 5m slot in production).
            z_min = float(getattr(config, "MID_Z_MIN", 0.30) or 0.30)
            fv = fair_value(
                spot=float(price_before),
                strike=float(slot_open_ref),
                remaining_seconds=float(remaining_secs or 1.0),
                volatility_daily=config.VOLATILITY_DAILY,
                vol_blend=config.VOL_BLEND,
                z_min_significant=z_min,
            )
            noise_cap = float(getattr(config, "MID_NOISE_MOVE_CAP", 40.0) or 40.0)
            noise_floor = float(fv.min_move_usd)
            if noise_cap > 0:
                noise_floor = min(noise_floor, noise_cap)
            # Effective min move: fixed $ only unless fair-value noise is ON.
            # Noise floor is optional — when markets are quiet ($5–15 moves), a
            # $22 noise floor + no rechecks = zero trades for hours.
            eff_min_move = float(min_move)
            if getattr(config, "MID_USE_FAIR_VALUE", True) and noise_cap > 0:
                # Blend: light noise bump, never force huge floor
                eff_min_move = max(min_move, min(noise_floor, noise_cap))
            journal_extra.update({
                "fair_prob": fv.fair_prob_up,
                "z_score": fv.z_score,
                "sigma_usd": fv.sigma_usd,
                "min_move_usd": eff_min_move,
            })
            if eff_min_move > 0 and move_usd < eff_min_move:
                print(f"  move ${move_usd:.2f} < min ${eff_min_move:.2f} "
                      f"(Z={fv.z_score:+.2f} sigma=${fv.sigma_usd:.0f} "
                      f"cap=${noise_cap:.0f}); skip")
                if _mid_no_trade("thin_move", "move too small vs noise") == "hybrid_late":
                    continue
                continue
            # Significant Z is advisory only when REQUIRE is off (default paper).
            if (getattr(config, "MID_REQUIRE_SIGNIFICANT", False)
                    and getattr(config, "MID_USE_FAIR_VALUE", True)
                    and not fv.significant):
                print(f"  fair-value not significant |Z|={abs(fv.z_score):.2f} "
                      f"< {z_min:.2f}; skip")
                if _mid_no_trade("thin_move", f"|Z|={abs(fv.z_score):.2f}") == "hybrid_late":
                    continue
                continue

            sig = compute_signal(
                prices,
                slot_start_price=slot_open_ref,
                remaining_seconds=remaining_secs,
                volatility_daily=config.VOLATILITY_DAILY,
                sample_seconds=spacing if spacing > 0 else config.SAMPLE_SECONDS,
                vol_blend=config.VOL_BLEND,
                prefer_window_delta_side=True,
                z_min_significant=z_min,
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
            bid_sz = q.get("bid_size")
            journal_extra["ask_size"] = ask_sz
            journal_extra["spread"] = q.get("spread")
            if min_ask_sz > 0 and ask_sz is not None and ask_sz < min_ask_sz:
                print(f"  ask size {ask_sz:.2f} < MIN_ASK_SIZE {min_ask_sz:.2f}; thin book")
                if _mid_no_trade("thin_book", "thin ask") == "hybrid_late":
                    continue
                continue
            # Tier 2: book imbalance — if bid is tiny vs ask, toxic lift risk
            min_ba = float(getattr(config, "MID_MIN_BID_ASK_RATIO", 0) or 0)
            if (min_ba > 0 and ask_sz and bid_sz is not None
                    and ask_sz > 0 and float(bid_sz) / float(ask_sz) < min_ba):
                print(f"  thin bid/ask ratio {float(bid_sz)/float(ask_sz):.2f} "
                      f"< {min_ba:.2f}; skip")
                if _mid_no_trade("thin_book", "bid/ask imbalance") == "hybrid_late":
                    continue
                continue

            model_side_prob = (
                sig.model_prob_up if sig.side == "UP" else (1 - sig.model_prob_up)
            )
            fair_side_prob = None
            if sig.fair_prob_up is not None:
                fair_side_prob = (
                    sig.fair_prob_up if sig.side == "UP" else (1.0 - sig.fair_prob_up)
                )

            # Mid quality: model range (skip soft + overconfident CDF tails).
            mid_lo = float(getattr(config, "MID_MIN_MODEL", 0.60) or 0.0)
            mid_hi = float(getattr(config, "MID_MAX_MODEL", 0.88) or 1.0)
            if mid_lo > 0 and model_side_prob < mid_lo:
                if _mid_no_trade(
                    "no_edge",
                    f"model {model_side_prob:.2f} < MID_MIN_MODEL {mid_lo:.2f}",
                ) == "hybrid_late":
                    continue
                continue
            if mid_hi < 1.0 and model_side_prob > mid_hi:
                if _mid_no_trade(
                    "no_edge",
                    f"model {model_side_prob:.2f} > MID_MAX_MODEL {mid_hi:.2f} "
                    f"(overconfident)",
                ) == "hybrid_late":
                    continue
                continue
            # Cheap ask + very high model = classic false edge (archive losses).
            cheap_ask = float(getattr(config, "MID_CHEAP_ASK", 0.38) or 0.0)
            cheap_max_model = float(getattr(config, "MID_CHEAP_MAX_MODEL", 0.82) or 1.0)
            if cheap_ask > 0 and mkt < cheap_ask and model_side_prob > cheap_max_model:
                if _mid_no_trade(
                    "no_edge",
                    f"cheap overconf ask {mkt:.2f} model {model_side_prob:.2f}",
                ) == "hybrid_late":
                    continue
                continue

            # Requote: skip if ask jumped against us before entry.
            if getattr(config, "MID_REQUOTE", True):
                rq_sleep = float(getattr(config, "MID_REQUOTE_SLEEP_S", 0.40) or 0.0)
                if config.ROUND_SECONDS > 0 and rq_sleep > 0:
                    time.sleep(min(rq_sleep, 1.0))
                try:
                    if hasattr(pub, "buy_quote"):
                        q2 = pub.buy_quote(token_id)
                    else:
                        q2 = q
                    mkt2 = float(q2["price"])
                    max_jump = float(getattr(config, "MID_MAX_ASK_JUMP", 0.02) or 0.02)
                    if mkt2 > mkt + max_jump:
                        if _mid_no_trade(
                            "wide_spread",
                            f"requote ask jumped {mkt:.3f}->{mkt2:.3f}",
                        ) == "hybrid_late":
                            continue
                        continue
                    if mkt2 < min_entry or mkt2 > max_entry:
                        if _mid_no_trade(
                            "quote_range",
                            f"requote ask {mkt2:.3f} outside band",
                        ) == "hybrid_late":
                            continue
                        continue
                    # Conservative: use higher ask for sizing/fill sim.
                    if mkt2 > mkt:
                        mkt = mkt2
                        q = q2
                        ask_sz = q2.get("ask_size")
                except Exception as e:
                    if _mid_no_trade("quote_fail", f"requote failed ({e})") == "hybrid_late":
                        continue
                    continue

            fee = getattr(config, "FEE_BUFFER", 0.0) or 0.0
            # Prefer fair-value side prob for edge when available (Tier 1)
            edge_prob = fair_side_prob if fair_side_prob is not None else model_side_prob
            stake = risk.size_trade(edge_prob, mkt, confidence=sig.confidence)
            edge = edge_prob - mkt - fee
            fair_edge = (
                (fair_side_prob - mkt - fee) if fair_side_prob is not None else edge
            )
            rv_txt = (f" rv={sig.realized_vol_daily:.3f}"
                      if sig.realized_vol_daily is not None else "")
            z_txt = (f" Z={sig.z_score:+.2f}" if sig.z_score is not None else "")
            fair_s = f"{fair_side_prob:.2f}" if fair_side_prob is not None else "n/a"
            print(f"  btc=${price_before:,.2f} strike=${slot_open_ref:,.2f} "
                  f"move=${move_usd:.1f} side={sig.side} model={model_side_prob:.2f} "
                  f"fair={fair_s} ask={mkt:.2f} edge={edge:+.2f} "
                  f"conf={sig.confidence:.2f} stake=${stake:.2f}{z_txt}{rv_txt}")
            write_status(phase="analyzing", btc_price=round(price_before, 2),
                         side=sig.side, model=round(model_side_prob, 4),
                         mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                         confidence=round(sig.confidence, 3),
                         end_ts=end_ts, question=question, strategy="mid")
            if stake <= 0:
                why = getattr(risk, "last_skip_reason", "") or "no qualifying edge"
                print(f"  size skip ({why}) -> skip this slot")
                if _mid_no_trade("no_edge", why, {
                    "side": sig.side, "model_prob": model_side_prob,
                    "fair_prob": fair_side_prob, "ask": mkt, "edge": edge,
                }) == "hybrid_late":
                    continue
                continue
            # Hybrid prefer-late: only take mid when signal is exceptional;
            # otherwise defer this slot to late_scalp (avoids weak mid fills).
            if hybrid and prefer_late:
                ok_mid, mid_why = config.mid_exceptional_ok(
                    edge=edge,
                    move_usd=move_usd,
                    model_side_prob=model_side_prob,
                    confidence=float(sig.confidence or 0.0),
                )
                if not ok_mid:
                    print(f"  [hybrid] mid not exceptional ({mid_why}); "
                          f"prefer late_scalp")
                    if _mid_no_trade("no_edge", f"prefer late: {mid_why}") == "hybrid_late":
                        continue
                    continue
                print(f"  [hybrid] exceptional mid OK ({mid_why}) - taking mid")
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

        # Tier 1: pessimistic paper fills — pay ask + slippage (not optimistic mid)
        fill_px = float(mkt)
        if paper and not use_late:
            slip = float(getattr(config, "PAPER_FILL_SLIPPAGE", 0.01) or 0.0)
            if slip > 0:
                fill_px = min(0.99, float(mkt) + slip)
                if fill_px != mkt:
                    shares = round(stake / fill_px, 4) if fill_px > 0 else shares
                    stake = round(shares * fill_px, 2)
                    print(f"  [paper] pessimistic fill ask {mkt:.3f} + "
                          f"slip {slip:.3f} -> {fill_px:.3f} "
                          f"(shares={shares:.2f} stake=${stake:.2f})")

        trade = Trade(
            ts=time.time(),
            round_id=placed + 1,
            side=sig.side,
            entry_price=round(fill_px, 4),
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
        strat_lbl = "late_scalp" if use_late else "mid"
        # Decision journal: trade intent
        try:
            _journal(
                _ensure_session_id(),
                "paper_live" if paper else "live",
                "hybrid" if hybrid else strat_lbl,
                phase=strat_lbl,
                action="trade",
                reason="placed",
                slug=trade.slug,
                question=question,
                side=sig.side,
                spot=price_before,
                strike=slot_open_ref,
                strike_src=strike_src if not use_late else "",
                move_usd=abs(price_before - slot_open_ref) if (
                    price_before and slot_open_ref) else None,
                remaining_s=remaining,
                model_prob=model_side_prob,
                fair_prob=getattr(sig, "fair_prob_up", None),
                z_score=getattr(sig, "z_score", None),
                ask=mkt,
                fill_price=fill_px,
                edge=edge,
                conf=getattr(sig, "confidence", None),
                stake=stake,
                shadow=False,
            )
        except Exception:
            pass
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
                      f"- may NOT match Polymarket. Prefer restarting bot "
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
        # Loss cooldowns (protect bankroll after a hit).
        if not trade.won:
            if use_late and late_pause_n > 0:
                late_pause_slots = max(late_pause_slots, late_pause_n)
                print(f"  [late_scalp] LOSS cooldown: skip next "
                      f"{late_pause_slots} late window(s)")
            if (not use_late) and mid_pause_n > 0:
                mid_pause_slots = max(mid_pause_slots, mid_pause_n)
                print(f"  [mid] LOSS cooldown: skip next "
                      f"{mid_pause_slots} mid slot(s)")
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
    ap.add_argument("--strategy",
                    choices=["mid", "late_scalp", "late_every", "hybrid"],
                    default=None,
                    help="mid | late_scalp | late_every (bet most 5m slots) | hybrid")
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
    # Normalize menu aliases (late_every -> EVERY_SLOT on, late_scalp selective)
    if hasattr(config, "apply_strategy_flags"):
        config.apply_strategy_flags()
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
