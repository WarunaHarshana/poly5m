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
import asyncio
import json
import math
import os
import time
from types import SimpleNamespace

import config
import observations
import paths
from price_feed import SimulatedPriceFeed, LivePriceFeed, ChainlinkDataStream
from signal_engine import compute_signal, count_strike_crossings, fair_value
from risk import RiskManager
from executor import PaperExecutor, Trade, stamp_trade_times
from decision_journal import log_decision

# Parallel timeframe instances MUST NOT share state files or they overwrite
# each other's trades/stats. STATE_SUBDIR (or --timeframe on a non-5m run)
# gives each instance its own directory; the 5m default keeps the original
# paths so existing history and the dashboard are unaffected.
_STATE_SUB = (os.getenv("STATE_SUBDIR") or "").strip()
if not _STATE_SUB and getattr(config, "TIMEFRAME", "5m") != "5m":
    _STATE_SUB = config.TIMEFRAME
# paths.state_dir honours STATE_ROOT so a Railway volume (e.g. /data) holds the
# archive instead of the container's ephemeral filesystem.
STATE_DIR = paths.state_dir(_STATE_SUB)
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


def _set_session_id(session_id: str) -> None:
    """Adopt an existing session id (used when resuming after a restart)."""
    global _session_id
    _session_id = session_id


# ---------------------------------------------------------------------------
# Single-instance lock
# ---------------------------------------------------------------------------
# Two bots sharing one state directory interleave writes to trades.json and
# stats.json and corrupt the account: each keeps its own in-memory bankroll and
# overwrites the other's. That is easy to trigger by accident — the supervisor
# runs a bot AND the dashboard has a Start button pointing at the same state.
# A heartbeat lock is used rather than an OS file lock because it works the
# same on Windows and Linux and survives a hard kill (the entry simply goes
# stale and the next process takes it).
LOCK_PATH = os.path.join(STATE_DIR, "bot.lock")
LOCK_STALE_S = float(os.getenv("BOT_LOCK_STALE_S", "90"))
LOCK_REFRESH_S = float(os.getenv("BOT_LOCK_REFRESH_S", "20"))


def _read_lock() -> dict | None:
    data = read_json_safe(LOCK_PATH, default=None)
    return data if isinstance(data, dict) else None


def _write_lock() -> None:
    _atomic_write_json(LOCK_PATH, {
        "pid": os.getpid(),
        "heartbeat": time.time(),
        "mode": config.MODE,
        "session_id": _ensure_session_id(),
        "state_dir": STATE_DIR,
    })


def acquire_lock(force: bool = False) -> None:
    """Claim this state directory, or exit with an explanation."""
    held = _read_lock()
    if held and not force:
        age = time.time() - float(held.get("heartbeat") or 0)
        if age < LOCK_STALE_S and held.get("pid") != os.getpid():
            raise SystemExit(
                f"Another bot is already trading this state directory "
                f"(pid {held.get('pid')}, last seen {age:.0f}s ago in "
                f"{STATE_DIR}).\n"
                f"Two bots on one state directory overwrite each other's "
                f"trades and bankroll.\n"
                f"Stop the other one first, or pass --force-lock if you are "
                f"certain it is gone.")
        if age >= LOCK_STALE_S:
            print(f"[lock] taking over a stale lock from pid "
                  f"{held.get('pid')} (last heartbeat {age:.0f}s ago)")
    _write_lock()


def release_lock() -> None:
    held = _read_lock()
    if held and held.get("pid") == os.getpid():
        try:
            os.remove(LOCK_PATH)
        except OSError:
            pass


async def _lock_heartbeat() -> None:
    """Keep the lock fresh so a live bot is never mistaken for a dead one."""
    while True:
        await asyncio.sleep(LOCK_REFRESH_S)
        try:
            _write_lock()
        except OSError:
            pass


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


# Cap the *live* blotter. Running 24/7 for months would otherwise grow
# trades.json without bound and make every dashboard poll heavier; the full
# record already lives in the archive, which is never trimmed.
LIVE_BLOTTER_MAX = int(os.getenv("LIVE_BLOTTER_MAX", "400"))


def load_session() -> tuple[list, float, str | None]:
    """Restore the previous run's blotter so a restart continues the account.

    A supervised 24/7 process gets restarted — on crash, on redeploy, when the
    round budget runs out. Without this every restart silently reset the paper
    bankroll to STARTING_BANKROLL and reported a fresh 0-trade session, which
    makes "is this profitable?" unanswerable across restarts.

    Returns (trades, bankroll, session_id). Missing/unreadable state yields
    the cold-start defaults.
    """
    trades: list[Trade] = []
    raw = read_json_safe(TRADES_PATH, default=None)
    if isinstance(raw, list):
        fields = {f for f in Trade.__dataclass_fields__}
        for d in raw:
            if not isinstance(d, dict):
                continue
            try:
                trades.append(Trade(**{k: v for k, v in d.items() if k in fields}))
            except Exception:
                continue
    stats = read_json_safe(STATS_PATH, default=None) or {}
    bankroll = stats.get("bankroll")
    try:
        bankroll = float(bankroll)
    except (TypeError, ValueError):
        bankroll = None
    if bankroll is None or bankroll <= 0:
        # No usable stats: rebuild from the blotter's last settled trade.
        settled = [t for t in trades if t.won is not None and t.bankroll_after]
        bankroll = float(settled[-1].bankroll_after) if settled \
            else float(config.STARTING_BANKROLL)
    session_id = stats.get("session_id") or None
    if len(trades) > LIVE_BLOTTER_MAX:
        trades = trades[-LIVE_BLOTTER_MAX:]
    return trades, bankroll, session_id


def _atomic_write_json(path: str, obj) -> None:
    """Write JSON so a reader never sees a half-written file.

    Plain open(path,"w") truncates first: a dashboard poll landing mid-write
    gets a partial parse, and a process killed while the handle is open can
    leave the file as pure NUL bytes on Windows (observed: status.json,
    1052 bytes, all zeros — permanently unreadable until rewritten).
    Write to a temp file in the same directory, flush+fsync, then os.replace,
    which is atomic on both Windows and POSIX.
    """
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json_safe(path: str, default=None):
    """Read JSON, tolerating a torn/NUL-filled file from an older run."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = f.read()
        if not data or data[:1] == "\x00":
            return default
        return json.loads(data)
    except Exception:
        return default


_status: dict = {}
# Trades placed but not yet settled. Since settlement moved to background
# tasks the main loop keeps writing status for the NEXT slot, which clobbered
# the "holding" snapshot — the dashboard showed phase=waiting/side=None while
# money was actually at risk. This list survives those writes so the UI can
# always show what is open (there can be several at once).
_open_positions: dict = {}


def _register_open(trade, end_ts) -> None:
    _open_positions[str(trade.round_id)] = {
        "round_id": trade.round_id,
        "side": trade.side,
        "entry_price": trade.entry_price,
        "shares": trade.shares,
        "stake_usd": trade.stake_usd,
        "end_ts": end_ts,
        "slug": trade.slug,
        "question": trade.question,
        "event_url": trade.event_url,
        "placed_ts": trade.ts,
        "time_et": trade.time_et,
        "is_maker": bool(getattr(trade, "is_maker", False)),
    }


def _clear_open(trade) -> None:
    _open_positions.pop(str(getattr(trade, "round_id", "")), None)


def write_status(**kw):
    """Merge + persist a live snapshot for the dashboard status bar."""
    _status.update(kw)
    _status["mode"] = config.MODE
    # Always current, never stale-merged like the rest of the snapshot.
    _status["open_positions"] = list(_open_positions.values())
    _status["updated_at"] = time.time()
    try:
        from config import get_time_suitability
        suitability, reason = get_time_suitability()
        _status["time_suitability"] = f"{suitability}: {reason}"
    except Exception:
        pass
    try:
        _atomic_write_json(STATUS_PATH, _status)
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
        _copy_file_safe(os.path.join(STATE_DIR, "bot.log"), os.path.join(copy_dir, "bot.log"))
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
    # Trim the live blotter in place before writing. Running for months would
    # otherwise make trades.json grow without bound and every dashboard poll
    # heavier; archive_records() below has already banked the full history.
    # Only settled trades are dropped — an open position must stay visible.
    if len(trades) > LIVE_BLOTTER_MAX:
        keep = [t for t in trades[:-LIVE_BLOTTER_MAX] if t.won is None]
        del trades[:-LIVE_BLOTTER_MAX]
        trades[:0] = keep
    _atomic_write_json(TRADES_PATH, [t.to_dict() for t in trades])
    settled = [t for t in trades if t.won is not None]
    wins = sum(1 for t in settled if t.won)
    # Split the headline: ARB wins are structural (a paired position pays
    # either way), so blending them with directional momentum trades makes
    # the win rate look like skill it is not.
    mom = [t for t in settled if t.side in ("UP", "DOWN")]
    arb_t = [t for t in settled if t.side == "ARB"]
    mom_wins = sum(1 for t in mom if t.won)
    mom_pnl = sum(t.pnl for t in mom)
    arb_pnl = sum(t.pnl for t in arb_t)
    total_fees = sum(getattr(t, "fee_usd", 0.0) or 0.0 for t in settled)
    maker_n = sum(1 for t in mom if getattr(t, "is_maker", False))
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
        # Momentum-only view (the actual skill question) + arb split
        "momentum_trades": len(mom),
        "momentum_wins": mom_wins,
        "momentum_win_rate": round(100 * mom_wins / len(mom), 1) if mom else 0.0,
        "momentum_pnl": round(mom_pnl, 2),
        "momentum_breakeven_wr": (
            round(100 * (sum(t.entry_price for t in mom) / len(mom)), 1)
            if mom else 0.0),
        "arb_trades": len(arb_t),
        "arb_pnl": round(arb_pnl, 2),
        "fees_paid": round(total_fees, 2),
        "maker_fills": maker_n,
        "maker_fill_rate": round(100 * maker_n / len(mom), 1) if mom else 0.0,
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
    _atomic_write_json(STATS_PATH, stats)
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
    persist(trades, risk, skips)  # clean slate visible immediately after --reset

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


def _now(cl_stream: ChainlinkDataStream | None = None) -> float:
    """Wall time for slot math — prefer Chainlink stream clock (PC often drifts)."""
    if cl_stream is not None and cl_stream.ready:
        return cl_stream.market_now()
    return time.time()


async def _heartbeat_sleep(seconds: float, chunk: float = 5.0):
    """Sleep, refreshing status.json every few seconds.

    A plain sleep here left status.json untouched for up to ~300s while the
    bot waited out a slot. The dashboard then showed STALE during completely
    normal operation, and its server-clock offset — derived from
    status.updated_at — drifted by the full staleness, throwing off every
    countdown on the page.
    """
    if seconds <= 0:
        return
    remaining = float(seconds)
    while remaining > 0:
        step = min(chunk, remaining)
        await asyncio.sleep(step)
        remaining -= step
        if remaining > 0:
            write_status()  # touch updated_at only


async def _sleep_until_slot_end(end_ts, cl_stream: ChainlinkDataStream | None = None):
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
            await _heartbeat_sleep(min(wait + 1.0, cap))
        else:
            # Past end already — don't tight-loop; give Gamma/discovery a moment
            # to roll to the next slug (and let stream clock advance).
            await asyncio.sleep(min(5.0, max(2.0, config.ROUND_SECONDS * 0.02)))
        return
    await asyncio.sleep(min(5.0, config.ROUND_SECONDS))


async def _backoff(seconds):
    """Bounded retry sleep that is skipped in fast/test mode (ROUND_SECONDS<=0)."""
    if config.ROUND_SECONDS <= 0:
        return
    await asyncio.sleep(seconds)


_last_contract_prices = {}  # token_id -> (price, local_ts)

def validate_and_record_quote(token_id: str, q: dict) -> dict:
    """Reject a single anomalous quote spike without wedging the token.

    Only a large jump within a short window is suspicious; books legitimately
    gap late in a slot. The new price is ALWAYS recorded (even when rejected),
    so a genuine repricing is accepted on the next read instead of being
    compared against a stale reference forever. Old entries are pruned so the
    map doesn't grow unbounded across slots.
    """
    if not q or "price" not in q:
        return q
    mkt = float(q["price"])
    now = time.time()
    global _last_contract_prices
    if len(_last_contract_prices) > 64:
        _last_contract_prices = {
            k: v for k, v in _last_contract_prices.items()
            if now - v[1] < 600.0
        }
    prev = _last_contract_prices.get(token_id)
    _last_contract_prices[token_id] = (mkt, now)
    if prev is not None:
        prev_mkt, prev_ts = prev
        delta = abs(mkt - prev_mkt)
        if delta > 0.15 and (now - prev_ts) < 5.0:
            print(f"  [safety] WARNING: Rejected anomalous contract price jump: "
                  f"{prev_mkt:.3f} -> {mkt:.3f} (delta={delta:.3f})")
            raise ValueError(f"Anomalous contract price jump: {prev_mkt:.3f} -> {mkt:.3f}")
    return q

def _quote_side(pub, market, side: str) -> dict | None:
    """Best-ask buy quote for UP or DOWN, or None on failure."""
    try:
        token_id = market["tokens"][side]
        if hasattr(pub, "buy_quote"):
            q = pub.buy_quote(token_id)
        else:
            mid = pub.midpoint(token_id)
            q = {"price": mid, "mid": mid, "best_bid": None,
                 "best_ask": None, "spread": None, "ask_size": None}
        return validate_and_record_quote(token_id, q)
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


async def _try_maker_entry(pub, market, side: str, best_ask: float,
                           remaining: float | None) -> float | None:
    """Rest one tick under the ask; return the fill price if we got filled.

    PAPER FILL RULE (deliberately strict): we only count a fill when the
    observed best ask drops to at or below our resting price — i.e. the
    market actually traded down to us. Simply waiting never produces a fill.

    Still slightly optimistic: it ignores queue position (other bids at our
    price may absorb the flow first), so treat maker fill RATES from paper as
    an upper bound. The fill PRICE and the zero fee are exact.
    """
    if not getattr(config, "MAKER_FIRST_ENABLED", True):
        return None
    tick = 0.01
    ticks = max(1, int(getattr(config, "MAKER_IMPROVE_TICKS", 1) or 1))
    limit = round(float(best_ask) - tick * ticks, 2)
    if limit <= 0 or limit < float(getattr(config, "MIN_ENTRY_PRICE", 0) or 0):
        return None
    wait_s = float(getattr(config, "MAKER_WAIT_SECONDS", 3.0) or 0.0)
    min_rem = float(getattr(config, "MAKER_MIN_REMAINING", 6.0) or 0.0)
    if remaining is not None and remaining - wait_s < min_rem:
        return None  # not enough slot left to wait then still act
    if config.ROUND_SECONDS <= 0:
        wait_s = min(wait_s, 0.05)  # fast/test mode
    deadline = time.time() + wait_s
    while time.time() < deadline:
        await asyncio.sleep(min(0.5, max(0.05, wait_s / 6.0)))
        q = await asyncio.to_thread(_quote_side, pub, market, side)
        if not q:
            continue
        # best_ask is None when the book is one-sided and buy_quote fell back
        # to the midpoint — use the executable price in that case, otherwise
        # the fill check silently never triggers.
        ba = q.get("best_ask")
        if ba is None:
            ba = q.get("price")
        if ba is not None and float(ba) <= limit + 1e-9:
            return limit
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


async def run_realtime(rounds: int = 100, paper: bool = True,
                       resume: bool = False):
    """Real BTC data + real Polymarket quotes for the current 5-minute slot.

    SINGLE strategy: late_momentum — in the last LATE_MOMENTUM window
    (~8-40s left), if BTC moved >= LATE_MOMENTUM_MIN_MOVE from the slot's
    Chainlink strike, buy the side the move points to (entry band + edge
    floor/ceiling + book-quality gates). mid / late_scalp / late_every /
    hybrid were retired 2026-07-17: archive data showed negative live
    expectancy or paper results inflated by simulation bugs.

    Alongside it, a sum-to-one ARB scanner watches every analyzed slot: when
    ask_UP + ask_DOWN < 1 - ARB_MIN_EDGE, buying BOTH sides pays $1/share
    whichever way the slot resolves (structural edge, no prediction).
    Arb execution is PAPER-ONLY — live would need both legs to fill (leg
    risk), which is not handled; in live mode signals are logged only.

    paper=True  -> simulated fills (MODE=paper_live), no wallet/money.
    paper=False -> REAL orders (MODE=live), funded wallet required.

    Resolution (matches Polymarket rules):
      Up iff Chainlink Data Streams BTC/USD at end >= price at start of range.
      Paper PnL prefers Gamma's official outcome; stream used for strike/signal.
    """
    from polymarket_client import PolymarketPublic
    pub = PolymarketPublic()
    cl_stream = ChainlinkDataStream(asset=config.ASSET)
    cl_stream.start()
    feed = LivePriceFeed(asset=config.ASSET, sources=list(config.BTC_PRICE_SOURCES),
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

    if hasattr(config, "apply_strategy_flags"):
        config.apply_strategy_flags()

    # --- late_momentum parameters ---
    late_min = int(getattr(config, "LATE_MOMENTUM_MIN_REMAINING", 8))
    late_max = int(getattr(config, "LATE_MOMENTUM_TIME_LIMIT", 40))
    lm_min_move = float(getattr(config, "LATE_MOMENTUM_MIN_MOVE", 10.0))
    lm_min_entry = float(getattr(config, "LATE_MOMENTUM_MIN_ENTRY", 0.55))
    lm_max_entry = float(getattr(config, "LATE_MOMENTUM_MAX_ENTRY", 0.92))
    lm_min_edge = float(getattr(config, "LATE_MOMENTUM_MIN_EDGE", 0.0) or 0.0)
    lm_max_edge = float(getattr(config, "LATE_MOMENTUM_MAX_EDGE", 0.10) or 0.0)
    late_pause_slots = 0  # cooldown after a loss
    late_pause_n = int(getattr(config, "LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", 0) or 0)
    # Momentum needs the real Chainlink strike — a proxy strike produced fake
    # signals and unreliable local resolution in past sessions.
    require_cl_strike = bool(getattr(config, "LATE_SCALP_REQUIRE_CHAINLINK", True))
    # Entry band for sizing (risk.size_late_scalp reads config globals).
    config.MIN_ENTRY_PRICE = lm_min_entry
    config.MAX_ENTRY_PRICE = lm_max_entry
    config.MIN_SLOT_SECONDS = late_min
    config.MAX_TRADE_REMAINING = late_max

    # --- arb parameters ---
    arb_on = bool(getattr(config, "ARB_TRADE_ENABLED", True))
    arb_min_edge = float(getattr(config, "ARB_MIN_EDGE", 0.02) or 0.02)
    arb_min_rem = int(getattr(config, "ARB_MIN_REMAINING", 20))
    arb_max_stake = float(getattr(config, "ARB_MAX_STAKE_USD", 10.0) or 10.0)

    print(f"[STRATEGY: late_momentum] window {late_min}-{late_max}s left  "
          f"move>=${lm_min_move:.0f}  entry band [{lm_min_entry:.2f},{lm_max_entry:.2f}]")
    print(f"  edge gates: floor {lm_min_edge:+.2f} / ceiling "
          f"{lm_max_edge:.2f} (0=off)  chainlink_strike_required={require_cl_strike}  "
          f"pause_after_loss={late_pause_n}")
    print(f"  [arb] sum-to-one scanner: {'ON' if arb_on else 'off'}  "
          f"min_edge={arb_min_edge:.3f}  max_stake=${arb_max_stake:.2f}  "
          f"{'PAPER exec' if paper else 'LIVE: log-only (leg risk)'}")
    # Print the modes that change WHETHER we trade, not just how. Without this
    # the only evidence that observe-only is live is the absence of trades,
    # which is indistinguishable from a quiet market.
    print(f"  [modes] momentum_observe_only="
          f"{'ON (recording only, no momentum trades)' if getattr(config, 'MOMENTUM_OBSERVE_ONLY', False) else 'off'}"
          f"  min_shares_sizing={'ON' if getattr(config, 'MIN_SHARES_SIZING', False) else 'off'}"
          f"  quote_every_slot={'ON' if getattr(config, 'OBSERVE_QUOTE_ALWAYS', True) else 'off'}")

    label = "PAPER-LIVE (real data, simulated fills)" if paper else "LIVE (REAL MONEY)"
    strat_tag = config.strategy_name()
    print(f"[{label} | {strat_tag}] warming up Chainlink Data Stream "
          f"(Polymarket RTDS {config.ASSET}/usd)...")
    write_status(phase="starting", message="warming up Chainlink Data Stream",
                 strategy=strat_tag)

    if not await cl_stream.wait_ready(timeout=20.0):
        print("  [warn] Chainlink Data Stream slow/unavailable; "
              "falling back to other feeds (NOT official resolution source)")
    try:
        spot = await asyncio.to_thread(feed.price)
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
            bal = await asyncio.to_thread(live_client.get_usdc_balance, refresh=True)
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
    if resume:
        trades, resumed_bankroll, resumed_session = load_session()
        risk.bankroll = resumed_bankroll
        risk.starting_bankroll = resumed_bankroll
        if resumed_session:
            _set_session_id(resumed_session)
        settled_n = sum(1 for t in trades if t.won is not None)
        print(f"[resume] continuing session {_ensure_session_id()}: "
              f"bankroll ${resumed_bankroll:,.2f}, {settled_n} settled trade(s) "
              f"in the blotter (lifetime PnL "
              f"{resumed_bankroll - config.STARTING_BANKROLL:+,.2f})")
    placed = len(trades)
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
        "arb_thin": 0,
    }
    max_slots = max(rounds * 30, 300)  # safety cap so a dead market can't spin forever

    _hb_task = asyncio.create_task(_lock_heartbeat())

    settle_tasks: set = set()
    # One observation row per slot: the retry loop re-evaluates the same slot
    # every ~1s, and 40 near-identical rows would swamp the tuner's sample.
    obs_written: dict = {"end_ts": None}
    last_counted_end_ts = None  # count real slots, not poll iterations
    last_traded_end_ts = None   # momentum: never re-enter a traded slot
    arb_done_end_ts = None      # arb: at most one attempt per slot
    early_seen_end_ts = None    # count slot_too_early once per slot
    time_guard_seen_end_ts = None  # log the time-guard verdict once per slot

    # Write fresh (empty) trades/stats immediately so a --reset start shows a
    # clean dashboard right away — previously the files only appeared after
    # the FIRST settled trade, and the UI kept showing the old session.
    persist(trades, risk, skips)

    async def _settle_trade(trade, stake, shares, order_id, market, question,
                            end_ts, slot_start_time, slot_open_ref, strike_src,
                            price_before, was_late, mkt, edge):
        """Hold to slot end, check live fills, resolve, book PnL, persist.

        Runs as a background task for real slots so the main loop can keep
        analyzing the next slot while Gamma resolution is polled (this used to
        block the loop for up to ~4.5 minutes per trade). Open exposure is
        released in risk.on_close, so the exposure caps still bound overlap.
        """
        nonlocal late_pause_slots
        try:
            hold = (end_ts - _now(cl_stream)) if end_ts else config.ROUND_SECONDS
            print(f"  holding {max(0, hold):.0f}s until the slot resolves...")
            write_status(phase="holding", end_ts=end_ts, side=trade.side,
                         mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                         question=question, slug=trade.slug,
                         message=f"holding {trade.side} @ {mkt:.2f}")
            if config.ROUND_SECONDS > 0 and hold and hold > 0:
                await asyncio.sleep(hold)

            # Live: cancel resting leftovers and check the matched fill amount
            matched_shares = float(shares)
            if not paper and live_client is not None:
                try:
                    if order_id:
                        print(f"  [live fill] cancelling order {order_id} to check fills...")
                        await asyncio.to_thread(live_client.cancel_order, str(order_id))
                        try:
                            c = live_client._client()
                            trades_list = await asyncio.to_thread(c.get_trades)
                            filled = 0.0
                            if trades_list and isinstance(trades_list, list):
                                for tr in trades_list:
                                    # Match on the taker order id only — tr["id"]
                                    # is the TRADE id, never our order id.
                                    if tr.get("taker_order_id") == order_id:
                                        filled += float(tr.get("size") or 0.0)
                                    else:
                                        maker_ords = tr.get("maker_orders") or []
                                        for mo in maker_ords:
                                            if mo.get("order_id") == order_id:
                                                filled += float(mo.get("matched_amount") or 0.0)
                            print(f"  [live fill] matched shares: {filled:.4f} "
                                  f"(requested: {shares:.4f})")
                            matched_shares = filled
                        except Exception as fe:
                            print(f"  [live fill check error] {fe}. "
                                  f"Defaulting to full size.")
                    else:
                        await asyncio.to_thread(live_client.cancel_all)
                except Exception as e:
                    print(f"  cancel cleanup: {e}")

            # Update trade shares/stake based on actual fill size
            if not paper:
                trade.shares = matched_shares
                trade.stake_usd = round(matched_shares * trade.entry_price, 4)
                if matched_shares == 0.0:
                    print("  [live fill] Trade was completely unfilled. "
                          "Setting PnL to 0.")
                    skips["unfilled"] = skips.get("unfilled", 0) + 1
                    risk.trades_today = max(0, risk.trades_today - 1)

            # --- Resolution (Polymarket rules) ---
            # Up iff end_price >= start_price on Chainlink Data Streams BTC/USD.
            # ALWAYS prefer official Gamma outcome (matches website). Local
            # compare is a last resort — and UNRELIABLE with a proxy strike.
            price_after = None
            if slot_start_time:
                try:
                    price_after = await asyncio.to_thread(
                        feed.get_slot_close_price, slot_start_time)
                except Exception as e:
                    print(f"  failed to fetch slot close price: {e}")
            if price_after is None:
                try:
                    price_after = await asyncio.to_thread(feed.price)
                except Exception:
                    price_after = price_before
            local_up = price_after >= slot_open_ref

            official = None
            slug = market.get("slug") or ""
            proxy_strike = strike_src in ("exchange-proxy", "warmup-fallback")
            poll_attempts = 90
            if slug:
                write_status(phase="resolving",
                             message="waiting for Polymarket outcome",
                             end_ts=end_ts, question=question, side=trade.side)
                # Brief grace — Gamma often lags end_ts by 10–30s+
                if config.ROUND_SECONDS > 0:
                    await asyncio.sleep(12.0)
                print(f"  [resolve] polling official outcome up to "
                      f"~{poll_attempts * 3}s (strike={strike_src})...")
                for attempt in range(poll_attempts):
                    try:
                        official = await asyncio.to_thread(
                            pub.get_official_btc_updown_outcome, slug)
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
                    await asyncio.sleep(3.0)
                    write_status()  # keep the dashboard alive while polling

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
                    print(f"  [resolve] no Gamma outcome yet; PROVISIONAL "
                          f"{src} {'UP' if local_up else 'DOWN'} "
                          f"end=${price_after:,.2f} vs start=${slot_open_ref:,.2f}"
                          f"\n            local resolution disagreed with "
                          f"Polymarket on 7 of the first 56 trades (near-ties "
                          f"especially) — reconcile.py re-checks this later and "
                          f"corrects the trade and bankroll if it is wrong.")

            trade = executor.open_and_resolve(trade, resolved_up)
            trade.resolve_source = src
            risk.on_close(stake, trade.pnl)
            trade.bankroll_after = round(risk.bankroll, 2)
            trades.append(trade)
            # Loss cooldown (protect bankroll after a hit). Arb losses do not
            # trigger it — a losing "arb" means fills were worse than quoted,
            # not that momentum is out of regime.
            if not trade.won and was_late and late_pause_n > 0:
                late_pause_slots = max(late_pause_slots, late_pause_n)
                print(f"  [late_momentum] LOSS cooldown: skip next "
                      f"{late_pause_slots} late window(s)")
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
                print("  note: live wallet PnL is on Polymarket; "
                      "local bankroll is estimate")
                if live_client is not None:
                    try:
                        bal = await asyncio.to_thread(
                            live_client.get_usdc_balance, refresh=True)
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
        except Exception as e:
            import traceback
            print(f"  [settle] ERROR settling R{trade.round_id}: {e}")
            traceback.print_exc()
            if trade.won is None:
                # Release exposure so one settle failure can't wedge sizing.
                risk.on_close(stake, 0.0)
                persist(trades, risk, skips)
        finally:
            # Position is no longer open however this ended.
            _clear_open(trade)
            write_status()

    async def _try_arb(market, question, end_ts, remaining):
        """Detect and (paper-)execute a sum-to-one arb. One attempt per slot.

        Buys EQUAL shares of UP and DOWN at their asks when the pair costs
        less than $1 after simulated slippage; exactly one leg pays $1/share
        at resolution, so pnl = shares * (1 - pair_cost) regardless of
        direction. Returns True if a trade was placed.
        """
        nonlocal arb_done_end_ts, placed
        arb = await asyncio.to_thread(_scan_sum_to_one_arb, pub, market)
        if not arb:
            return False
        arb_done_end_ts = end_ts  # one attempt per slot (signal seen)
        print(f"  [arb] sum-to-one edge={arb['edge']:+.3f} "
              f"UP@{arb['ask_up']:.3f}+DOWN@{arb['ask_down']:.3f}="
              f"{arb['sum']:.3f}")
        # Too-good-to-be-true gate: a 15c+ "free" gap is makers pulling quotes
        # (dust levels), not a real fill. R8-style +$5.87 outliers were paper
        # fiction — a live taker would catch pennies of it.
        max_edge = float(getattr(config, "ARB_MAX_EDGE", 0.15) or 0)
        if max_edge > 0 and float(arb["edge"]) > max_edge:
            print(f"  [arb] edge {arb['edge']:+.3f} > {max_edge:.2f} — "
                  f"degenerate/emptying book, not a fillable arb; skip")
            skips["arb_thin"] = skips.get("arb_thin", 0) + 1
            return False
        try:
            _journal(
                _ensure_session_id(),
                "paper_live" if paper else "live",
                "arb",
                phase="arb", action="signal", reason="sum_to_one",
                slug=market.get("slug") or "", question=question,
                ask=arb["sum"], edge=arb["edge"], arb_sum=arb["sum"],
                remaining_s=remaining, shadow=not paper,
            )
        except Exception:
            pass
        if not paper and not bool(getattr(config, "ARB_LIVE_ENABLED", False)):
            print("  [arb] live execution disabled (ARB_LIVE_ENABLED=0) — "
                  "logged only")
            return False
        if risk.halted or not risk.daily_loss_ok():
            return False

        slip = float(getattr(config, "PAPER_FILL_SLIPPAGE", 0.01) or 0.0)
        leg_up = float(arb["ask_up"]) + slip
        leg_dn = float(arb["ask_down"]) + slip
        pair_cost = leg_up + leg_dn
        # REAL taker fees (verified live: crypto_fees_v2, rate*min(p,1-p) per
        # share, per leg). Near 0.5/0.5 books this is ~6.6c/pair — it kills
        # thin arbs, so it must be inside the gate, not discovered live.
        fee_rate = float(market.get("fee_rate")
                         if market.get("fee_rate") is not None
                         else getattr(config, "TAKER_FEE_RATE", 0.07))
        fee_pair = (config.taker_fee_per_share(leg_up, fee_rate)
                    + config.taker_fee_per_share(leg_dn, fee_rate))
        edge_after = 1.0 - pair_cost - fee_pair
        min_locked = max(0.005, float(getattr(config, "ARB_MIN_LOCKED_EDGE", 0.02) or 0.0))
        if edge_after < min_locked:
            print(f"  [arb] locked edge {edge_after:+.3f} < {min_locked:.3f} "
                  f"after slippage+fees (pair=${pair_cost:.3f} "
                  f"fees=${fee_pair:.3f}/pair rate={fee_rate:.2f}); skip")
            skips["arb_thin"] = skips.get("arb_thin", 0) + 1
            return False

        shares = arb_max_stake / pair_cost
        up_sz, dn_sz = arb.get("up_size"), arb.get("down_size")
        if up_sz is None or dn_sz is None:
            # Can't verify depth on both legs — size at the venue minimum
            # only, never full stake into an unverifiable book.
            shares = min(shares, float(getattr(config, "MIN_ORDER_SHARES", 5) or 5))
        else:
            # Take at most half of each visible ask level.
            shares = min(shares, float(up_sz) * 0.5, float(dn_sz) * 0.5)
        min_sh = float(getattr(config, "MIN_ORDER_SHARES", 5) or 5)
        if shares < min_sh:
            if getattr(config, "ENFORCE_MIN_SHARES", True):
                need = round(min_sh * pair_cost, 2)
                if need <= arb_max_stake and need <= risk.bankroll - risk.open_exposure:
                    shares = min_sh
                else:
                    print(f"  [arb] book too thin for venue min "
                          f"({shares:.1f} < {min_sh:.0f} shares); skip")
                    skips["arb_thin"] = skips.get("arb_thin", 0) + 1
                    return False
        shares = round(shares, 2)
        stake = round(shares * pair_cost, 2)
        if stake < 0.5 or stake > risk.bankroll - risk.open_exposure:
            print(f"  [arb] stake ${stake:.2f} not affordable "
                  f"(bankroll ${risk.bankroll:.2f}, open ${risk.open_exposure:.2f}); skip")
            skips["arb_thin"] = skips.get("arb_thin", 0) + 1
            return False

        slot_start_time = (end_ts - config.SLOT_SECONDS) if end_ts else None
        trade = Trade(
            ts=time.time(),
            round_id=placed + 1,
            side="ARB",
            entry_price=round(pair_cost, 4),  # sum of both legs incl. slippage
            shares=shares,                    # PAIRS of contracts
            stake_usd=stake,
            model_prob=1.0,                   # pays whichever way it resolves
            edge=round(edge_after, 4),        # locked edge net of slip + fees
            fee_usd=round(shares * fee_pair, 4),
            slug=market.get("slug") or "",
            question=question,
            slot_start_ts=slot_start_time,
            end_ts=end_ts,
        )
        stamp_trade_times(trade)

        # --- LIVE: both legs FOK, or hold nothing ---
        # Paper cannot answer the only question that matters here (does the
        # second leg actually fill?), because it assumes both do. Live runs
        # measure it; arb_executor bounds the downside while they do.
        if not paper:
            import arb_executor
            live_res = await asyncio.to_thread(
                arb_executor.execute,
                live_client, market,
                float(arb["ask_up"]), float(arb["ask_down"]), shares,
                arb.get("up_size"), arb.get("down_size"),
                arb.get("bid_up"), arb.get("bid_down"),
            )
            print(f"  [arb/live] {live_res.reason}")
            if not live_res.ok:
                # Either nothing was bought, or the stranded leg was unwound.
                # Book the realised unwind cost (0 when no leg filled) and
                # move on — there is no pair to settle.
                if live_res.realized_pnl:
                    risk.on_close(0.0, live_res.realized_pnl)
                    print(f"  [arb/live] unwind cost "
                          f"{live_res.realized_pnl:+.2f}; "
                          f"bankroll ${risk.bankroll:.2f}")
                st = arb_executor.stats()
                print(f"  [arb/live] execution record: "
                      f"{st['both_legs_locked']}/{st['attempts']} locked "
                      f"({st['lock_rate_pct']}%), "
                      f"{st['second_leg_killed_and_unwound']} unwound, "
                      f"net {st['net_pnl']:+.2f}")
                skips["arb_thin"] = skips.get("arb_thin", 0) + 1
                return False

        risk.on_open(stake)
        _register_open(trade, end_ts)
        placed += 1
        print(f"  TRADE PLACED [arb] R{trade.round_id} ARB "
              f"{shares:.2f} pairs @ {pair_cost:.3f} ${stake:.2f} "
              f"(locked edge {edge_after:+.3f}/pair)  "
              f"{trade.time_et or trade.time_utc}")
        try:
            _journal(
                _ensure_session_id(),
                "paper_live" if paper else "live",
                "arb",
                phase="arb", action="trade", reason="placed",
                slug=trade.slug, question=question, side="ARB",
                ask=pair_cost, fill_price=pair_cost, edge=edge_after,
                stake=stake, remaining_s=remaining, shadow=False,
            )
        except Exception:
            pass
        write_status(
            phase="holding", trade_event="placed",
            trade_event_id=f"placed-{trade.round_id}-{int(trade.ts)}",
            end_ts=end_ts, side="ARB", mkt=round(pair_cost, 4),
            edge=round(edge_after, 4), stake=stake, question=question,
            slug=trade.slug, event_url=trade.event_url,
            time_et=trade.time_et, time_utc=trade.time_utc,
            strategy="arb",
            message=f"ARB pair @ {pair_cost:.3f} (edge {edge_after:+.3f})",
        )
        settle_coro = _settle_trade(
            trade=trade, stake=stake, shares=shares, order_id=None,
            market=market, question=question, end_ts=end_ts,
            slot_start_time=slot_start_time, slot_open_ref=0.0,
            strike_src="n/a-arb", price_before=0.0,
            was_late=False, mkt=pair_cost, edge=edge_after,
        )
        if config.ROUND_SECONDS <= 0 or not end_ts:
            await settle_coro
        else:
            _t = asyncio.create_task(settle_coro)
            settle_tasks.add(_t)
            _t.add_done_callback(settle_tasks.discard)
        return True

    while placed < rounds and slots_seen < max_slots:
        # Continuous operation crosses UTC midnight; clear the per-day caps
        # when it does or "trades per day" becomes "trades ever".
        if risk.roll_day():
            print(f"[day] UTC day rolled over — daily counters reset "
                  f"(bankroll ${risk.bankroll:,.2f})")

        if risk.halted:
            print(f"[HALT] {risk.halt_reason}; stopping.")
            write_status(phase="halted", message=risk.halt_reason)
            break

        # Fetch the freshest current 5-minute slot's token IDs.
        # Pass stream clock so a slow PC clock doesn't re-select an ended slot.
        now_clk = _now(cl_stream)
        market = None
        lookup_err = None
        # Tight retry loop (poll every 1s, up to 6 attempts) for creation lag.
        for attempt in range(6):
            try:
                market = await asyncio.to_thread(pub.find_btc_5min_market, now=now_clk)
                if market is not None:
                    break
            except Exception as e:
                lookup_err = e
                if attempt < 5:
                    print(f"  [market lookup] Not found yet "
                          f"(attempt {attempt+1}/6); retrying in 1s...")
                    await asyncio.sleep(1.0)
                    now_clk = _now(cl_stream)

        if market is None:
            lookup_fails += 1
            err_msg = lookup_err or "Market not found"
            print(f"[market lookup failed] {err_msg}; retrying in 15s")
            write_status(phase="searching", message=str(err_msg))
            if lookup_fails >= 20:
                print("[giving up] market not found after 20 tries. Run discover.py.")
                write_status(phase="error", message="market not found after 20 tries")
                break
            await _backoff(15)
            continue

        lookup_fails = 0

        question = market.get("question") or "BTC 5-minute Up/Down"
        end_ts = market.get("end_ts")
        remaining = (end_ts - _now(cl_stream)) if end_ts else None
        # Count SLOTS toward max_slots, not loop iterations. The arb scanner
        # re-polls every ~15s, so iteration-counting hit the 300 cap in ~75
        # minutes and shut down a healthy (just selective) live session with
        # zero trades placed.
        if end_ts is None or end_ts != last_counted_end_ts:
            slots_seen += 1
            last_counted_end_ts = end_ts
        if end_ts is not None and end_ts == last_traded_end_ts:
            # Already traded this slot (settling in background) — never
            # double-enter the same market; wait for the next slot.
            write_status(phase="holding",
                         message="trade open on this slot; waiting for slot end")
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue
        rem_txt = f"{remaining:.0f}s left" if remaining is not None else "no end time"
        print(f"\nslot: {question}  ({rem_txt})")
        # non-empty => walk the full gate stack and record, but never trade
        observe_only = ("momentum_observe_only: recording for the tuner, "
                        "not trading"
                        if getattr(config, "MOMENTUM_OBSERVE_ONLY", False)
                        else "")
        try:
            from config import get_time_suitability
            suitability, reason = get_time_suitability()
            # The retry loop revisits a slot every ~15s; printing the guard
            # verdict each time buries the trading decisions in a 24/7 log.
            first_look = end_ts != time_guard_seen_end_ts
            time_guard_seen_end_ts = end_ts
            if first_look:
                print(f"  [Time Guard] Suitability: {suitability} - {reason}")
            if suitability == "AVOID":
                # Walk the slot through the full gate stack but refuse to
                # trade it. Bailing out here (the old behaviour) meant the
                # avoided hours produced no observations at all, so the tuner
                # could never test whether the guard actually helps — the
                # filter was permanently unfalsifiable. Now it is measured.
                observe_only = f"time_guard: {reason}"
                if first_look:
                    print(f"  [Time Guard] observe-only for this slot "
                          f"(recording features, placing no trade)")
            # A passing time guard must not clear an observe-only mode set for
            # another reason — blanking it here would have quietly limited
            # MOMENTUM_OBSERVE_ONLY to the avoided hours only.
        except Exception:
            pass
        write_status(phase="analyzing", question=question, slug=market.get("slug"),
                     end_ts=end_ts, side=None, edge=None, mkt=None)

        # ---- ARB SCAN: any point in the slot with enough time left ----
        if (arb_on and remaining is not None and remaining >= arb_min_rem
                and end_ts != arb_done_end_ts):
            try:
                if await _try_arb(market, question, end_ts, remaining):
                    continue  # arb placed; next loop pass (momentum may still run)
            except Exception as e:
                print(f"  [arb] scan error: {e}")

        # ---- WINDOW CHECKS (late_momentum: last late_min..late_max s) ----
        min_slot = late_min
        max_rem = late_max
        if remaining is not None and remaining < min_slot:
            print(f"  slot closing ({remaining:.0f}s); waiting for the next slot...")
            skips["slot_closing"] += 1
            write_status(phase="waiting", message="slot closing")
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue
        if remaining is not None and max_rem > 0 and remaining > max_rem:
            wait = remaining - max_rem
            if end_ts != early_seen_end_ts:
                early_seen_end_ts = end_ts
                skips["slot_too_early"] += 1
                print(f"  slot too early ({remaining:.0f}s left); waiting "
                      f"{wait:.0f}s for late_momentum window (max rem {max_rem}s)...")
            write_status(phase="waiting", message="waiting for late window")
            # Sample a price while waiting (builds momentum/vol history) and
            # wake periodically so the arb scanner keeps watching the book.
            try:
                await asyncio.to_thread(feed.price)
            except Exception:
                pass
            if config.ROUND_SECONDS > 0 and wait > 0:
                if arb_on and wait > 20:
                    await _heartbeat_sleep(15.0)  # re-scan arb roughly every 15s
                else:
                    await _heartbeat_sleep(min(wait, max(1.0, wait - 1.5)))
            continue

        remaining = (end_ts - _now(cl_stream)) if end_ts else remaining
        if remaining is not None and remaining < min_slot:
            print(f"  window missed ({remaining:.0f}s left); next slot...")
            skips["slot_closing"] += 1
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        # ---- LATE MOMENTUM PATH ----
        phase_name = "late_momentum"

        # Feature vector for THIS slot, filled in as the gates run and flushed
        # once — traded or skipped — by _flush_obs below. Retries rebuild it
        # from scratch (the loop `continue`s back to here), so only the final
        # evaluation of a slot is ever recorded.
        slot_obs: dict = {
            "slug": market.get("slug"),
            "end_ts": end_ts,
            "slot_start_ts": (end_ts - config.SLOT_SECONDS) if end_ts else None,
            "mode": config.MODE,
            "timeframe": getattr(config, "TIMEFRAME", "5m"),
            "asset": getattr(config, "ASSET", "btc"),
            "session_id": _ensure_session_id(),
            "remaining_s": remaining,
        }
        def _flush_obs(decision: str, skip_reason: str = "") -> None:
            """Record this slot's features exactly once."""
            if not slot_obs.get("end_ts") or obs_written.get("end_ts") == end_ts:
                return
            obs_written["end_ts"] = end_ts
            slot_obs["decision"] = decision
            slot_obs["skip_reason"] = skip_reason
            observations.record(slot_obs)

        # Stagger entry: if we just entered the late window, add a tiny random
        # delay (100-400ms) to reduce API congestion and quote volatility.
        if remaining is not None and remaining >= late_max - 0.5:
            import random
            delay = random.uniform(0.1, 0.4)
            await asyncio.sleep(delay)
            remaining -= delay

        async def _late_skip(skip_key: str, msg: str, retry: bool = True):
            """Skip this attempt; optionally retry while the window is open.

            Counters increment only when the slot is finally given up —
            counting every ~1s retry poll inflated skip stats ~50x.
            """
            write_status(phase="waiting", message=msg, strategy=phase_name)
            print(f"  [{phase_name}] {msg}")
            rem_now = (end_ts - _now(cl_stream)) if end_ts else remaining
            if retry and rem_now is not None and rem_now > late_min + 2:
                await asyncio.sleep(min(1.2, max(0.4, rem_now - late_min)))
                return "retry"
            skips[skip_key] = skips.get(skip_key, 0) + 1
            _flush_obs("skip", msg)
            await _sleep_until_slot_end(end_ts, cl_stream)
            return "done"

        # Cooldown after a loss (harder to chain blowups).
        if late_pause_slots > 0:
            print(f"  [{phase_name}] pause after loss "
                  f"({late_pause_slots} late window(s) left); skip")
            skips["no_edge"] = skips.get("no_edge", 0) + 1
            late_pause_slots -= 1
            write_status(phase="waiting", message="late pause after loss",
                         strategy=phase_name)
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        slot_start_time = (end_ts - config.SLOT_SECONDS) if end_ts else None
        slot_open_ref = None
        strike_src = "unknown"
        if slot_start_time:
            try:
                slot_open_ref = await asyncio.to_thread(
                    feed.get_slot_start_price, slot_start_time)
                if slot_open_ref is not None:
                    has_cl = bool(
                        cl_stream.ready and (
                            cl_stream.price_at_or_before(
                                float(slot_start_time), max_age_s=60.0)
                            or cl_stream.price_at(
                                float(slot_start_time), max_skew_s=20.0)
                        )
                    )
                    strike_src = ("chainlink-data-stream" if has_cl
                                  else "exchange-proxy")
            except Exception as e:
                print(f"  failed to fetch slot start price: {e}")
        try:
            price_before = await asyncio.to_thread(feed.price)
        except Exception:
            price_before = slot_open_ref or 0.0
        if slot_open_ref is None:
            slot_open_ref = price_before
            strike_src = "warmup-fallback"
        move_usd = abs(price_before - slot_open_ref) if slot_open_ref else 0.0
        slot_obs.update(strike=slot_open_ref, strike_src=strike_src,
                        spot=price_before)
        rem_lbl = f"{remaining:.0f}s left" if remaining is not None else "n/a left"
        print(f"  [{phase_name}] strike=${slot_open_ref:,.2f} [{strike_src}]  "
              f"btc=${price_before:,.2f} move=${move_usd:.1f}  {rem_lbl}")

        # Momentum is strike-relative: a proxy/fallback strike produces fake
        # signals AND unreliable local resolution. Skip the slot.
        if require_cl_strike and strike_src != "chainlink-data-stream":
            if await _late_skip(
                "proxy_strike",
                f"no Chainlink tick for slot open — resolution risk too high "
                f"(strike from {strike_src})",
                retry=False,
            ) == "retry":
                continue
            continue

        # Realized vol + chop from the Chainlink tick buffer, scoped to THIS
        # slot. The polled `feed.recent()` history is sampled irregularly
        # (~15s+ apart, spanning several slots) while _realized_vol_daily
        # assumed a fixed 5s spacing — that overstated vol ~1.7x and made the
        # chop filter compare previous-slot prices to the current strike.
        slot_ticks = []
        rv = None
        if slot_start_time and cl_stream.ready:
            # Fixed recent window so chop/vol mean the same thing on 5m and
            # 15m (crossing counts grow with elapsed time). On 5m this is the
            # whole slot, so behaviour there is unchanged.
            lookback = float(getattr(config, "CHOP_LOOKBACK_S", 300) or 300)
            window_start = max(float(slot_start_time),
                               _now(cl_stream) - lookback)
            slot_ticks = cl_stream.ticks_since(window_start)
            rv = cl_stream.realized_vol_daily_since(window_start)
        if rv is None:
            # Fallback: polled history, but with the spacing it really has.
            prices_poll = feed.recent(int(getattr(config, "MOMENTUM_LOOKBACK", 12)))
            if len(prices_poll) >= 4:
                from signal_engine import _realized_vol_daily
                rv = _realized_vol_daily(
                    prices_poll, float(getattr(config, "ARB_MIN_REMAINING", 15)))
        slot_prices = [px for _ts, px in slot_ticks]
        price_window_start = slot_prices[0] if slot_prices else price_before
        price_now = price_before

        slot_obs["realized_vol_daily"] = rv
        max_chop = int(getattr(config, "LATE_MOMENTUM_MAX_CHOP", 3) or 0)
        if slot_open_ref and len(slot_prices) >= 4:
            slot_obs["chop_crosses"] = count_strike_crossings(
                slot_prices, float(slot_open_ref))

        diff = price_now - slot_open_ref
        # Scale the required lead with sqrt(time left): diffusion noise over
        # the remaining seconds grows ~sqrt(t), so a $10 lead at 40s is much
        # weaker evidence than $10 at 12s. Calibrated so the configured value
        # applies at 20s remaining.
        min_move = float(lm_min_move)
        if remaining is not None and remaining > 0:
            min_move *= math.sqrt(max(float(remaining), 1.0) / 20.0)

        # Record the direction the move points at even when it is too small to
        # trade — that is exactly the counterfactual the tuner needs to answer
        # "would a lower min-move threshold have made money?".
        side_hint = "UP" if diff >= 0 else "DOWN"
        slot_obs.update(move_usd=diff, min_move_req=min_move, side=side_hint)

        # Quote the momentum side BEFORE the cheap gates get a chance to bail.
        #
        # This costs one extra book read on slots we are about to skip, and it
        # buys the only thing that makes those gates testable. Previously the
        # chop filter and the move threshold rejected a slot before any price
        # was recorded, so 46% of a day's labeled observations carried no ask —
        # and a backtest cannot price a trade it has no price for. The tuner
        # could therefore never answer "is MAX_CHOP=3 too strict?", which meant
        # those gates were permanently exempt from the evidence. Same class of
        # blind spot as the time guard, fixed the same way: evaluate and
        # record, then decide.
        q = None
        mkt = None
        if bool(getattr(config, "OBSERVE_QUOTE_ALWAYS", True)):
            try:
                q = await asyncio.to_thread(_quote_side, pub, market, side_hint)
                if q and q.get("price") is not None:
                    mkt = float(q["price"])
                    slot_obs.update(ask=mkt, bid=q.get("bid"),
                                    spread=q.get("spread"),
                                    ask_size=q.get("ask_size"))
            except Exception as e:
                # Observation-only failure: never let it kill the slot. The
                # gates below re-fetch if they actually need a tradeable quote.
                print(f"  [obs] quote for {side_hint} unavailable: {e}")
                q = None

        # Score the model edge for every priced slot, not just the ones that
        # survive the entry band. fair_value is local arithmetic — no API call —
        # and without this the edge floor/ceiling are exactly as untestable as
        # the chop filter was: only 3.6% of rows carried an edge, all of them
        # rows that had already passed the band being questioned.
        fee_rate_obs = float(market.get("fee_rate")
                             if market.get("fee_rate") is not None
                             else getattr(config, "TAKER_FEE_RATE", 0.07))
        fv_obs = None
        if mkt is not None and slot_open_ref:
            try:
                fv_obs = fair_value(
                    spot=float(price_now),
                    strike=float(slot_open_ref),
                    remaining_seconds=float(remaining or 1.0),
                    volatility_daily=float(getattr(config, "VOLATILITY_DAILY", 0.045)),
                    realized_vol_daily=rv,
                    vol_blend=float(getattr(config, "VOL_BLEND", 0.5)),
                )
                prob_obs = (fv_obs.fair_prob_up if side_hint == "UP"
                            else 1.0 - fv_obs.fair_prob_up)
                fee_ps_obs = config.taker_fee_per_share(mkt, fee_rate_obs)
                slot_obs.update(model_prob=prob_obs,
                                edge=prob_obs - mkt,
                                edge_net=prob_obs - mkt - fee_ps_obs,
                                fee_per_share=fee_ps_obs)
            except Exception as e:
                print(f"  [obs] fair value unavailable: {e}")
                fv_obs = None

        # Chop filter: if price flip-flopped across the strike WITHIN this
        # slot, a "lead" is noise, not momentum — skip.
        if max_chop > 0 and slot_open_ref and len(slot_prices) >= 4:
            crossings = slot_obs["chop_crosses"]
            if crossings > max_chop:
                if await _late_skip(
                    "thin_move",
                    f"choppy: {crossings} strike crossings > {max_chop}",
                    retry=True,
                ) == "retry":
                    continue
                continue

        side = None
        if diff >= min_move:
            side = "UP"
        elif diff <= -min_move and config.LATE_MOMENTUM_BIDIRECTIONAL:
            side = "DOWN"

        if not side:
            if await _late_skip(
                "thin_move",
                f"strike gap {diff:+.1f} < min ${min_move:.1f}",
                retry=True,
            ) == "retry":
                continue
            continue

        # side is always side_hint here (both derive from the sign of diff), so
        # the observation quote above is for the right token; only re-fetch if
        # it failed or was disabled.
        token_id = market["tokens"][side]
        if mkt is None:
            try:
                q = await asyncio.to_thread(_quote_side, pub, market, side)
                if not q:
                    raise ValueError("Failed to get quote")
                mkt = float(q["price"])
                slot_obs.update(ask=mkt, bid=q.get("bid"),
                                spread=q.get("spread"),
                                ask_size=q.get("ask_size"))
            except Exception as e:
                if await _late_skip("quote_fail",
                                    f"failed to get quote for {side}: {e}",
                                    retry=True) == "retry":
                    continue
                continue

        min_entry = lm_min_entry
        max_entry = lm_max_entry
        if mkt < min_entry or mkt > max_entry:
            if await _late_skip(
                "quote_range",
                f"{side} ask {mkt:.3f} outside entry band "
                f"[{min_entry:.2f}, {max_entry:.2f}]",
                retry=True,
            ) == "retry":
                continue
            continue

        # Book-quality protections (spread + requote before committing).
        lm_max_spread = float(getattr(config, "LATE_SCALP_MAX_SPREAD", 0)
                              or getattr(config, "MAX_SPREAD", 0) or 0)
        if (lm_max_spread > 0 and q.get("spread") is not None
                and q["spread"] > lm_max_spread):
            if await _late_skip(
                "wide_spread",
                f"spread {q['spread']:.3f} > {lm_max_spread:.3f}",
                retry=True,
            ) == "retry":
                continue
            continue
        if getattr(config, "LATE_SCALP_REQUOTE", True):
            rq_sleep = float(getattr(config, "LATE_SCALP_REQUOTE_SLEEP_S",
                                     0.35) or 0.35)
            if config.ROUND_SECONDS > 0 and rq_sleep > 0:
                await asyncio.sleep(min(rq_sleep, 1.0))
            q2 = await asyncio.to_thread(_quote_side, pub, market, side)
            if q2 and "price" in q2:
                mkt2 = float(q2["price"])
                max_jump = float(getattr(config, "LATE_SCALP_MAX_ASK_JUMP",
                                         0.04) or 0.04)
                if mkt2 > mkt + max_jump:
                    if await _late_skip(
                        "wide_spread",
                        f"requote ask jumped {mkt:.3f}->{mkt2:.3f}",
                        retry=True,
                    ) == "retry":
                        continue
                    continue
                if mkt2 != mkt:
                    # Conservative: size/fill at the fresher price.
                    mkt = mkt2
                    q = q2
                if mkt < min_entry or mkt > max_entry:
                    if await _late_skip(
                        "quote_range",
                        f"after requote ask {mkt:.3f} outside band",
                        retry=True,
                    ) == "retry":
                        continue
                    continue

        fv = fair_value(
            spot=float(price_now),
            strike=float(slot_open_ref),
            remaining_seconds=float(remaining or 1.0),
            volatility_daily=float(getattr(config, "VOLATILITY_DAILY", 0.045)),
            realized_vol_daily=rv,
            vol_blend=float(getattr(config, "VOL_BLEND", 0.5)),
        )
        model_side_prob = fv.fair_prob_up if side == "UP" else (1.0 - fv.fair_prob_up)
        edge = model_side_prob - mkt
        # Net edge after the REAL taker fee (rate*min(p,1-p)/share). At 0.85
        # that's ~1c/share; at 0.60 it's ~2.8c — cheap entries pay the most.
        fee_rate = float(market.get("fee_rate")
                         if market.get("fee_rate") is not None
                         else getattr(config, "TAKER_FEE_RATE", 0.07))
        fee_ps = config.taker_fee_per_share(mkt, fee_rate)
        edge_net = edge - fee_ps
        slot_obs.update(model_prob=model_side_prob, edge=edge,
                        edge_net=edge_net, fee_per_share=fee_ps,
                        side=side, ask=mkt, remaining_s=remaining)

        if lm_min_edge != 0.0 and edge_net < lm_min_edge:
            if await _late_skip(
                "no_edge",
                f"net edge {edge_net:.3f} < floor {lm_min_edge:.3f} "
                f"(model {model_side_prob:.3f} vs ask {mkt:.3f} "
                f"+ fee {fee_ps:.3f}/sh)",
                retry=True,
            ) == "retry":
                continue
            continue
        # Edge CEILING: when the model is far above the book this late in the
        # slot, the book is usually right and the model is stale/overconfident
        # (archive: edge>=0.15 bucket was 75% WR, -$0.15/trade).
        if lm_max_edge > 0 and edge > lm_max_edge:
            if await _late_skip(
                "no_edge",
                f"edge {edge:.3f} > ceiling {lm_max_edge:.3f} "
                f"(model {model_side_prob:.3f} way above ask {mkt:.3f} — "
                f"suspect stale strike/vol; book is usually right)",
                retry=True,
            ) == "retry":
                continue
            continue

        if observe_only:
            # All features are captured; the slot is recorded and left alone.
            print(f"  [late_momentum] {observe_only} — would have taken "
                  f"{side} @ {mkt:.3f} (edge {edge_net:+.3f}); not trading")
            _flush_obs("skip", observe_only)
            write_status(phase="waiting", message=observe_only,
                         strategy=phase_name)
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        # Prefer the venue's own published minimum over the config default, so
        # sizing tracks the market rather than an assumption that could go
        # stale (verified live: orderMinSize = 5 on the BTC 5m markets).
        venue_min = market.get("min_order_size")
        if venue_min and float(venue_min) > 0:
            config.MIN_ORDER_SHARES = float(venue_min)

        stake = risk.size_late_scalp(mkt, edge=max(edge_net, 0.0))

        ask_sz = q.get("ask_size")
        print(f"  [late_momentum] side={side} strike=${slot_open_ref:.2f} "
              f"spot=${price_now:.2f} win_start=${price_window_start:.2f} "
              f"gap=${diff:+.2f} ask={mkt:.3f} edge={edge:.3f} "
              f"(net {edge_net:.3f} after fee {fee_ps:.3f}/sh) stake=${stake:.2f}")
        write_status(
            phase="analyzing", btc_price=round(price_now, 2),
            side=side, model=round(model_side_prob, 4),
            mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
            confidence=round(fv.fair_prob_up, 4),
            end_ts=end_ts, question=question,
            strategy="late_momentum",
        )

        if stake <= 0:
            why = getattr(risk, "last_skip_reason", "") or "no stake"
            if await _late_skip("no_edge", f"size skip ({why})",
                                retry=True) == "retry":
                continue
            continue

        sig = SimpleNamespace(
            side=side,
            confidence=round(fv.fair_prob_up, 4),
            model_prob_up=fv.fair_prob_up,
            realized_vol_daily=rv,
            z_score=fv.z_score,
            sigma_usd=fv.sigma_usd,
            fair_prob_up=fv.fair_prob_up,
        )

        shares = round(stake / mkt, 4)
        # Ensure we don't request more shares than visible ask size when known.
        if ask_sz is not None and ask_sz > 0:
            shares = min(shares, round(ask_sz * 0.5, 4))  # take at most half the level
            stake = round(shares * mkt, 2)
            if stake < 0.5:
                print("  sized below dust after depth cap; skip")
                skips["thin_book"] += 1
                _flush_obs("skip", "sized below dust after depth cap")
                await _sleep_until_slot_end(end_ts, cl_stream)
                continue
            # Venue minimum: if the depth cap pushed us under MIN_ORDER_SHARES,
            # a live order would be rejected — skip instead of sending it.
            min_sh = float(getattr(config, "MIN_ORDER_SHARES", 0) or 0)
            if (getattr(config, "ENFORCE_MIN_SHARES", True)
                    and min_sh > 0 and shares < min_sh):
                print(f"  depth cap left {shares:.2f} shares < venue min "
                      f"{min_sh:.0f}; skip (book too thin)")
                skips["thin_book"] += 1
                _flush_obs("skip", "depth cap below venue minimum shares")
                await _sleep_until_slot_end(end_ts, cl_stream)
                continue

        # --- MAKER-FIRST: try to rest under the ask before crossing ---
        # Makers pay ZERO fee here (feeSchedule.takerOnly), and the fee is
        # several times the spread — so this is the single largest
        # controllable cost in the system.
        is_maker = False
        maker_ok = (paper or getattr(config, "MAKER_FIRST_LIVE", False))
        if maker_ok:
            rem_now = (end_ts - _now(cl_stream)) if end_ts else remaining
            maker_px = await _try_maker_entry(pub, market, side, mkt, rem_now)
            if maker_px is not None:
                is_maker = True
                mkt = maker_px
                print(f"  [maker] filled resting @ {maker_px:.3f} "
                      f"(ask was {q['price']:.3f}) — no taker fee")

        # Pessimistic paper fills — pay ask + slippage (thin end-of-slot books
        # slip the most; skipping this made late paper results unrealistic).
        # Maker fills skip slippage: we set the price, we did not cross.
        fill_px = float(mkt)
        if paper and not is_maker:
            slip = float(getattr(config, "PAPER_FILL_SLIPPAGE", 0.01) or 0.0)
            if slip > 0:
                fill_px = min(0.99, float(mkt) + slip)
                if fill_px != mkt and fill_px > 0:
                    # An order is placed for a SIZE, so slippage makes you pay
                    # more per share — it does not hand you fewer shares.
                    # Holding the dollar amount fixed and dividing by the worse
                    # price silently shrank the order below the venue minimum
                    # (5 shares became 4.94), simulating a fill that the CLOB
                    # would have rejected outright. Keep the size, raise the
                    # cost, and only fall back to the dollar-fixed behaviour
                    # when we are not sizing off the venue minimum.
                    min_sh = float(getattr(config, "MIN_ORDER_SHARES", 0) or 0)
                    hold_size = (getattr(config, "MIN_SHARES_SIZING", False)
                                 and min_sh > 0
                                 and shares <= min_sh + 1e-6)
                    if hold_size:
                        stake = round(shares * fill_px, 2)
                    else:
                        shares = round(stake / fill_px, 4)
                        stake = round(shares * fill_px, 2)
                    print(f"  [paper] pessimistic fill ask {mkt:.3f} + "
                          f"slip {slip:.3f} -> {fill_px:.3f} "
                          f"(shares={shares:.2f} stake=${stake:.2f}"
                          f"{'; size held at venue min' if hold_size else ''})")
        elif is_maker:
            # Keep the share count the depth cap approved and spend LESS —
            # recomputing shares from the cheaper maker price would push size
            # back above the cap (and past the venue-minimum check already
            # done above), buying more than the book was shown to support.
            stake = round(shares * fill_px, 2)

        trade = Trade(
            ts=time.time(),
            round_id=placed + 1,
            side=sig.side,
            entry_price=round(fill_px, 4),
            shares=shares,
            stake_usd=stake,
            model_prob=round(model_side_prob, 4),
            edge=round(edge, 4),
            # Maker fills pay no fee (takerOnly). Rebates are real but NOT
            # credited — they are a proportional daily pool, not guaranteed.
            fee_usd=(0.0 if is_maker else
                     round(shares * config.taker_fee_per_share(fill_px, fee_rate), 4)),
            is_maker=is_maker,
            slug=market.get("slug") or "",
            question=question,
            slot_start_ts=slot_start_time,
            end_ts=end_ts,
        )
        stamp_trade_times(trade)
        # `ask` stays the RAW book ask on every row. Overwriting it with the
        # post-slippage/maker fill would make traded rows systematically
        # cheaper than skipped ones and bias every backtest that compares them.
        slot_obs.update(fill_price=fill_px, is_maker=is_maker, stake_usd=stake)
        _flush_obs("trade")
        risk.on_open(stake)
        _register_open(trade, end_ts)
        trade_event_id = f"placed-{trade.round_id}-{int(trade.ts)}"
        strat_lbl = "late_momentum"
        try:
            _journal(
                _ensure_session_id(),
                "paper_live" if paper else "live",
                strat_lbl,
                phase=strat_lbl,
                action="trade",
                reason="placed",
                slug=trade.slug,
                question=question,
                side=sig.side,
                spot=price_before,
                strike=slot_open_ref,
                strike_src=strike_src,
                move_usd=move_usd,
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
                    q2 = await asyncio.to_thread(_quote_side, pub, market, sig.side)
                    if not q2 or "price" not in q2:
                        raise RuntimeError("failed to re-check quote before order")
                    # Maker orders rest BELOW the ask on purpose — an ask that
                    # is higher than our limit is expected, not a reason to
                    # abort or to chase upward.
                    if not is_maker:
                        if q2["price"] > mkt + 0.02:
                            raise RuntimeError(
                                f"ask moved {mkt:.3f} -> {q2['price']:.3f} before order")
                        if q2["price"] > mkt:
                            # Keep the SHARE COUNT and pay slightly more.
                            # Recomputing shares from the higher ask shrank the
                            # order below the 5-share venue minimum — live
                            # rejected 4.93 shares on the very first real trade.
                            new_mkt = float(q2["price"])
                            new_stake = round(shares * new_mkt, 2)
                            stake_cap = float(getattr(
                                config, "LATE_SCALP_MAX_STAKE_USD", 5.0) or 5.0)
                            if (new_stake > stake_cap + 1e-9
                                    or new_stake > risk.bankroll):
                                raise RuntimeError(
                                    f"ask moved to {new_mkt:.3f}; stake "
                                    f"${new_stake:.2f} would exceed cap")
                            mkt = new_mkt
                            trade.entry_price = round(mkt, 4)
                            # Keep exposure accounting consistent with on_open.
                            risk.open_exposure += new_stake - stake
                            stake = new_stake
                            trade.stake_usd = stake
                except RuntimeError:
                    raise
                except Exception:
                    pass
                resp = await asyncio.to_thread(live_client.buy, token_id, float(mkt),
                                       shares, market["conditionId"])
                print(f"  ORDER PLACED: {resp}")
                if isinstance(resp, dict):
                    order_id = (resp.get("orderID") or resp.get("order_id")
                                or resp.get("id"))
            except Exception as e:
                print(f"  order failed: {e}")
                skips["order_fail"] += 1
                risk.on_close(stake, 0.0)
                _clear_open(trade)
                write_status(phase="waiting",
                             message=f"order failed: {str(e)[:80]}")
                await _sleep_until_slot_end(end_ts, cl_stream)
                continue

        # Settle in the background so the main loop can move on to the next
        # slot — resolution polling used to block the loop for up to ~4.5 min
        # per trade. `placed` counts at placement; exposure caps bound overlap.
        placed += 1
        last_traded_end_ts = end_ts
        settle_coro = _settle_trade(
            trade=trade, stake=stake, shares=shares, order_id=order_id,
            market=market, question=question, end_ts=end_ts,
            slot_start_time=slot_start_time, slot_open_ref=slot_open_ref,
            strike_src=strike_src, price_before=price_before,
            was_late=True, mkt=mkt, edge=edge,
        )
        if config.ROUND_SECONDS <= 0 or not end_ts:
            # Fast/test mode (or no end time): settle inline, deterministic.
            await settle_coro
        else:
            _task = asyncio.create_task(settle_coro)
            settle_tasks.add(_task)
            _task.add_done_callback(settle_tasks.discard)

    if settle_tasks:
        print(f"\n[settle] waiting for {len(settle_tasks)} open trade(s) to resolve...")
        await asyncio.gather(*settle_tasks, return_exceptions=True)
    _hb_task.cancel()
    cl_stream.stop()
    release_lock()
    stats = persist(trades, risk, skips)
    if not risk.halted:
        write_status(phase="stopped", message="run complete")
    print("\n=== SUMMARY ===")
    print(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    import argparse
    import signal
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["paper", "paper_live", "live"],
                    default=config.MODE, help="overrides MODE from .env")
    ap.add_argument("--strategy", default=None,
                    help="accepted for compatibility; every value maps to "
                         "late_momentum (other strategies were retired)")
    ap.add_argument("--asset", choices=["btc", "eth", "sol"], default=None,
                    help="btc | eth | sol (defaults to asset in .env/config)")
    ap.add_argument("--timeframe", choices=["5m", "15m", "4h"], default=None,
                    help="which recurring series to trade (only these exist). "
                         "Non-5m runs use their own state/<tf>/ directory so "
                         "parallel instances do not clobber each other.")
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
    ap.add_argument("--resume", action="store_true",
                    help="continue the previous run's bankroll and blotter "
                         "instead of restarting the paper account (used by "
                         "supervisor.py for 24/7 operation)")
    ap.add_argument("--force-lock", action="store_true",
                    help="start even if another bot appears to hold this "
                         "state directory (only if you know it is gone)")
    args = ap.parse_args()
    config.MODE = args.mode
    if args.asset:
        config.ASSET = args.asset.lower()
        config.reapply_asset_config()
    if args.timeframe and args.timeframe != config.TIMEFRAME:
        # TIMEFRAME shapes the slug prefix and the state paths, both of which
        # are resolved at import time — setting it via CLI here would be too
        # late and would silently write into the 5m state directory.
        raise SystemExit(
            f"--timeframe {args.timeframe} must be set via the environment so "
            f"slug + state paths are built for that series:\n"
            f"  PowerShell:  $env:TIMEFRAME='{args.timeframe}'; "
            f"python bot.py --mode paper_live\n"
            f"  bash:        TIMEFRAME={args.timeframe} "
            f"python bot.py --mode paper_live")
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
        if args.mode != "live":
            config.LATE_SCALP_MAX_STAKE_USD = max(getattr(config, "LATE_SCALP_MAX_STAKE_USD", 0.0), args.bet)
            config.MID_MAX_STAKE_USD = max(getattr(config, "MID_MAX_STAKE_USD", 0.0), args.bet)
    if args.reset:
        reset_state()

    # Claim the state directory before anything writes to it.
    acquire_lock(force=args.force_lock)

    # The supervisor recycles this process with SIGTERM every few hours. Python
    # does not run any cleanup for it by default, so the lock file was left
    # behind with a fresh heartbeat and the *replacement* bot refused to start
    # until it went stale — roughly 90s of crash-restart after every recycle
    # (measured: 31 restarts in 23h where 4 were expected). Releasing the lock
    # on the way out makes a recycle instant.
    def _release_and_exit(signum, _frame):
        print(f"[lock] signal {signum} — releasing {LOCK_PATH}")
        release_lock()
        raise SystemExit(0)

    for _sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if _sig is not None:
            try:
                signal.signal(_sig, _release_and_exit)
            except (ValueError, OSError):
                pass

    if args.mode == "paper":
        run_sim(rounds=args.rounds or 200, sleep_between=args.sleep, seed=args.seed)
    elif args.mode == "paper_live":
        asyncio.run(run_realtime(rounds=args.rounds or 100, paper=True,
                                 resume=args.resume and not args.reset))
    elif args.mode == "live":
        # Default to very few live rounds; override with --rounds if needed.
        asyncio.run(run_realtime(rounds=args.rounds or 5, paper=False,
                                 resume=args.resume and not args.reset))
