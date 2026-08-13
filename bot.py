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
import os
import time

import config
import order_state
import paths
from price_feed import LivePriceFeed, ChainlinkDataStream
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

    # Fold in any bankroll correction the reconciler owes us. It lives in its
    # own file precisely because persist() rewrites stats.json wholesale from
    # the in-memory bankroll and would otherwise erase the adjustment between
    # two trades. Cleared only after it has been applied here, so it can be
    # neither lost nor double-counted.
    pending_path = os.path.join(STATE_DIR, "pending_adjustment.json")
    pend = read_json_safe(pending_path, default=None)
    if isinstance(pend, dict):
        try:
            delta = float(pend.get("delta") or 0.0)
        except (TypeError, ValueError):
            delta = 0.0
        if abs(delta) > 1e-9:
            bankroll = round(bankroll + delta, 2)
            print(f"[resume] applied a {delta:+.2f} reconciliation adjustment "
                  f"from {len(pend.get('entries') or [])} correction batch(es); "
                  f"bankroll is now ${bankroll:.2f}")
        try:
            os.remove(pending_path)
        except OSError:
            pass

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
# Set by run_realtime once the book feed exists, so write_status can report its
# health without threading the object through every call site.
_book_feed_ref: dict = {"feed": None}
# How every arb scan sourced its quote. The hit rate is the only honest measure
# of whether the socket is doing its job; the per-signal [ws]/[rest] tag only
# fires on the rare scan that finds a gap.
_scan_src: dict = {"ws": 0, "rest": 0, "no_feed": 0}
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
    # Publish the book feed's health. A socket that has silently fallen back to
    # REST behaves identically from the outside, so it has to be stated.
    if _book_feed_ref.get("feed") is not None:
        try:
            st = _book_feed_ref["feed"].status()
            total = _scan_src["ws"] + _scan_src["rest"]
            st["scans_ws"] = _scan_src["ws"]
            st["scans_rest"] = _scan_src["rest"]
            st["ws_hit_rate_pct"] = (round(100.0 * _scan_src["ws"] / total, 1)
                                     if total else None)
            _status["book_feed"] = st
        except Exception:
            pass
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


def _prune_copies(copies_root: str, keep: int) -> None:
    """Keep only the newest `keep` backup folders; delete the rest.

    Unbounded backups on a fixed volume is a slow-motion outage: the bot fills
    the disk, then every state write raises OSError 28 and it crash-loops.
    """
    import shutil
    try:
        entries = [os.path.join(copies_root, d) for d in os.listdir(copies_root)]
        dirs = [d for d in entries if os.path.isdir(d)]
    except OSError:
        return
    if len(dirs) <= keep:
        return
    dirs.sort(key=lambda d: os.path.basename(d))   # names are session_stamp
    for old in dirs[:-keep]:
        try:
            shutil.rmtree(old, ignore_errors=True)
        except OSError:
            pass


def free_disk_mb(path: str) -> float | None:
    """Free space in MB for the filesystem holding `path` (None if unknown)."""
    try:
        import shutil
        return shutil.disk_usage(path).free / (1024 * 1024)
    except Exception:
        return None


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

        # Timestamped backup folder.
        #
        # This used to be written on EVERY settled trade and never pruned, and
        # each copy contained the whole history plus bot.log. That is O(n^2)
        # growth against a fixed volume: on Railway it reached "No space left
        # on device" and crash-looped the bot 19 times. The master JSONL/CSV
        # are the real archive and are appended in place; these copies are a
        # convenience, so they are now capped and never include the log.
        keep = int(getattr(config, "ARCHIVE_COPIES_KEEP", 3) or 0)
        if keep > 0:
            from datetime import datetime, timezone
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            copies_root = os.path.join(ARCHIVE_DIR, "copies")
            copy_dir = os.path.join(copies_root, f"{sid}_{stamp}")
            os.makedirs(copy_dir, exist_ok=True)
            _copy_file_safe(TRADES_PATH, os.path.join(copy_dir, "trades.json"))
            _copy_file_safe(STATS_PATH, os.path.join(copy_dir, "stats.json"))
            _copy_file_safe(HISTORY_JSONL,
                            os.path.join(copy_dir, "trades_history.jsonl"))
            _copy_file_safe(HISTORY_CSV,
                            os.path.join(copy_dir, "trades_history.csv"))
            _copy_file_safe(snap_path, os.path.join(copy_dir, "session.json"))
            with open(os.path.join(copy_dir, "readme.txt"), "w",
                      encoding="utf-8") as f:
                f.write(
                    f"Session {sid}\nSaved {stamp} UTC\n"
                    f"Mode={config.MODE} strategy={strat}\n"
                    f"Master history: {HISTORY_JSONL}\nMaster CSV: {HISTORY_CSV}\n"
                )
            _prune_copies(copies_root, keep)

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


# Polymarket's CLOB refuses orders from these on legal grounds, not technical
# ones. Being wrong in the cautious direction only costs a warning.
GEOBLOCKED_LOCS = {"US"}


def egress_location(timeout: float = 6.0) -> dict:
    """Which country the VENUE will think we are in, before we ask it to trade.

    Polymarket geoblocks by IP and enforces it on the ORDER, so the failure
    arrives as an HTTP 403 on a leg — indistinguishable, in any aggregate, from
    a leg the market simply did not fill. Eleven live attempts were spent
    learning that: every one was recorded as "first leg killed, no risk taken",
    which is true and also completely misleading, because it reads as a verdict
    on the edge when no order ever reached the book.

    One request at startup turns that back into a deployment fact.
    """
    try:
        import urllib.request
        with urllib.request.urlopen(
                "https://cloudflare.com/cdn-cgi/trace", timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
        kv = dict(ln.split("=", 1) for ln in body.splitlines() if "=" in ln)
        return {"ip": kv.get("ip", ""), "loc": kv.get("loc", ""),
                "colo": kv.get("colo", "")}
    except Exception as e:
        return {"error": str(e)}


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


def _scan_sum_to_one_arb(pub, market, feed=None) -> dict | None:
    """Tier 2: detect ask_UP + ask_DOWN < 1 - fee (structural mispricing).

    Prefers the pushed WebSocket book, which gives BOTH sides from a single
    instant. The REST path reads UP and then DOWN in separate round trips, so a
    pair priced from it may never have existed simultaneously — a correctness
    problem, not just a latency one. REST remains the fallback so a socket
    outage degrades the bot rather than stopping it.
    """
    if not getattr(config, "ARB_SCAN_ENABLED", True):
        return None
    try:
        p_up = p_dn = None
        up_sz = dn_sz = None
        # The BIDS are what an unwind sells into. Without them arb_executor
        # falls back to guessing (ask - 0.02), and a FAK sell priced above the
        # real bid matches nothing — which is how a "successful unwind" ends up
        # still holding a naked leg.
        b_up = b_dn = None
        src = "rest"
        if feed is not None:
            snap = feed.pair(market["tokens"]["UP"], market["tokens"]["DOWN"])
            if snap is not None:
                p_up, p_dn = snap.ask_up, snap.ask_down
                up_sz, dn_sz = snap.size_up, snap.size_down
                b_up, b_dn = snap.bid_up, snap.bid_down
                src = f"ws({snap.age_s * 1000:.0f}ms)"
                _scan_src["ws"] += 1
            else:
                # Counted, because only signal-FINDING scans get logged and
                # those are rare — two REST lines told us nothing about whether
                # the socket was being used 99% of the time or 0% of it.
                _scan_src["rest"] += 1
        else:
            _scan_src["no_feed"] += 1
        if p_up is None or p_dn is None:
            q_up = _quote_side(pub, market, "UP")
            q_dn = _quote_side(pub, market, "DOWN")
            if not q_up or not q_dn:
                return None
            p_up = float(q_up["price"])
            p_dn = float(q_dn["price"])
            up_sz, dn_sz = q_up.get("ask_size"), q_dn.get("ask_size")
            b_up, b_dn = q_up.get("best_bid"), q_dn.get("best_bid")
        if not (0.0 < p_up < 1.0 and 0.0 < p_dn < 1.0):
            return None
        s = p_up + p_dn
        edge = 1.0 - s
        # The gap has to beat the FEE, not a flat threshold. Fees are
        # rate * p * (1-p) per leg, so they peak near 0.50 and collapse at the
        # extremes: a 2c gap at 0.51/0.46 pays 3.5c in fees and loses, while
        # the same 2c gap at 0.95/0.03 pays 0.5c and wins. Measured live, 6 of
        # 7 signals were mid-priced pairs that could never clear — and because
        # a signal burns the slot's one attempt, they were crowding out the
        # lopsided books that are the only place this trade exists.
        #
        # A flat ARB_MIN_EDGE cannot express that; it is kept only as a floor
        # under the fee-aware requirement.
        floor = float(getattr(config, "ARB_MIN_EDGE", 0.02) or 0.0)
        min_locked = float(getattr(config, "ARB_MIN_LOCKED_EDGE", 0.02) or 0.0)
        fees = (config.taker_fee_per_share(p_up)
                + config.taker_fee_per_share(p_dn))
        if edge >= max(floor, fees + min_locked):
            return {
                "ask_up": p_up,
                "ask_down": p_dn,
                "sum": s,
                "edge": edge,
                "up_size": up_sz,
                "down_size": dn_sz,
                "bid_up": b_up,
                "bid_down": b_dn,
                "src": src,
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


async def run_realtime(rounds: int = 100, paper: bool = True,
                       resume: bool = False):
    """Real BTC data + real Polymarket quotes for the current 5-minute slot.

    SINGLE strategy: sum-to-one ARB. When ask_UP + ask_DOWN < 1 - ARB_MIN_EDGE,
    buying BOTH sides pays $1/share whichever way the slot resolves — a
    structural mispricing, not a prediction. Each slot is scanned from the
    moment it is discovered down to ARB_MIN_REMAINING seconds left; the tail is
    too short to fill two legs, so it is waited out.

    Every directional strategy was retired on measurement. mid / late_scalp /
    late_every / hybrid went on 2026-07-17 (negative live expectancy, or paper
    results inflated by simulation bugs); late_momentum followed after 41
    venue-confirmed trades at -$16.57 and 740+ priced slots showing the book
    already prices the signal correctly to within ~1pp before fees.

    paper=True  -> simulated fills (MODE=paper_live), no wallet/money.
    paper=False -> REAL orders (MODE=live), funded wallet required. Both legs
                   go out FOK via arb_executor, which unwinds a stranded leg.

    Resolution: a locked pair pays either way, so the official Gamma outcome
    only LABELS the row — it cannot change the PnL.
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

    # --- arb parameters ---
    arb_on = bool(getattr(config, "ARB_TRADE_ENABLED", True))
    arb_min_edge = float(getattr(config, "ARB_MIN_EDGE", 0.02) or 0.02)
    arb_min_rem = int(getattr(config, "ARB_MIN_REMAINING", 20))
    arb_max_stake = float(getattr(config, "ARB_MAX_STAKE_USD", 10.0) or 10.0)

    print(f"[STRATEGY: sum-to-one arb] scanning each slot from open down to "
          f"{arb_min_rem}s left")
    # This banner used to say "LIVE: log-only (leg risk)" unconditionally,
    # left over from before live arb was wired. With ARB_LIVE_ENABLED=1 the bot
    # really does place FOK orders, so the banner was telling the operator that
    # nothing would trade while real money was about to move.
    if paper:
        _arb_exec_label = "PAPER exec (simulated fills)"
    elif bool(getattr(config, "ARB_LIVE_ENABLED", False)):
        _arb_exec_label = ("LIVE EXEC — REAL ORDERS (FOK both legs, "
                           "unwind on a stranded leg)")
    else:
        _arb_exec_label = "LIVE: log-only (ARB_LIVE_ENABLED=0)"
    print(f"  [arb] sum-to-one scanner: {'ON' if arb_on else 'off'}  "
          f"min_edge={arb_min_edge:.3f}  max_stake=${arb_max_stake:.2f}  "
          f"max_exposure=${float(getattr(config, 'ARB_MAX_EXPOSURE_USD', 0) or 0):.2f}  "
          f"{_arb_exec_label}")
    # Print the modes that change WHETHER we trade, not just how — otherwise a
    # gate silently holding everything back is indistinguishable from a quiet
    # market.
    print(f"  [modes] scan_interval={float(getattr(config, 'ARB_SCAN_INTERVAL_S', 1.0)):.2f}s"
          f"  min_shares_sizing={'ON' if getattr(config, 'MIN_SHARES_SIZING', False) else 'off'}"
          f"  book_feed={'ON' if getattr(config, 'BOOK_FEED_ENABLED', True) else 'off'}"
          f"  min_order_shares={float(getattr(config, 'MIN_ORDER_SHARES', 5)):.0f}")

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

    # Live: say where the venue thinks we are BEFORE anything is sent, so a
    # blocked region is a startup line rather than a slow drip of refusals that
    # look like the market declining to fill us.
    if not paper:
        geo = await asyncio.to_thread(egress_location)
        loc = geo.get("loc") or ""
        if loc:
            blocked = loc in GEOBLOCKED_LOCS
            print(f"  [geo] container egresses from {loc} "
                  f"(Cloudflare PoP {geo.get('colo') or '?'})"
                  + ("\n  *** POLYMARKET REFUSES ORDERS FROM HERE. Every live "
                     "leg will come back 403 'trading restricted in your "
                     "region' and be recorded as an unfilled attempt. This is "
                     "a deployment region problem, not a strategy result. ***"
                     if blocked else "  — not on the known-blocked list"))
            write_status(egress_loc=loc, egress_blocked=blocked)
        else:
            print(f"  [geo] could not determine egress region "
                  f"({geo.get('error') or 'no answer'}); continuing")

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
    # Only the counters the arb path actually writes. The momentum keys that
    # used to live here have been permanently zero since it was retired, and a
    # row of zeroes reads like "we checked and it never happens" rather than
    # "nothing writes this any more".
    skips = {
        # A gap the socket never saw: priced from two separate REST reads, so
        # the pair may never have simultaneously existed. Every signal in the
        # logged history was one of these.
        "arb_rest_phantom": 0,
        # Real pair, refused: fees, depth, venue minimum, or a risk cap.
        "arb_thin": 0,
        # Live order sent, nothing came back filled.
        "unfilled": 0,
    }
    max_slots = max(rounds * 30, 300)  # safety cap so a dead market can't spin forever

    _hb_task = asyncio.create_task(_lock_heartbeat())

    # Pushed order book. Falls back to REST per-scan if it is down or stale,
    # so a socket problem slows the bot rather than stopping it.
    book_feed_client = None
    if bool(getattr(config, "BOOK_FEED_ENABLED", True)):
        try:
            from book_feed import BookFeed
            book_feed_client = BookFeed(
                max_age_s=float(getattr(config, "BOOK_FEED_MAX_AGE_S", 2.0)))
            book_feed_client.start()
            _book_feed_ref["feed"] = book_feed_client
            print(f"  [book] websocket feed starting "
                  f"(max age {book_feed_client.max_age_s:.1f}s; "
                  f"REST fallback stays active)")
        except Exception as e:
            print(f"  [book] websocket feed unavailable ({e}); REST only")
            book_feed_client = None

    settle_tasks: set = set()
    # One observation row per slot: the retry loop re-evaluates the same slot
    # every ~1s, and 40 near-identical rows would swamp the tuner's sample.
    obs_written: dict = {"end_ts": None}
    last_counted_end_ts = None  # count real slots, not poll iterations
    arb_done_end_ts = None      # arb: at most one attempt per slot
    # Boxed so the nested _try_arb can update it without a nonlocal decl.
    rest_phantom_slot = [None]  # last slot whose REST-priced signal was logged
    early_seen_end_ts = None    # count slot_too_early once per slot
    time_guard_seen_end_ts = None  # log the time-guard verdict once per slot

    # Write fresh (empty) trades/stats immediately so a --reset start shows a
    # clean dashboard right away — previously the files only appeared after
    # the FIRST settled trade, and the UI kept showing the old session.
    persist(trades, risk, skips)

    async def _settle_trade(trade, stake, shares, market, question,
                            end_ts, slot_start_time, mkt, edge,
                            verified_fill=None, leg_order_ids=None):
        """Hold to slot end, resolve, book PnL, persist.

        Runs as a background task for real slots so the main loop can keep
        scanning the next slot while Gamma resolution is polled (this used to
        block the loop for up to ~4.5 minutes per trade). Open exposure is
        released in risk.on_close, so the exposure caps still bound overlap.

        An arb pair pays $1 on exactly one leg whichever way the slot lands, so
        the outcome LABELS the row but cannot change its PnL. That is why there
        is no local price comparison here any more: guessing a direction from a
        local close price was wrong on 7 of the first 56 rows, and for an arb it
        buys nothing even when it is right.
        """
        try:
            hold = (end_ts - _now(cl_stream)) if end_ts else config.ROUND_SECONDS
            print(f"  holding {max(0, hold):.0f}s until the slot resolves...")
            write_status(phase="holding", end_ts=end_ts, side=trade.side,
                         mkt=round(mkt, 4), edge=round(edge, 4), stake=stake,
                         question=question, slug=trade.slug,
                         message=f"holding {trade.side} @ {mkt:.2f}")
            if config.ROUND_SECONDS > 0 and hold and hold > 0:
                # Heartbeat, not a raw sleep: holding a pair to slot end can be
                # most of five minutes, and this coroutine is the only thing
                # running once the main loop has finished its rounds. That is
                # exactly the "status.json untouched for ~300s" case.
                await _heartbeat_sleep(hold)

            # Live: cancel resting leftovers and check the matched fill amount.
            # Starts at 0 for live: a fill has to be PROVEN against the venue,
            # not assumed from the fact that we sent an order. (Paper keeps the
            # requested size — its fills are simulated by definition.)
            matched_shares = float(shares) if paper else 0.0
            if verified_fill is not None:
                # Already PROVEN against the venue by whoever placed it. An arb
                # pair is two FOK legs, each positively confirmed by
                # arb_executor before this coroutine is spawned, and a FOK
                # leaves nothing resting — so there is nothing to cancel and
                # nothing left to look up.
                #
                # Re-deriving the fill here from a single `order_id` (which a
                # two-leg pair does not have, and so passed as None) sent it
                # down the "no order id — book nothing" branch below: a pair
                # the venue confirmed on BOTH legs was recorded as UNFILLED
                # with its shares and stake zeroed, while the wallet held the
                # real position.
                matched_shares = float(verified_fill)
            elif not paper:
                # Live, but nobody proved a fill. arb_executor confirms BOTH
                # legs against the venue before this coroutine is spawned, so
                # reaching here means a caller skipped that handoff. Book
                # nothing rather than assume a position exists — a submitted
                # order and a filled order are not the same thing.
                matched_shares = 0.0
                trade.fill_unverified = True
                print("  [live fill] settler got no verified fill — booking "
                      "NOTHING rather than assuming the pair exists")
                if live_client is not None:
                    try:
                        await asyncio.to_thread(live_client.cancel_all)
                    except Exception as e:
                        print(f"  cancel cleanup: {e}")

            # Update trade shares/stake based on actual fill size
            if not paper:
                trade.shares = matched_shares
                trade.stake_usd = round(matched_shares * trade.entry_price, 4)
                # An arb has no single order id — it has one per leg. Record
                # both, so the trail names the orders the venue actually
                # matched instead of an empty string.
                _oid_str = ",".join(str(x) for x in (leg_order_ids or []) if x)
                _st_common = dict(round_id=trade.round_id, slug=trade.slug,
                                  side=trade.side, order_id=_oid_str,
                                  requested_shares=shares,
                                  filled_shares=matched_shares,
                                  price=trade.entry_price, mode=config.MODE)
                if getattr(trade, "fill_unverified", False):
                    order_state.record(
                        order_state.UNVERIFIED, **_st_common,
                        note="no verified fill reached the settler; booked as "
                             "UNFILLED (any real position is unaccounted for)")
                elif matched_shares <= 0.0:
                    order_state.record(order_state.CANCELLED, **_st_common,
                                       note="cancelled with nothing matched")
                elif matched_shares + 1e-9 < shares:
                    order_state.record(order_state.PARTIAL, **_st_common,
                                       note="venue confirmed a partial fill")
                else:
                    order_state.record(order_state.FILLED, **_st_common,
                                       note="venue confirmed the full size")
                if matched_shares == 0.0:
                    print("  [live fill] Trade was completely unfilled. "
                          "Setting PnL to 0.")
                    skips["unfilled"] = skips.get("unfilled", 0) + 1
                    risk.trades_today = max(0, risk.trades_today - 1)

            # --- Resolution label (Polymarket official, or nothing) ---
            # price_after is for the dashboard's BTC readout only; it no longer
            # decides anything, because a locked pair's PnL is already settled
            # by the fills.
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
                    price_after = 0.0

            official = None
            slug = market.get("slug") or ""
            poll_attempts = 90
            if slug:
                write_status(phase="resolving",
                             message="waiting for Polymarket outcome",
                             end_ts=end_ts, question=question, side=trade.side)
                # Brief grace — Gamma often lags end_ts by 10-30s+. Heartbeat
                # through it: a raw sleep here left status.json untouched for
                # the full 12s after every placement, which the dashboard reads
                # as STALE at exactly the moment you are watching a trade.
                if config.ROUND_SECONDS > 0:
                    await _heartbeat_sleep(12.0)
                print(f"  [resolve] polling official outcome up to "
                      f"~{poll_attempts * 3}s...")
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
                print(f"  [resolve] official={official} "
                      f"(BTC ${price_after:,.2f} at close)")
            else:
                # The PnL is already final — one leg of the pair pays $1/share
                # either way — so only the direction label is missing. Say that
                # plainly instead of inventing a direction from a local price
                # compare, which disagreed with Polymarket on 7 of the first 56
                # rows. reconcile.py fills the label in later.
                resolved_up = True
                src = "arb-label-pending"
                print(f"  [resolve] no Gamma outcome yet. PnL is final "
                      f"regardless (a locked pair pays either way); the "
                      f"direction label is provisional and reconcile.py will "
                      f"correct it.")

            trade = executor.open_and_resolve(trade, resolved_up)
            trade.resolve_source = src
            risk.on_close(stake, trade.pnl)
            trade.bankroll_after = round(risk.bankroll, 2)
            trades.append(trade)
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
        arb = await asyncio.to_thread(_scan_sum_to_one_arb, pub, market,
                                      book_feed_client)
        if not arb:
            return False
        # NOTE: the slot is NOT marked done here. It used to be — "one attempt
        # per slot (signal seen)" — which meant the first gap that failed the
        # fee check consumed the slot's only look, and a genuinely profitable
        # pair appearing later in the same slot was never seen. Live data made
        # that expensive: 6 of 7 observed signals were mid-priced pairs that
        # could not clear fees, so the budget was being spent precisely on the
        # ones that cannot work.
        #
        # The slot is marked done where an ORDER is actually attempted, which
        # is what "one attempt per slot" was protecting against in the first
        # place. Rejections that cost nothing no longer end the hunt.
        # Say WHERE the quote came from. Without this there is no way to tell a
        # socket read from a REST fallback, which is the whole point of the
        # feed — and a silently-degraded feed looks exactly like a working one.
        print(f"  [arb] sum-to-one edge={arb['edge']:+.3f} "
              f"UP@{arb['ask_up']:.3f}+DOWN@{arb['ask_down']:.3f}="
              f"{arb['sum']:.3f}  [{arb.get('src', 'rest')}]")

        # A pair is only real if BOTH legs were priced from one instant. The
        # REST fallback reads UP and then DOWN in separate round trips, so a
        # price move between them manufactures a gap that never existed. Across
        # all logged history that is where every single signal came from: 236
        # of 236 from REST, zero from the socket, which was doing most of the
        # scanning. These are measurement error, not mispricings — and acting
        # on one means sending a first leg into a book that was never there.
        src = str(arb.get("src") or "rest")
        if (bool(getattr(config, "ARB_REQUIRE_ATOMIC_BOOK", True))
                and not src.startswith("ws")):
            skips["arb_rest_phantom"] = skips.get("arb_rest_phantom", 0) + 1
            # Log once per slot: the same phantom can persist for many scans.
            if end_ts != rest_phantom_slot[0]:
                rest_phantom_slot[0] = end_ts
                print(f"  [arb] priced from {src.upper()}, not one socket "
                      f"snapshot — the two legs were read in separate round "
                      f"trips, so this gap may never have existed. Logged, "
                      f"not traded (ARB_REQUIRE_ATOMIC_BOOK=1).")
            return False
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
        if not paper:
            _blocked, _win = config.in_live_blackout()
            if _blocked:
                print(f"  [arb] venue maintenance window {_win} UTC — no live "
                      f"orders. A 503 between the two legs is the one path "
                      f"that strands a naked position, and with order status "
                      f"also down we could not even tell. Signal logged only.")
                return False
        if risk.halted or not risk.daily_loss_ok():
            return False
        # Arb used to skip this. size_late_scalp calls max_trades_ok() for
        # momentum, but arb never goes through the sizer, so a live arb could
        # run past MAX_TRADES_PER_DAY unchecked — the cap silently applied to
        # the strategy that no longer trades and not to the one that does.
        if not risk.max_trades_ok():
            print(f"  [arb] {risk.halt_reason or 'max trades/day'}; skip")
            return False

        # Slippage models a SIMULATED fill. A live leg goes out as FOK at the
        # quoted ask, so it fills at that price or not at all — there is no
        # slippage to pay, and charging it here made the gate reject arbs on a
        # price the bot would never have paid. Measured live: a pair at 0.950
        # with 0.030 fees scored -0.000 against the slipped cost and was
        # skipped, while its real locked edge was +0.020 — which is exactly
        # what arb_executor.evaluate() computes from the raw asks moments
        # later, so the two gates disagreed with each other.
        #
        # The protection against a stale quote is the order type, not a made-up
        # cost: if the pair is not really there, the FOK is killed and the
        # attempt is free.
        slip = (float(getattr(config, "PAPER_FILL_SLIPPAGE", 0.01) or 0.0)
                if paper else 0.0)
        leg_up = float(arb["ask_up"]) + slip
        leg_dn = float(arb["ask_down"]) + slip
        pair_cost = leg_up + leg_dn
        # REAL taker fees, per leg: rate * p * (1-p) per share, the venue's
        # documented curve. Near 0.5/0.5 this is ~3.5c/pair — it kills thin
        # arbs, so it must be inside the gate, not discovered live.
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
        # Arb gets its OWN exposure cap, not the momentum one. MAX_EXPOSURE_USD
        # is sized for a single directional leg ($1 under the live profile),
        # while the cheapest legal arb pair is 5 shares a side — about $4.50.
        # Reusing it here rejected every possible live arb, which would have
        # looked like "live mode does nothing" rather than like a bug. The two
        # risks are not comparable: a hedged pair cannot lose the way a naked
        # leg can, so it earns its own limit.
        exp_cap = float(getattr(config, "ARB_MAX_EXPOSURE_USD", 0) or 0)
        if exp_cap > 0 and risk.open_exposure + stake > exp_cap + 1e-9:
            print(f"  [arb] open ${risk.open_exposure:.2f} + ${stake:.2f} "
                  f"exceeds ARB_MAX_EXPOSURE_USD ${exp_cap:.2f}; skip")
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
        # From here an order goes out (or a paper fill is booked), so this is
        # the real "one attempt per slot" boundary.
        arb_done_end_ts = end_ts
        live_res = None
        if not paper:
            import arb_executor
            live_res = await asyncio.to_thread(
                arb_executor.execute,
                live_client, market,
                float(arb["ask_up"]), float(arb["ask_down"]), shares,
                arb.get("up_size"), arb.get("down_size"),
                arb.get("bid_up"), arb.get("bid_down"),
                False,
                # The tuner cannot learn a gate whose value was never recorded
                # beside the outcome it produced. ARB_MIN_REMAINING in
                # particular is unlearnable without remaining_s on the row.
                {"remaining_s": round(float(remaining), 1) if remaining else None,
                 "quote_src": arb.get("src"), "slug": market.get("slug")},
            )
            print(f"  [arb/live] {live_res.reason}")
            # Say WHY, per leg. "not filled" covers two opposite situations —
            # the venue killed a FOK because nothing matched (the edge is not
            # capturable) versus the venue REJECTED the order (a fixable setup
            # problem: allowance, signing, tick size, min size). Without the
            # venue's own words those are indistinguishable, and the first is a
            # verdict on the strategy while the second is a bug in ours.
            for _lg in live_res.legs:
                print(f"    leg {_lg.side} px={_lg.price:.3f} "
                      f"size={_lg.size} filled={_lg.filled}"
                      + (f"  order_id={_lg.order_id}" if _lg.order_id else "")
                      + (f"\n      venue said: {str(_lg.error)[:300]}"
                         if _lg.error else "  (no error — order was killed, "
                                           "not rejected)"))
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
        # Both legs were positively confirmed against the venue by
        # arb_executor. Hand that verdict to the settler rather than making it
        # re-derive a fill from a single order id, which a two-leg pair does
        # not have — that is what booked a locked pair as UNFILLED.
        arb_verified = None
        arb_leg_ids = None
        if not paper and live_res is not None:
            arb_verified = float(live_res.shares or shares)
            arb_leg_ids = [lg.order_id for lg in live_res.legs if lg.order_id]
        settle_coro = _settle_trade(
            trade=trade, stake=stake, shares=shares,
            market=market, question=question, end_ts=end_ts,
            slot_start_time=slot_start_time,
            mkt=pair_cost, edge=edge_after,
            verified_fill=arb_verified, leg_order_ids=arb_leg_ids,
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

        # Subscribe this slot's tokens as soon as it is discovered, so the book
        # is already warm when the arb window opens rather than being fetched
        # cold at decision time.
        if book_feed_client is not None:
            try:
                book_feed_client.watch(market["tokens"]["UP"],
                                       market["tokens"]["DOWN"])
            except Exception:
                pass

        question = market.get("question") or "BTC 5-minute Up/Down"
        end_ts = market.get("end_ts")
        remaining = (end_ts - _now(cl_stream)) if end_ts else None

        # Prefer the venue's own published minimum over the config default, so
        # sizing tracks the market rather than an assumption that could go
        # stale (verified live: orderMinSize = 5 on the BTC 5m markets). This
        # used to sit in the momentum path, downstream of an observe-only
        # `continue` that always fired first — so it never actually ran, and
        # arb only ever saw the config default. Now it runs before every scan,
        # which is the path that reads MIN_ORDER_SHARES.
        venue_min = market.get("min_order_size")
        if venue_min and float(venue_min) > 0:
            config.MIN_ORDER_SHARES = float(venue_min)

        # Count SLOTS toward max_slots, not loop iterations, so a selective but
        # healthy session is not shut down by the safety cap.
        if end_ts is None or end_ts != last_counted_end_ts:
            slots_seen += 1
            last_counted_end_ts = end_ts
        if end_ts is not None and end_ts == arb_done_end_ts:
            # Arb takes at most one attempt per market. Nothing left to do on
            # this slot but wait it out.
            write_status(phase="waiting",
                         message="arb attempted on this slot; waiting for slot end")
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        rem_txt = f"{remaining:.0f}s left" if remaining is not None else "no end time"
        print(f"\nslot: {question}  ({rem_txt})")
        write_status(phase="analyzing", question=question, slug=market.get("slug"),
                     end_ts=end_ts, side=None, edge=None, mkt=None)

        if not arb_on:
            print("  [arb] scanner disabled (ARB_TRADE_ENABLED=0); idling this slot")
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        # ---- ARB SCAN: poll this slot's book until it pays or time runs out ----
        # The re-scan cadence used to live INSIDE momentum's "slot too early"
        # branch, which only ran while more than late_max (40s) remained. Two
        # consequences: the last 40s..ARB_MIN_REMAINING of every slot was never
        # re-scanned at all, and deleting the momentum block would have silently
        # dropped arb to a single scan per slot. Scanning is an in-memory read of
        # the pushed book, so poll it properly — the whole slot down to
        # ARB_MIN_REMAINING now belongs to arb.
        scan_gap = max(0.2, float(getattr(config, "ARB_SCAN_INTERVAL_S", 1.0) or 1.0))
        scans = 0
        last_beat = 0.0
        while True:
            # _heartbeat_sleep only refreshes status.json when the sleep is
            # LONGER than its 5s chunk, so a 1s scan cadence never touched it:
            # the dashboard read STALE through 119 perfectly healthy scans, and
            # its clock offset — derived from status.updated_at — drifted by the
            # whole slot. Beat here instead, at the rate the file actually needs.
            _now_wall = time.time()
            if _now_wall - last_beat >= 5.0:
                last_beat = _now_wall
                write_status(phase="analyzing", question=question,
                             slug=market.get("slug"), end_ts=end_ts,
                             message=f"scanning for an arb ({scans} so far)")
            remaining = (end_ts - _now(cl_stream)) if end_ts else None
            if remaining is not None and remaining < arb_min_rem:
                print(f"  [arb] {scans} scan(s) this slot; {remaining:.0f}s left "
                      f"is under the {arb_min_rem}s floor — waiting it out")
                break
            scans += 1
            try:
                if await _try_arb(market, question, end_ts, remaining):
                    break
            except Exception as e:
                print(f"  [arb] scan error: {e}")
            if end_ts is not None and end_ts == arb_done_end_ts:
                # A signal was seen and declined (edge too wide, book too thin,
                # a risk cap). One attempt per slot, so stop re-reading a book
                # we have already decided not to act on.
                break
            if config.ROUND_SECONDS <= 0 or end_ts is None:
                break  # fast/deterministic test mode: single pass, no sleeping
            await _heartbeat_sleep(scan_gap)

        # No point waiting out a slot we are done with — the loop is about to
        # exit anyway once the round count is reached.
        if placed < rounds:
            await _sleep_until_slot_end(end_ts, cl_stream)

    if settle_tasks:
        print(f"\n[settle] waiting for {len(settle_tasks)} open trade(s) to resolve...")
        await asyncio.gather(*settle_tasks, return_exceptions=True)
    _hb_task.cancel()
    cl_stream.stop()
    if book_feed_client is not None:
        print(f"  [book] feed status: {json.dumps(book_feed_client.status())}")
        book_feed_client.stop()
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
                    help="accepted for compatibility; arb is the only "
                         "strategy left, so the value is ignored")
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
    if args.bankroll is not None:
        config.STARTING_BANKROLL = args.bankroll
    if args.bet is not None:
        config.BASE_TRADE_USD = args.bet
        config.MAX_TRADE_USD = args.bet
        config.FIXED_STAKE_USD = args.bet
        if config.MAX_EXPOSURE_USD < args.bet:
            config.MAX_EXPOSURE_USD = args.bet
        # --bet sizes a single position, so it raises the arb pair cap too.
        if args.mode != "live":
            config.ARB_MAX_STAKE_USD = max(
                getattr(config, "ARB_MAX_STAKE_USD", 0.0), args.bet)
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
        # The offline simulator drove a synthetic directional book, which only
        # ever meant anything for momentum. An arb needs two real, live-quoted
        # sides to be worth simulating at all, so paper_live is the paper mode.
        raise SystemExit(
            "MODE=paper (offline simulation) was momentum-only and is gone. "
            "Use --mode paper_live for simulated fills against the real book.")
    elif args.mode == "paper_live":
        asyncio.run(run_realtime(rounds=args.rounds or 100, paper=True,
                                 resume=args.resume and not args.reset))
    elif args.mode == "live":
        # Default to very few live rounds; override with --rounds if needed.
        asyncio.run(run_realtime(rounds=args.rounds or 5, paper=False,
                                 resume=args.resume and not args.reset))
