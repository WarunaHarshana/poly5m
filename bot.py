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
from pricing_model import DiffusionPricingModel, TWAPPricingModel, classify_session, calculate_regime_multiplier

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
LOG_FILE_PATH = os.path.join(STATE_DIR, "bot.log")

class _TeeOut:
    def __init__(self, target_stream, log_path):
        self.target = target_stream
        self.log_path = log_path
        self._is_tee = True

    def write(self, s):
        self.target.write(s)
        self.target.flush()
        # Avoid duplicate writes if target is already the log file or if not a tty
        target_name = getattr(self.target, "name", None)
        if target_name and os.path.abspath(str(target_name)) == os.path.abspath(self.log_path):
            return
        try:
            with open(self.log_path, "a", encoding="utf-8", errors="ignore") as f:
                f.write(s)
        except Exception:
            pass

    def flush(self):
        self.target.flush()

import sys
if sys.stdout.isatty() and not getattr(sys.stdout, "_is_tee", False):
    sys.stdout = _TeeOut(sys.stdout, LOG_FILE_PATH)

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

    Handles Windows file-lock contention gracefully with retries and fallback.
    """
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, default=str)
            f.flush()
        for _ in range(5):
            try:
                os.replace(tmp, path)
                return
            except OSError:
                time.sleep(0.02)
        # Fallback if replace is momentarily locked on Windows
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, default=str)
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    except Exception:
        pass


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
    if _status.get("end_ts"):
        _status["remaining_s"] = max(0.0, float(_status["end_ts"]) - time.time())
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


def _scan_sum_to_one_arb(pub, market, book_feed_client=None):
    """Scan market for sum-to-one arbitrage (ask_up + ask_down < 1.0 - min_edge)."""
    tokens = market.get("tokens", {})
    up_id = tokens.get("UP")
    dn_id = tokens.get("DOWN")
    if not up_id or not dn_id:
        return None

    pair = None
    if book_feed_client is not None:
        try:
            pair = book_feed_client.pair(up_id, dn_id)
        except Exception:
            pair = None

    if pair is not None:
        ask_up = pair.ask_up
        ask_down = pair.ask_down
        bid_up = pair.bid_up
        bid_down = pair.bid_down
        up_size = pair.size_up
        down_size = pair.size_down
        src = f"ws ({pair.age_s:.2f}s)"
    else:
        q_up = _quote_side(pub, market, "UP")
        q_down = _quote_side(pub, market, "DOWN")
        if not q_up or not q_down:
            return None
        ask_up = float(q_up["price"])
        ask_down = float(q_down["price"])
        bid_up = q_up.get("best_bid")
        bid_down = q_up.get("best_bid")
        up_size = float(q_up.get("ask_size", 0) or 0)
        down_size = float(q_up.get("ask_size", 0) or 0)
        src = "rest"

    cost = ask_up + ask_down
    raw_edge = 1.0 - cost
    min_edge = float(getattr(config, "ARB_MIN_EDGE", 0.02))
    if raw_edge < min_edge:
        return None

    return {
        "src": src,
        "sum": cost,
        "raw_edge": raw_edge,
        "ask_up": ask_up,
        "ask_down": ask_down,
        "bid_up": bid_up,
        "bid_down": bid_down,
        "up_size": up_size,
        "down_size": down_size,
    }


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

    # --- diffusion momentum model parameters (H-001 - H-005) ---
    diffusion_model = DiffusionPricingModel(
        alpha=float(getattr(config, "DIFFUSION_ALPHA", 1.75)),
        min_sigma_usd=float(getattr(config, "DIFFUSION_MIN_SIGMA_USD", 2.5)),
        min_z=float(getattr(config, "DIFFUSION_Z_MIN", 1.8)),
        high_z_breakout=float(getattr(config, "DIFFUSION_Z_BREAKOUT", 2.2)),
        max_chop_marginal=int(getattr(config, "DIFFUSION_MAX_CHOP_MARGINAL", 3)),
    )
    mom_on = bool(getattr(config, "MOMENTUM_TRADE_ENABLED", True))
    mom_observe_only = bool(getattr(config, "MOMENTUM_OBSERVE_ONLY", False))

    print(f"[STRATEGY: Regime-Conditioned Terminal Diffusion Engine (H-001 - H-005)]")
    print(f"  [diffusion] min_z={diffusion_model.min_z:.2f}  breakout_z={diffusion_model.high_z_breakout:.2f}  "
          f"max_chop={diffusion_model.max_chop_marginal}  alpha={diffusion_model.alpha:.2f}  "
          f"window=[{config.TERMINAL_WINDOW_MAX_S}s..{config.TERMINAL_WINDOW_MIN_S}s]  "
          f"status={'OBSERVE-ONLY' if mom_observe_only else ('ON' if mom_on else 'OFF')}")
    print(f"  [modes] min_shares_sizing={'ON' if getattr(config, 'MIN_SHARES_SIZING', False) else 'off'}"
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
    momentum_done_end_ts = None # momentum: at most one attempt per slot
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

    prev_slot_abs_move: float | None = None
    last_counted_end_ts = None
    momentum_done_end_ts = None
    early_seen_end_ts = None
    time_guard_seen_end_ts = None

    async def _try_momentum(market, question, end_ts, remaining, verbose: bool = True):
        """Evaluate and execute regime-conditioned terminal diffusion momentum (H-001 - H-005)."""
        nonlocal momentum_done_end_ts, placed
        nonlocal momentum_done_end_ts
        if end_ts is None:
            return False
        if end_ts and momentum_done_end_ts == end_ts:
            return True

        slot_start_time = (end_ts - config.SLOT_SECONDS) if end_ts else None
        strike = None
        strike_src = "none"
        if slot_start_time:
            try:
                strike = await asyncio.to_thread(feed.get_slot_start_price, slot_start_time)
                if strike is not None:
                    strike_src = "chainlink-data-stream" if cl_stream.ready else "proxy"
            except Exception as e:
                if verbose:
                    print(f"  [diffusion] strike fetch failed: {e}")

        if strike is None or strike <= 0:
            if verbose:
                print("  [diffusion] no verified slot strike available; skipping")
            return False

        spot = None
        try:
            spot = await asyncio.to_thread(feed.price)
        except Exception as e:
            if verbose:
                print(f"  [diffusion] spot fetch failed: {e}")
            return False

        chop_crosses = feed.strike_crossings_since(slot_start_time, strike) if slot_start_time else 0
        var_sec = feed.var_per_sec_since(slot_start_time) if slot_start_time else None
        realized_vol = feed.realized_vol_daily_since(slot_start_time) if slot_start_time else None

        move_usd = spot - strike
        side_candidate = "UP" if move_usd >= 0 else "DOWN"
        tokens = market.get("tokens") or {}
        token_id = tokens.get(side_candidate)

        # Read best ask / bid
        ask = None
        bid = None
        spread = None
        ask_size = None
        if book_feed_client is not None and token_id:
            try:
                top = book_feed_client.get_top(token_id)
                if top:
                    ask = top.get("ask")
                    bid = top.get("bid")
                    spread = top.get("spread")
                    ask_size = top.get("ask_size")
            except Exception:
                pass

        if ask is None and token_id:
            try:
                q = await asyncio.to_thread(pub.buy_quote, token_id)
                if q:
                    ask = q.get("best_ask") or q.get("price")
                    bid = q.get("best_bid")
                    spread = q.get("spread")
                    ask_size = q.get("ask_size")
            except Exception:
                try:
                    book = await asyncio.to_thread(pub.get_order_book, token_id)
                    if book:
                        asks = book.get("asks", [])
                        bids = book.get("bids", [])
                        if asks:
                            ask = float(asks[0]["price"])
                            ask_size = float(asks[0].get("size", 0))
                        if bids:
                            bid = float(bids[0]["price"])
                        if ask is not None and bid is not None:
                            spread = round(ask - bid, 4)
                except Exception:
                    pass

        fee_rate = float(market.get("fee_rate")
                         if market.get("fee_rate") is not None
                         else getattr(config, "TAKER_FEE_RATE", 0.07))

        utc_hour = int(time.strftime("%H", time.gmtime(_now(cl_stream))))

        realized_twap = None
        elapsed_s = None
        if slot_start_time and hasattr(feed, "compute_realized_twap"):
            try:
                realized_twap, elapsed_s = feed.compute_realized_twap(slot_start_time)
            except Exception:
                realized_twap = spot

        signal = diffusion_model.evaluate(
            spot=spot,
            strike=strike,
            remaining_s=remaining or 10.0,
            ask=ask,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            chop_crosses=chop_crosses,
            realized_vol_daily=realized_vol,
            var_per_sec=var_sec,
            strike_src=strike_src,
            require_verified_strike=getattr(config, "MOMENTUM_REQUIRE_VERIFIED_STRIKE", True),
            prev_slot_abs_move=prev_slot_abs_move,
            utc_hour=utc_hour,
            min_entry=config.LATE_MOMENTUM_MIN_ENTRY,
            max_entry=config.LATE_MOMENTUM_MAX_ENTRY,
            min_edge=config.LATE_MOMENTUM_MIN_EDGE,
            max_edge=config.LATE_MOMENTUM_MAX_EDGE,
            fee_rate=fee_rate,
        )

        mom_trade_on = bool(getattr(config, "MOMENTUM_TRADE_ENABLED", True))
        mom_observe = bool(getattr(config, "MOMENTUM_OBSERVE_ONLY", False))

        # Log observation once per slot for the learning archive
        if obs_written.get("end_ts") != end_ts:
            obs_written["end_ts"] = end_ts
            try:
                import observations
                obs_row = {
                    "slug": market.get("slug"),
                    "end_ts": end_ts,
                    "slot_start_ts": slot_start_time,
                    "mode": "paper_live" if paper else "live",
                    "timeframe": getattr(config, "TIMEFRAME", "5m"),
                    "asset": getattr(config, "ASSET", "btc"),
                    "strike": strike,
                    "strike_src": strike_src,
                    "spot": spot,
                    "move_usd": move_usd,
                    "abs_move": abs(move_usd),
                    "remaining_s": remaining,
                    "chop_crosses": chop_crosses,
                    "realized_vol_daily": realized_vol,
                    "side": signal.side,
                    "model_prob": signal.model_prob,
                    "edge": signal.edge,
                    "edge_net": signal.edge_net,
                    "ask": ask,
                    "bid": bid,
                    "spread": spread,
                    "ask_size": ask_size,
                    "session": signal.session_name,
                    "regime_mult": signal.regime_mult,
                    "decision": "trade" if (signal.passed_gates and mom_trade_on and not mom_observe) else "skip",
                    "skip_reason": signal.skip_reason if not signal.passed_gates else ("momentum_observe_only" if mom_observe else None),
                }
                observations.record(obs_row)
            except Exception as e:
                print(f"  [obs] record failed: {e}")

        write_status(
            phase="analyzing",
            question=question,
            slug=market.get("slug"),
            end_ts=end_ts,
            remaining_s=remaining,
            spot_price=round(spot, 2) if spot is not None else None,
            strike_price=round(strike, 2) if strike is not None else None,
            z_score=round(signal.z_score, 2),
            sigma_tau=round(signal.sigma_tau, 2),
            model_prob=round(signal.model_prob, 4),
            edge_net=round(signal.edge_net, 4),
            session_name=signal.session_name,
            chop_crosses=chop_crosses,
            strategy="diffusion_momentum",
            message=f"Z={signal.z_score:.2f} ({signal.side}) prob={signal.model_prob:.2f}",
        )

        if verbose or signal.passed_gates:
            print(f"  [diffusion] Z={signal.z_score:.2f} (|move|=${abs(move_usd):.2f}, "
                  f"sigma_tau=${signal.sigma_tau:.2f}, chop={chop_crosses}) "
                  f"{signal.side} @ ask={ask if ask is not None else 0.0:.3f} "
                  f"(prob={signal.model_prob:.3f}, edge={signal.edge_net:+.3f}, "
                  f"session='{signal.session_name}', regime={signal.regime_mult:.2f}x)")

        if not signal.passed_gates:
            if verbose:
                print(f"  [diffusion] skipped: {signal.skip_reason}")
            skips["diffusion_gates"] = skips.get("diffusion_gates", 0) + 1
            return False

        if mom_observe or not mom_trade_on:
            print("  [diffusion] observe-only (MOMENTUM_OBSERVE_ONLY=1) — signal logged, not traded")
            skips["momentum_observe_only"] = skips.get("momentum_observe_only", 0) + 1
            return False

        if risk.halted or not risk.daily_loss_ok() or not risk.max_trades_ok():
            print(f"  [diffusion] {risk.halt_reason or 'risk limit reached'}; skip")
            return False

        entry_price = float(ask)
        if paper:
            slip = float(getattr(config, "PAPER_FILL_SLIPPAGE", 0.01) or 0.0)
            entry_price = min(0.999, entry_price + slip)

        base_stake = float(config.FIXED_STAKE_USD or config.BASE_TRADE_USD)
        # Apply regime multiplier if enabled
        if getattr(config, "MOMENTUM_USE_REGIME_SIZING", True):
            base_stake = round(base_stake * signal.regime_mult, 2)

        stake = min(base_stake, risk.bankroll - risk.open_exposure)
        if stake < 0.5:
            print("  [diffusion] stake below $0.50 floor; skip")
            return False

        shares = round(stake / entry_price, 2)
        min_sh = float(getattr(config, "MIN_ORDER_SHARES", 5) or 5)
        if shares < min_sh:
            shares = min_sh
            stake = round(shares * entry_price, 2)

        if stake > risk.bankroll - risk.open_exposure:
            print(f"  [diffusion] stake ${stake:.2f} not affordable "
                  f"(bankroll ${risk.bankroll:.2f}, open ${risk.open_exposure:.2f}); skip")
            return False

        fee_usd = round(shares * config.taker_fee_per_share(entry_price, fee_rate), 4)
        trade = Trade(
            ts=time.time(),
            round_id=placed + 1,
            side=signal.side,
            entry_price=round(entry_price, 4),
            shares=shares,
            stake_usd=round(stake, 2),
            model_prob=round(signal.model_prob, 4),
            edge=round(signal.edge_net, 4),
            fee_usd=fee_usd,
            slug=market.get("slug") or "",
            question=question,
            slot_start_ts=slot_start_time,
            end_ts=end_ts,
        )
        stamp_trade_times(trade)
        momentum_done_end_ts = end_ts

        # If live, route to live_client
        if not paper:
            try:
                cond_id = market.get("conditionId") or market.get("condition_id") or ""
                order_res = await asyncio.to_thread(
                    live_client.buy,
                    token_id,
                    entry_price,
                    shares,
                    cond_id,
                )
                order_id = ""
                if isinstance(order_res, dict):
                    order_id = (order_res.get("orderID") or order_res.get("order_id") or
                                (order_res.get("order") or {}).get("id") or "")
                success = bool(order_res.get("success") or order_id) if isinstance(order_res, dict) else False
                if not success:
                    print(f"  [diffusion/live] order placement failed: {order_res}")
                    return False
            except Exception as e:
                print(f"  [diffusion/live] order error: {e}")
                return False

        risk.on_open(stake)
        _register_open(trade, end_ts)
        placed += 1
        print(f"  TRADE PLACED [diffusion] R{trade.round_id} {trade.side} "
              f"{shares:.2f} shares @ {entry_price:.3f} ${stake:.2f} "
              f"(prob {signal.model_prob:.3f}, edge {signal.edge_net:+.3f})  "
              f"{trade.time_et or trade.time_utc}")

        try:
            _journal(
                _ensure_session_id(),
                "paper_live" if paper else "live",
                "diffusion_momentum",
                phase="momentum", action="trade", reason="placed",
                slug=trade.slug, question=question, side=trade.side,
                ask=entry_price, fill_price=entry_price, edge=signal.edge_net,
                stake=stake, remaining_s=remaining, shadow=False,
            )
        except Exception:
            pass

        write_status(
            phase="holding", trade_event="placed",
            trade_event_id=f"placed-{trade.round_id}-{int(trade.ts)}",
            end_ts=end_ts, side=trade.side, mkt=round(entry_price, 4),
            edge=round(signal.edge_net, 4), stake=stake, question=question,
            slug=trade.slug, event_url=trade.event_url,
            time_et=trade.time_et, time_utc=trade.time_utc,
            strategy="diffusion_momentum",
            message=f"{trade.side} @ {entry_price:.3f} (Z={signal.z_score:.2f}, prob={signal.model_prob:.2f})",
        )

        settle_coro = _settle_trade(
            trade=trade, stake=stake, shares=shares,
            market=market, question=question, end_ts=end_ts,
            slot_start_time=slot_start_time,
            mkt=entry_price, edge=signal.edge_net,
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
        if end_ts is not None and end_ts == momentum_done_end_ts:
            # A trade was attempted on this slot. Nothing left to do on
            # this slot but wait it out.
            write_status(phase="waiting",
                         message="slot trade evaluated; waiting for slot end")
            await _sleep_until_slot_end(end_ts, cl_stream)
            continue

        rem_txt = f"{remaining:.0f}s left" if remaining is not None else "no end time"
        utc_hour = int(time.strftime("%H", time.gmtime(_now(cl_stream))))
        session_tag, _ = classify_session(utc_hour)
        reg_mult = calculate_regime_multiplier(prev_slot_abs_move)

        print(f"\nslot: {question} ({rem_txt}) [{session_tag} | PrevMove=${prev_slot_abs_move or 0.0:.1f} ({reg_mult:.2f}x)]")
        spot_init = None
        try:
            spot_init = feed.price()
        except Exception:
            pass
        write_status(
            phase="analyzing",
            question=question,
            slug=market.get("slug"),
            end_ts=end_ts,
            remaining_s=remaining,
            spot_price=round(spot_init, 2) if spot_init is not None else None,
            session_name=session_tag,
            side=None,
            edge=None,
            mkt=None,
            strategy="diffusion_momentum",
            message=f"monitoring {session_tag} ({rem_txt})",
        )

        # ---- Dedicated Terminal Diffusion Execution ----
        term_max_s = int(getattr(config, "TERMINAL_WINDOW_MAX_S", 60) or 60)
        term_min_s = int(getattr(config, "TERMINAL_WINDOW_MIN_S", 5) or 5)

        # Wait until terminal window opens
        while remaining is not None and remaining > term_max_s:
            wait_step = min(5.0, remaining - term_max_s)
            if wait_step > 0 and config.ROUND_SECONDS > 0:
                await _heartbeat_sleep(wait_step)
            remaining = (end_ts - _now(cl_stream)) if end_ts else None
            if config.ROUND_SECONDS <= 0:
                break

        # In the execution window: continuously scan until trade is placed or window expires
        scan_gap = max(0.5, float(getattr(config, "DIFFUSION_SCAN_INTERVAL_S", 1.0) or 1.0))
        scan_count = 0
        while True:
            remaining = (end_ts - _now(cl_stream)) if end_ts else None
            if remaining is None or remaining < term_min_s:
                break
            scan_count += 1
            # Verbose on first scan of slot or at final seconds
            is_verbose = (scan_count == 1) or (remaining <= term_min_s + 1)
            try:
                placed_ok = await _try_momentum(market, question, end_ts, remaining, verbose=is_verbose)
                if placed_ok:
                    break
            except Exception as e:
                print(f"  [diffusion] evaluation error: {e}")
            if config.ROUND_SECONDS <= 0:
                break
            await _heartbeat_sleep(scan_gap)

        # Sleep out remainder of slot
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
