"""24/7 entrypoint: dashboard + supervised bot + the nightly learning cycle.

Railway runs one process per service, so this is that process. It owns three
concerns and nothing else — all the trading logic stays in bot.py:

  web       serves the existing dashboard (app.py's handler) on $PORT
  trading   keeps `bot.py --mode paper_live --resume` alive, forever
  learning  labels settled slots, then runs the tuner on a nightly cycle

Design notes that matter operationally:

* **Restarts are normal, not failures.** The bot is recycled on a fixed
  interval (BOT_LIFETIME_HOURS) as well as on crashes. Long-lived websocket
  feeds go stale in ways that are hard to detect from inside; a scheduled
  restart is cheaper than diagnosing that. `--resume` means the paper account
  carries across every restart, so recycling costs nothing but a few seconds.
* **Crash backoff is capped.** A tight restart loop against a down API just
  burns quota, so failures back off exponentially to BACKOFF_MAX_S. A run that
  survived a while resets the backoff.
* **Learning is decoupled from trading.** Labeling and tuning happen in this
  process on their own timers. The bot never blocks on them, and a tuner
  exception can't take trading down.
* **Learned parameters apply on the next bot restart**, not mid-run, because
  config is read at import. The nightly cycle therefore recycles the bot right
  after a successful tune so the change actually takes effect.

Environment: MODE is forced to paper_live unless SUPERVISOR_ALLOW_LIVE=1.
Real money should never be the thing that starts by default on a redeploy.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

import paths

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = paths.state_dir()
SUPERVISOR_STATUS = os.path.join(STATE_DIR, "supervisor.json")
BOT_LOG = os.path.join(STATE_DIR, "bot.log")

# --- knobs -----------------------------------------------------------------
MODE = (os.getenv("MODE") or "paper_live").strip()
ALLOW_LIVE = (os.getenv("SUPERVISOR_ALLOW_LIVE") or "").strip() in ("1", "true", "yes")
BOT_LIFETIME_H = float(os.getenv("BOT_LIFETIME_HOURS", "6"))
BACKOFF_START_S = float(os.getenv("BACKOFF_START_S", "10"))
BACKOFF_MAX_S = float(os.getenv("BACKOFF_MAX_S", "300"))
HEALTHY_RUN_S = float(os.getenv("HEALTHY_RUN_S", "300"))
LABEL_INTERVAL_S = float(os.getenv("LABEL_INTERVAL_S", "900"))      # 15 min
TUNE_INTERVAL_S = float(os.getenv("TUNE_INTERVAL_S", str(24 * 3600)))
TUNE_FIRST_DELAY_S = float(os.getenv("TUNE_FIRST_DELAY_S", "3600"))
BANKROLL = os.getenv("PAPER_BANKROLL", "").strip()
BET = os.getenv("PAPER_BET", "").strip()
LOG_MAX_BYTES = int(os.getenv("BOT_LOG_MAX_BYTES", str(8 * 1024 * 1024)))

_stop = threading.Event()
_state_lock = threading.Lock()
_state = {
    "started_at": time.time(),
    "mode": MODE,
    "bot_pid": None,
    "bot_restarts": 0,
    "bot_started_at": None,
    "last_exit_code": None,
    "last_label": None,
    "last_tune": None,
    "recycle_requested": False,
}


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[supervisor {stamp}] {msg}", flush=True)


def _update(**kw) -> None:
    with _state_lock:
        _state.update(kw)
        _state["updated_at"] = time.time()
        snapshot = dict(_state)
    try:
        tmp = SUPERVISOR_STATUS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, default=str)
        os.replace(tmp, SUPERVISOR_STATUS)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Trading process
# ---------------------------------------------------------------------------
def _bot_command() -> list[str]:
    mode = MODE
    if mode == "live" and not ALLOW_LIVE:
        log("MODE=live requested without SUPERVISOR_ALLOW_LIVE=1 - "
            "falling back to paper_live. Real money never starts by default.")
        mode = "paper_live"
    cmd = [sys.executable, "-u", os.path.join(HERE, "bot.py"),
           "--mode", mode,
           # The loop exits at `placed >= rounds`; this is effectively "never".
           # Recycling is handled on a wall clock instead, which is predictable.
           "--rounds", "1000000",
           "--resume"]
    if BANKROLL:
        cmd += ["--bankroll", BANKROLL]
    if BET:
        cmd += ["--bet", BET]
    return cmd


def _rotate_log() -> None:
    """Keep bot.log bounded - a volume filling up takes the service down."""
    try:
        if os.path.isfile(BOT_LOG) and os.path.getsize(BOT_LOG) > LOG_MAX_BYTES:
            prev = BOT_LOG + ".1"
            if os.path.exists(prev):
                os.remove(prev)
            os.replace(BOT_LOG, prev)
    except OSError:
        pass


def trading_loop() -> None:
    backoff = BACKOFF_START_S
    while not _stop.is_set():
        _rotate_log()
        cmd = _bot_command()
        log(f"starting bot: {' '.join(cmd[1:])}")
        started = time.time()
        try:
            with open(BOT_LOG, "a", encoding="utf-8", errors="replace") as logf:
                logf.write(f"\n===== bot start {datetime.now(timezone.utc)} =====\n")
                logf.flush()
                proc = subprocess.Popen(
                    cmd, cwd=HERE, stdout=logf, stderr=subprocess.STDOUT)
        except OSError as e:
            log(f"could not launch bot: {e}; retrying in {backoff:.0f}s")
            _stop.wait(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)
            continue

        with _state_lock:
            _state["bot_restarts"] += 1
        _update(bot_pid=proc.pid, bot_started_at=started, recycle_requested=False)

        deadline = started + BOT_LIFETIME_H * 3600 if BOT_LIFETIME_H > 0 else None
        while True:
            try:
                code = proc.wait(timeout=5)
                break
            except subprocess.TimeoutExpired:
                pass
            if _stop.is_set():
                _terminate(proc, "shutdown")
                return
            recycle = _state.get("recycle_requested")
            if recycle or (deadline and time.time() >= deadline):
                why = "learned new parameters" if recycle else \
                      f"scheduled recycle after {BOT_LIFETIME_H:g}h"
                log(f"recycling bot ({why})")
                _terminate(proc, why)
                code = None
                break

        ran_for = time.time() - started
        _update(bot_pid=None, last_exit_code=code, recycle_requested=False)
        if _stop.is_set():
            return
        if code in (0, None):
            # Clean exit or an intentional recycle: no need to back off.
            backoff = BACKOFF_START_S
            log(f"bot stopped after {ran_for / 60:.1f} min (exit={code}); "
                f"restarting")
            _stop.wait(2)
            continue

        log(f"bot exited with code {code} after {ran_for / 60:.1f} min")
        if ran_for >= HEALTHY_RUN_S:
            backoff = BACKOFF_START_S  # it was healthy; treat as a one-off
        log(f"restarting in {backoff:.0f}s (see {BOT_LOG})")
        _stop.wait(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX_S)


def _terminate(proc, why: str) -> None:
    if proc.poll() is not None:
        return
    log(f"stopping bot pid={proc.pid} ({why})")
    try:
        proc.terminate()
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        log("bot did not stop in 20s; killing")
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:
            pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Learning cycle
# ---------------------------------------------------------------------------
def learning_loop() -> None:
    """Label settled slots continuously; tune on the slower cycle."""
    next_label = time.time() + 120        # let the bot record something first
    next_tune = time.time() + TUNE_FIRST_DELAY_S
    while not _stop.is_set():
        now = time.time()

        if now >= next_label:
            next_label = now + LABEL_INTERVAL_S
            try:
                import observations
                summary = observations.backfill_outcomes()
                if summary.get("labeled"):
                    log(f"labeled {summary['labeled']} settled slot(s) "
                        f"({summary.get('pending', 0)} still pending, "
                        f"{summary.get('total', 0)} total)")
                _update(last_label={"at": now, **summary})
            except Exception as e:
                log(f"labeling failed: {e}")

        if now >= next_tune:
            next_tune = now + TUNE_INTERVAL_S
            try:
                import tuner
                # Label everything available first — tuning on a dataset whose
                # newest rows are unlabeled means the holdout slice, which is
                # exactly the newest rows, would be empty.
                import observations
                observations.backfill_outcomes(max_lookups=2000)
                result = tuner.run()
                _update(last_tune={"at": now, "action": result.get("action"),
                                   "changes": result.get("changes"),
                                   "reasons": result.get("reasons")})
                if result.get("action") in ("apply", "revert"):
                    # config is read at import, so the running bot still has the
                    # old values. Recycle it to pick them up.
                    log("parameters changed - recycling the bot to apply them")
                    _update(recycle_requested=True)
            except Exception as e:
                log(f"tuning cycle failed: {e}")

        _stop.wait(min(30.0, max(5.0, next_label - time.time())))


# ---------------------------------------------------------------------------
# Web
# ---------------------------------------------------------------------------
def serve_dashboard() -> None:
    import http.server
    os.environ.setdefault("NO_BROWSER", "1")
    import app  # imports are side-effect light; the server is started here

    httpd = http.server.ThreadingHTTPServer((app.HOST, app.PORT), app.Handler)
    log(f"dashboard listening on {app.HOST}:{app.PORT}"
        + ("  (token required)" if app.DASH_TOKEN else ""))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def check_persistence() -> dict:
    """Is the state directory actually on a persistent volume?

    Getting this wrong is silent and expensive: the container happily writes to
    /data whether or not a volume is mounted there, so a deploy without one
    looks completely healthy right up until the next redeploy erases the trade
    archive and everything the tuner has learned.

    Two signals. Railway sets RAILWAY_VOLUME_MOUNT_PATH when a volume is
    attached, which is definitive when present. Otherwise compare device ids:
    a real mount is a different filesystem from /, a plain directory is not.
    """
    root = paths.state_root()
    mount = (os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
    result = {"state_root": root, "volume_mount": mount or None}

    if mount:
        same = os.path.abspath(root).startswith(os.path.abspath(mount))
        result["persistent"] = bool(same)
        result["reason"] = ("state root is inside the mounted volume" if same
                            else f"a volume is mounted at {mount} but state "
                                 f"is being written to {root}")
        return result

    # Only claim "not persistent" when we know we are deployed. Running from a
    # checkout the state directory is an ordinary folder that persists fine,
    # and the st_dev test would flag it every time — on Windows especially,
    # where os.stat("/") resolves to C:\ and matches everything on that drive.
    in_container = (os.name == "posix"
                    and (os.path.exists("/.dockerenv")
                         or any(k.startswith("RAILWAY_") for k in os.environ)))
    if not in_container:
        result["persistent"] = None
        result["reason"] = "not a container deployment; local state persists"
        return result

    try:
        # A real mount is a different filesystem from the container's root.
        result["persistent"] = os.stat(root).st_dev != os.stat("/").st_dev
        result["reason"] = ("state root is on its own filesystem"
                            if result["persistent"] else
                            "state root is on the container filesystem")
    except OSError as e:
        result["persistent"] = None
        result["reason"] = f"could not determine ({e})"
    return result


def main() -> int:
    log(f"state root: {paths.state_root()}")
    persistence = check_persistence()
    if persistence["persistent"] is False:
        log("=" * 68)
        log("WARNING: state is NOT on a persistent volume "
            f"({persistence['reason']}).")
        log("Everything written here - the trade archive, the observation log")
        log("and every parameter the tuner learns - is DESTROYED on the next")
        log("deploy or restart. The bot will appear to work and will never")
        log("improve, because it starts from zero every time.")
        log("Fix: attach a volume to this service with mount path /data.")
        log("=" * 68)
    elif persistence["persistent"]:
        log(f"state is on a persistent volume ({persistence['reason']}) - "
            f"history and learned parameters survive redeploys")
    _update(persistence=persistence)
    log(f"mode={MODE}  bot recycle every {BOT_LIFETIME_H:g}h  "
        f"tune every {TUNE_INTERVAL_S / 3600:.1f}h")

    def _shutdown(signum, _frame):
        log(f"signal {signum} - shutting down")
        _stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown)
        except (ValueError, OSError):
            pass

    serve_dashboard()
    threading.Thread(target=learning_loop, daemon=True, name="learning").start()

    trader = threading.Thread(target=trading_loop, name="trading")
    trader.start()
    try:
        while trader.is_alive():
            trader.join(timeout=1.0)
    except KeyboardInterrupt:
        _stop.set()
        trader.join(timeout=30)
    log("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
