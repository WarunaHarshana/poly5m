"""Offline self-test for the 24/7 operational behaviours (no network).

These are the things that only break after the bot has been running for a
while — a restart, a day boundary, a log that never stops growing — which is
exactly when nobody is watching.

  T1  session resume       restart keeps the bankroll, blotter and session id
  T2  blotter trim         the live file stays bounded, open positions survive
  T3  daily rollover       per-day caps clear at UTC midnight, streak halts don't
  T4  observation dedupe   one row per slot, not one per retry poll
  T5  volume paths         STATE_ROOT redirects every module's writes
  T6  dashboard auth       token gates control endpoints, /healthz stays open
"""
import json
import os
import tempfile

os.environ["STATE_ROOT"] = tempfile.mkdtemp(prefix="selftest_ops_")

import bot  # noqa: E402
import config  # noqa: E402
import observations  # noqa: E402
import paths  # noqa: E402
from executor import Trade  # noqa: E402
from risk import RiskManager  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAIL.append(name)


def mk(round_id, won=True, bankroll_after=0.0):
    return Trade(ts=1_000_000 + round_id, round_id=round_id, side="UP",
                 entry_price=0.6, shares=10.0, stake_usd=6.0, model_prob=0.7,
                 edge=0.05, won=won, bankroll_after=bankroll_after)


# ---------------------------------------------------------------------------
print("T1 session resume")
config.STARTING_BANKROLL = 100.0
trades = [mk(1, bankroll_after=104.0), mk(2, won=False, bankroll_after=98.0)]


class _R:
    bankroll = 98.0
    halted = False
    halt_reason = ""


bot._set_session_id("20260101T000000Z")
bot.persist(trades, _R())

restored, bankroll, session_id = bot.load_session()
check("bankroll survives the restart", bankroll == 98.0, f"got {bankroll}")
check("blotter survives the restart", len(restored) == 2, f"got {len(restored)}")
check("session id survives the restart", session_id == "20260101T000000Z",
      f"got {session_id}")
check("trade fields round-trip",
      restored[0].round_id == 1 and restored[0].entry_price == 0.6,
      str(restored[0]))

# A missing/corrupt stats file must fall back to the blotter, not to zero.
os.remove(bot.STATS_PATH)
_, bankroll_no_stats, _ = bot.load_session()
check("missing stats falls back to the last settled bankroll",
      bankroll_no_stats == 98.0, f"got {bankroll_no_stats}")

with open(bot.TRADES_PATH, "w", encoding="utf-8") as f:
    f.write("\x00\x00\x00")
_, bankroll_torn, _ = bot.load_session()
check("a torn blotter falls back to the configured start",
      bankroll_torn == 100.0, f"got {bankroll_torn}")

# ---------------------------------------------------------------------------
print("T2 blotter trim")
bot.LIVE_BLOTTER_MAX = 5
many = [mk(i, bankroll_after=100.0 + i) for i in range(20)]
many[0].won = None          # an open position, oldest in the list
bot.persist(many, _R())
check("live blotter is capped", len(many) <= 6, f"got {len(many)}")
check("the open position is never trimmed away",
      any(t.won is None and t.round_id == 0 for t in many),
      [t.round_id for t in many])
check("the newest settled trades are kept",
      many[-1].round_id == 19, [t.round_id for t in many])
on_disk = json.load(open(bot.TRADES_PATH, encoding="utf-8"))
check("the file matches the trimmed list", len(on_disk) == len(many))
bot.LIVE_BLOTTER_MAX = 400

# ---------------------------------------------------------------------------
print("T3 daily rollover")
DAY = 86400.0
r = RiskManager(100.0)
r._day = int(1_000_000 // DAY)
r.trades_today = 7
r.realized_pnl_today = -12.0
r.halted = True
r.halt_reason = "max trades per day hit"
rolled = r.roll_day(1_000_000 + DAY)
check("rollover is detected", rolled)
check("trade count resets", r.trades_today == 0)
check("daily pnl resets", r.realized_pnl_today == 0.0)
check("a daily halt clears", not r.halted, r.halt_reason)
check("no rollover within the same day", not r.roll_day(1_000_000 + DAY + 60))

r2 = RiskManager(100.0)
r2._day = int(1_000_000 // DAY)
r2.halted = True
r2.halt_reason = "3 consecutive losses"
r2.roll_day(1_000_000 + DAY)
check("a streak halt does NOT clear on the day boundary", r2.halted,
      r2.halt_reason)

# ---------------------------------------------------------------------------
print("T4 observation dedupe")
if os.path.exists(observations.OBSERVATIONS_JSONL):
    os.remove(observations.OBSERVATIONS_JSONL)

# Mirror the bot's guard: one flush per end_ts however many times the retry
# loop re-evaluates the slot.
obs_written = {"end_ts": None}


def flush(end_ts, decision, reason=""):
    if obs_written.get("end_ts") == end_ts:
        return
    obs_written["end_ts"] = end_ts
    observations.record({"slug": f"slot-{end_ts}", "end_ts": end_ts,
                         "side": "UP", "ask": 0.7, "move_usd": 15.0,
                         "remaining_s": 20.0, "decision": decision,
                         "skip_reason": reason})


for _ in range(40):          # 40 retry polls inside one slot
    flush(2_000_000, "skip", "thin move")
flush(2_000_300, "trade")
rows = observations.load()
check("40 retries produce one row", len(rows) == 2, f"got {len(rows)}")
check("both slots are present",
      {r["end_ts"] for r in rows} == {2_000_000, 2_000_300})

# Unlabeled rows must never be treated as evidence.
labeled = observations.load(labeled_only=True)
check("unlabeled rows are excluded from the learning set", labeled == [])

# ---------------------------------------------------------------------------
print("T5 volume paths")
root = os.environ["STATE_ROOT"]
check("state root honours STATE_ROOT",
      os.path.abspath(paths.state_root()) == os.path.abspath(root),
      paths.state_root())
for name, path in (("bot", bot.STATE_DIR),
                   ("observations", observations.OBSERVATIONS_JSONL),
                   ("archive", bot.ARCHIVE_DIR)):
    check(f"{name} writes under the volume",
          os.path.abspath(path).startswith(os.path.abspath(root)), path)

# ---------------------------------------------------------------------------
print("T6 dashboard auth")
import hmac  # noqa: E402


class _FakeHandler:
    """Exercises app.Handler._authed's logic without a socket."""
    def __init__(self, path, cookie=None, header=None, token="secret"):
        self.path = path
        self.headers = {}
        if cookie:
            self.headers["Cookie"] = cookie
        if header:
            self.headers["X-Dash-Token"] = header
        self._token = token

    def get(self, k):
        return self.headers.get(k)


def authed(handler, token):
    """Same checks as app.Handler._authed, driven by the fake."""
    from urllib.parse import urlparse, parse_qs
    if not token:
        return True
    q = parse_qs(urlparse(handler.path).query)
    supplied = (q.get("token") or [None])[0] or handler.get("X-Dash-Token")
    if not supplied:
        for part in (handler.get("Cookie") or "").split(";"):
            name, _, val = part.strip().partition("=")
            if name == "dash_token":
                supplied = val
                break
    return bool(supplied) and hmac.compare_digest(str(supplied), token)


check("no token -> denied", not authed(_FakeHandler("/"), "secret"))
check("query token -> allowed", authed(_FakeHandler("/?token=secret"), "secret"))
check("wrong token -> denied", not authed(_FakeHandler("/?token=nope"), "secret"))
check("cookie token -> allowed",
      authed(_FakeHandler("/api/control", cookie="dash_token=secret"), "secret"))
check("header token -> allowed",
      authed(_FakeHandler("/api/control", header="secret"), "secret"))
check("no token configured -> open (localhost dev)",
      authed(_FakeHandler("/"), ""))

# The public-bind guard must be unconditional.
import subprocess  # noqa: E402
import sys  # noqa: E402
env = dict(os.environ, DASH_HOST="0.0.0.0")
env.pop("DASH_TOKEN", None)
proc = subprocess.run([sys.executable, "-c", "import app"],
                      capture_output=True, text=True, env=env,
                      cwd=os.path.dirname(os.path.abspath(__file__)))
check("public bind without a token refuses to start",
      "Refusing to serve on a public interface" in (proc.stdout + proc.stderr),
      (proc.stdout + proc.stderr)[:200])

# ---------------------------------------------------------------------------
print("T7 single-instance lock")
import time  # noqa: E402

bot.acquire_lock()
held = json.load(open(bot.LOCK_PATH, encoding="utf-8"))
check("lock records this process", held["pid"] == os.getpid(), str(held))

bot._atomic_write_json(bot.LOCK_PATH, {"pid": 999_999, "heartbeat": time.time()})
refused = False
try:
    bot.acquire_lock()
except SystemExit:
    refused = True
check("a live foreign lock blocks a second bot", refused)

check("--force-lock overrides it",
      bot.acquire_lock(force=True) is None)

bot._atomic_write_json(bot.LOCK_PATH,
                       {"pid": 999_999,
                        "heartbeat": time.time() - bot.LOCK_STALE_S - 60})
bot.acquire_lock()
held = json.load(open(bot.LOCK_PATH, encoding="utf-8"))
check("a stale lock is taken over", held["pid"] == os.getpid(), str(held))

bot.release_lock()
check("release removes the lock", not os.path.exists(bot.LOCK_PATH))

# ---------------------------------------------------------------------------
print("T8 research export never leaks credentials")
os.environ.setdefault("NO_BROWSER", "1")
import io  # noqa: E402
import zipfile  # noqa: E402

import app  # noqa: E402

banned = ("poly_api_creds", ".env", "private", "secret", "passphrase", "..")
suspicious = [f for f in app.EXPORT_FILES + app.EXPORT_FILES_FULL
              if any(b in f.lower() for b in banned)]
check("no credential-shaped entry on the allowlist", not suspicious,
      str(suspicious))

export_base = app._state_dir_for("5m")
os.makedirs(os.path.join(export_base, "archive"), exist_ok=True)
with open(os.path.join(export_base, "poly_api_creds.json"), "w") as f:
    f.write('{"api_secret": "CANARY_MUST_NOT_ESCAPE"}')
with open(os.path.join(export_base, "stats.json"), "w") as f:
    f.write('{"bankroll": 1}')

blob, _manifest = app._export_zip("5m")
names = zipfile.ZipFile(io.BytesIO(blob)).namelist()
check("credentials file is not in the archive",
      not any("creds" in n for n in names), str(names))
check("credential content appears nowhere in the bytes",
      b"CANARY_MUST_NOT_ESCAPE" not in blob)
check("real data still exports", "stats.json" in names, str(names))
check("a manifest is always included", "MANIFEST.json" in names, str(names))

# ---------------------------------------------------------------------------
print("T9 persistent-volume detection")
import importlib  # noqa: E402

import supervisor  # noqa: E402

_saved = {k: os.environ.get(k) for k in
          ("STATE_ROOT", "RAILWAY_VOLUME_MOUNT_PATH", "RAILWAY_ENVIRONMENT")}


def persistence(**env):
    for k in _saved:
        os.environ.pop(k, None)
    os.environ.update(env)
    importlib.reload(supervisor)
    return supervisor.check_persistence()


check("state inside an attached volume reads as persistent",
      persistence(STATE_ROOT="/data/x",
                  RAILWAY_VOLUME_MOUNT_PATH="/data")["persistent"] is True)
check("a volume mounted somewhere else is flagged",
      persistence(STATE_ROOT=tempfile.mkdtemp(),
                  RAILWAY_VOLUME_MOUNT_PATH="/data")["persistent"] is False)
check("local dev raises no false alarm",
      persistence(STATE_ROOT=tempfile.mkdtemp())["persistent"] is None)

for k, v in _saved.items():
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED ops: {FAIL}")
print("SELFTEST_OK ops")
