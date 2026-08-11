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

# ---------------------------------------------------------------------------
print("T10 recycle releases the state lock")
# The bot is recycled with SIGTERM every few hours. If it leaves its lock
# behind with a fresh heartbeat, the replacement refuses to start until the
# heartbeat ages out - which cost 31 restarts in 23h on the first deployment
# where 4 were expected.
bot.acquire_lock()
check("a lock exists while running", os.path.exists(bot.LOCK_PATH))

held_pid = json.load(open(bot.LOCK_PATH, encoding="utf-8"))["pid"]
supervisor.STATE_DIR = os.path.dirname(bot.LOCK_PATH)
supervisor._clear_stale_lock(held_pid)
check("the supervisor clears the lock of a child it reaped",
      not os.path.exists(bot.LOCK_PATH))

# It must never touch a lock belonging to something else.
bot.acquire_lock()
supervisor._clear_stale_lock(999_999)
check("a lock owned by another pid is left alone",
      os.path.exists(bot.LOCK_PATH))
bot.release_lock()

# ---------------------------------------------------------------------------
print("T11 tuner retry cadence")
check("a 'wait' retry is far shorter than a full cycle",
      supervisor.TUNE_RETRY_S < supervisor.TUNE_INTERVAL_S / 4,
      f"retry={supervisor.TUNE_RETRY_S} interval={supervisor.TUNE_INTERVAL_S}")
check("retry is at most an hour", supervisor.TUNE_RETRY_S <= 3600,
      str(supervisor.TUNE_RETRY_S))

# ---------------------------------------------------------------------------
print("T12 minimum-share sizing")
config.MODE = "paper_live"
config.MIN_ORDER_SHARES = 5.0
config.ENFORCE_MIN_SHARES = True
config.MIN_SHARES_SIZING = True
config.FIXED_STAKE_USD = 5.0
config.BASE_TRADE_USD = 5.0
config.MAX_TRADE_USD = 5.0
config.LATE_SCALP_MAX_STAKE_USD = 5.0
config.LATE_SCALP_USE_EDGE_SIZING = False
config.MIN_ENTRY_PRICE, config.MAX_ENTRY_PRICE = 0.0, 1.0
config.MAX_EXPOSURE_USD = 10.0

rm = RiskManager(100.0)
for px, want in ((0.59, 2.95), (0.70, 3.50), (0.87, 4.35)):
    got = rm.size_late_scalp(px, edge=0.0)
    check(f"ask {px} -> 5 shares costs ${want}", abs(got - want) < 0.01,
          f"got ${got}")
    shares = got / px if px else 0
    check(f"ask {px} -> exactly the venue minimum in shares",
          abs(shares - 5.0) < 0.02, f"got {shares:.3f} shares")

# The bump-UP behaviour must survive: a tiny stake still reaches the minimum.
config.FIXED_STAKE_USD = 0.5
config.BASE_TRADE_USD = 0.5
config.MAX_TRADE_USD = 0.5
rm2 = RiskManager(100.0)
got = rm2.size_late_scalp(0.87, edge=0.0)
check("a sub-minimum stake is still bumped UP to the venue floor",
      abs(got - 4.35) < 0.01, f"got ${got}")

# With the flag off, the old fixed-dollar behaviour is unchanged.
config.MIN_SHARES_SIZING = False
config.FIXED_STAKE_USD = 5.0
config.BASE_TRADE_USD = 5.0
config.MAX_TRADE_USD = 5.0
rm3 = RiskManager(100.0)
got = rm3.size_late_scalp(0.70, edge=0.0)
check("flag off -> fixed $5 stake as before", abs(got - 5.0) < 0.01,
      f"got ${got}")
config.MIN_SHARES_SIZING = True

# It must never spend money it does not have.
rm4 = RiskManager(2.00)
got = rm4.size_late_scalp(0.87, edge=0.0)
check("refuses when the bankroll cannot cover the minimum order",
      got == 0.0, f"got ${got} on a $2 bankroll needing $4.35")

# ---------------------------------------------------------------------------
print("T13 reconciliation against Polymarket")
import reconcile  # noqa: E402

# Rebuild a clean state dir for the reconciler.
for _p in (reconcile.HISTORY_JSONL, reconcile.TRADES_PATH,
           reconcile.STATS_PATH, reconcile.CORRECTIONS_LOG):
    if os.path.exists(_p):
        os.remove(_p)

OLD = time.time() - 3600  # comfortably past MIN_AGE_S


def rec_trade(rid, side, outcome, won, pnl, src, shares=5.0, stake=3.05):
    return {"round_id": rid, "slug": f"slot-{rid}", "side": side,
            "outcome": outcome, "won": won, "pnl": pnl, "shares": shares,
            "stake_usd": stake, "fee_usd": 0.0, "entry_price": stake / shares,
            "end_ts": OLD, "resolve_source": src}


seed = [
    # A fabricated win: booked UP/won from a local guess, really DOWN.
    rec_trade(55, "UP", "UP", True, 1.95, "chainlink-stream"),
    # The mirror image: booked a loss that was really a win.
    rec_trade(56, "DOWN", "UP", False, -2.80, "local-estimate"),
    # An ARB whose label is wrong but whose PnL is right either way - this is
    # the shape that hid the bug, since the money looked correct.
    rec_trade(41, "ARB", "UP", True, 1.01, "chainlink-stream",
              shares=5.0, stake=3.99),
    # Already confirmed: must not be touched.
    rec_trade(30, "UP", "UP", True, 1.50, "polymarket-official"),
]
with open(reconcile.HISTORY_JSONL, "w", encoding="utf-8") as f:
    for t in seed:
        f.write(json.dumps(t) + "\n")
with open(reconcile.TRADES_PATH, "w", encoding="utf-8") as f:
    json.dump(seed, f)
with open(reconcile.STATS_PATH, "w", encoding="utf-8") as f:
    json.dump({"bankroll": 100.0}, f)


class _FakePub:
    """Polymarket says DOWN for every slot in this fixture."""
    def __init__(self):
        self.lookups = 0

    def get_official_btc_updown_outcome(self, slug):
        self.lookups += 1
        return "DOWN"


fake = _FakePub()
res = reconcile.reconcile(pub=fake, verbose=False)
fixed = {t["round_id"]: t for t in reconcile._read_jsonl(reconcile.HISTORY_JSONL)}

check("the fabricated win becomes a loss",
      fixed[55]["won"] is False and fixed[55]["outcome"] == "DOWN",
      str(fixed[55]))
check("its pnl becomes the lost stake",
      abs(fixed[55]["pnl"] + 3.05) < 0.01, str(fixed[55]["pnl"]))
check("the mistaken loss becomes a win",
      fixed[56]["won"] is True and fixed[56]["pnl"] > 0, str(fixed[56]))
check("an ARB label is corrected but its pnl is not",
      fixed[41]["outcome"] == "DOWN" and abs(fixed[41]["pnl"] - 1.01) < 0.01,
      str(fixed[41]))
check("an already-official trade is left untouched",
      fixed[30]["resolve_source"] == "polymarket-official"
      and abs(fixed[30]["pnl"] - 1.50) < 1e-9, str(fixed[30]))
check("a lookup is not wasted on confirmed trades", fake.lookups == 3,
      f"{fake.lookups} lookups for 3 provisional slugs")

with open(reconcile.STATS_PATH, encoding="utf-8") as f:
    bank = json.load(f)["bankroll"]
check("the bankroll moves by the net correction",
      abs(bank - (100.0 + res["pnl_delta"])) < 0.01,
      f"bankroll {bank}, delta {res['pnl_delta']}")

check("every correction is journalled",
      len(reconcile._read_jsonl(reconcile.CORRECTIONS_LOG)) == res["corrected"],
      str(res["corrected"]))

again = reconcile.reconcile(pub=_FakePub(), verbose=False)
check("re-running corrects nothing further (idempotent)",
      again["corrected"] == 0, str(again))

# ---------------------------------------------------------------------------
print("T14 archive copies are bounded")
# Unbounded per-trade backups each containing the whole history is O(n^2)
# growth against a fixed volume. On Railway it reached "No space left on
# device" and crash-looped the bot 19 times.
copies_root = os.path.join(bot.ARCHIVE_DIR, "copies")
os.makedirs(copies_root, exist_ok=True)
for i in range(12):
    d = os.path.join(copies_root, f"sess_{i:04d}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "trades_history.jsonl"), "w") as f:
        f.write("x" * 1000)

bot._prune_copies(copies_root, keep=3)
left = sorted(os.listdir(copies_root))
check("only the newest N copies survive", len(left) == 3, str(left))
check("the ones kept are the newest",
      left == ["sess_0009", "sess_0010", "sess_0011"], str(left))

bot._prune_copies(copies_root, keep=3)
check("pruning again is stable", len(os.listdir(copies_root)) == 3)

# The supervisor must reclaim BEFORE the bot writes, or a full volume is a
# deadlock: the copy fails, so the prune that would free space never runs.
for i in range(12, 20):
    d = os.path.join(copies_root, f"sess_{i:04d}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "big.bin"), "w") as f:
        f.write("y" * 5000)
supervisor.STATE_DIR = bot.STATE_DIR
os.environ["ARCHIVE_COPIES_KEEP"] = "3"
supervisor._reclaim_disk()
check("the supervisor reclaims space up front",
      len(os.listdir(copies_root)) == 3, str(sorted(os.listdir(copies_root))))

check("a free-space reading is available",
      bot.free_disk_mb(bot.STATE_DIR) is not None)
check("the disk warning threshold is set",
      supervisor.DISK_WARN_MB > 0, str(supervisor.DISK_WARN_MB))

# ---------------------------------------------------------------------------
print("T15 order lifecycle states")
import order_state as ostate  # noqa: E402

if os.path.exists(ostate.ORDER_EVENTS):
    os.remove(ostate.ORDER_EVENTS)

# A live order that genuinely filled.
for st in (ostate.SIGNAL, ostate.SUBMITTED):
    ostate.record(st, round_id=1, slug="s1", side="UP", requested_shares=5,
                  mode="live")
ostate.record(ostate.ACCEPTED, round_id=1, slug="s1", side="UP", order_id="ord1",
              requested_shares=5, mode="live",
              venue_response={"status": "live", "orderID": "ord1"})
ostate.record(ostate.FILLED, round_id=1, slug="s1", side="UP", order_id="ord1",
              requested_shares=5, filled_shares=5, mode="live")

# Paper: nothing was ever sent.
ostate.record(ostate.SIMULATED, round_id=2, slug="s2", side="ARB",
              filled_shares=5, mode="paper_live")
# Live, but the venue never confirmed.
ostate.record(ostate.UNVERIFIED, round_id=3, slug="s3", side="UP", mode="live")
# Venue refused.
ostate.record(ostate.REJECTED, round_id=4, slug="s4", side="DOWN", mode="live")

trail = [e["state"] for e in ostate.trail(1)]
check("the full trail is recorded in order",
      trail == ["SIGNAL", "SUBMITTED", "ACCEPTED", "FILLED"], str(trail))

s = ostate.summary()
check("only venue-confirmed fills count as fills",
      s["venue_confirmed_fills"] == 1, str(s))
check("a SIMULATED paper fill is NOT counted as a fill",
      s["simulated"] == 1 and ostate.SIMULATED in ostate.NOT_CONFIRMED, str(s))
check("an UNVERIFIED fill is NOT counted as a fill",
      s["unverified"] == 1 and ostate.UNVERIFIED in ostate.NOT_CONFIRMED, str(s))
check("a REJECTED order is NOT counted as a fill",
      s["rejected"] == 1 and ostate.REJECTED in ostate.NOT_CONFIRMED, str(s))
check("only PARTIAL/FILLED hold a position",
      set(ostate.HOLDS_POSITION) == {ostate.PARTIAL, ostate.FILLED},
      str(ostate.HOLDS_POSITION))

# The venue's own words are retained as evidence.
acc = [e for e in ostate.trail(1) if e["state"] == "ACCEPTED"][0]
check("the venue response is kept with the event",
      "orderID" in (acc.get("venue_response") or ""), str(acc.get("venue_response")))

# ---------------------------------------------------------------------------
print("T16 shipped defaults are the safe ones")
# These four decide whether the bot can spend money and on what. A regression
# in any of them is the kind that is only noticed after it has cost something.
import subprocess  # noqa: E402  (already imported above; harmless)

probe = subprocess.run(
    [sys.executable, "-c",
     "import config, json; print(json.dumps({"
     "'momentum_observe_only': config.MOMENTUM_OBSERVE_ONLY,"
     "'arb_trade': config.ARB_TRADE_ENABLED,"
     "'arb_live': config.ARB_LIVE_ENABLED,"
     "'mode': config.MODE}))"],
    capture_output=True, text=True,
    cwd=tempfile.mkdtemp(),                       # no .env in scope
    env={**os.environ, "PYTHONPATH": os.path.dirname(os.path.abspath(__file__))})
try:
    defaults = json.loads(probe.stdout.strip().splitlines()[-1])
except Exception:
    defaults = {}
    check("defaults are readable", False, probe.stdout + probe.stderr)

if defaults:
    check("momentum ships observe-only (no demonstrated edge)",
          defaults["momentum_observe_only"] is True, str(defaults))
    check("arb still trades (the only demonstrated edge)",
          defaults["arb_trade"] is True, str(defaults))
    check("LIVE arb is opt-in, never a default",
          defaults["arb_live"] is False, str(defaults))
    check("mode never defaults to live",
          defaults["mode"] != "live", str(defaults))

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED ops: {FAIL}")
print("SELFTEST_OK ops")
