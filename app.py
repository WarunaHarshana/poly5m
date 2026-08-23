"""All-in-one dashboard server + bot control API.

Combines serve.py and a premium embedded dashboard into a single file.
Run:
    python app.py          # opens http://localhost:8000 automatically

Endpoints:
    GET  /               -> embedded dashboard
    GET  /dashboard      -> embedded dashboard
    GET  /api/control    -> {running, pid, mode, cmd, exit_code}
    POST /api/start      -> launch bot.py (paper_live / live with gates)
    POST /api/stop       -> terminate the bot
    GET  /api/wallet     -> live wallet readiness (key never returned full)
    POST /api/wallet     -> save wallet settings into local .env
    GET  /api/logs       -> recent bot.log text
    GET  /state/*.json   -> state files for dashboard polling
"""

import hmac
import http.server
import json
import os
import re
import subprocess
import sys
import threading
import time
import webbrowser

import paths

# Railway injects PORT; DASH_PORT stays the local override.
PORT = int(os.getenv("DASH_PORT") or os.getenv("PORT") or "8000")
# Bind publicly only when asked (Railway sets HOST=0.0.0.0 via the Dockerfile).
# Defaulting to loopback keeps the wallet-setup endpoints off the LAN locally.
HOST = (os.getenv("DASH_HOST") or "127.0.0.1").strip()
DASH_TOKEN = (os.getenv("DASH_TOKEN") or "").strip()
if HOST not in ("127.0.0.1", "localhost", "::1") and not DASH_TOKEN:
    raise SystemExit(
        "Refusing to serve on a public interface without DASH_TOKEN.\n"
        "This dashboard can start/stop the bot and write wallet settings, so a\n"
        "public bind needs a token. Set DASH_TOKEN=<a long random string> in\n"
        "the service variables and open the dashboard as ?token=<that value>.")
HERE = os.path.dirname(os.path.abspath(__file__))
# Mirror bot.py: a non-5m instance keeps its state in state/<timeframe>/, so
# a second dashboard can watch it on another port. Example (15m on :8015):
#   $env:TIMEFRAME='15m'; $env:DASH_PORT='8015'; python app.py
_TF = (os.getenv("TIMEFRAME") or "5m").strip().lower()
_STATE_SUB = (os.getenv("STATE_SUBDIR") or ("" if _TF == "5m" else _TF)).strip()
STATE_DIR = paths.state_dir(_STATE_SUB)
ENV_PATH = os.path.join(HERE, ".env")
os.chdir(HERE)

# MODE=paper was an OFFLINE simulator against a synthetic directional book —
# meaningful only for momentum. An arb needs two real, live-quoted sides to be
# worth simulating at all, so paper_live is the paper mode now.
ALLOWED_MODES = {"paper_live", "live"}
# Only these recurring series exist (verified live 2026-07-19).
ALLOWED_TIMEFRAMES = {"5m", "15m"}


# ---------------------------------------------------------------------------
# Research export
# ---------------------------------------------------------------------------
# Files the export endpoint may send, relative to the state dir. This is an
# ALLOWLIST on purpose: the same directory holds poly_api_creds.json (CLOB API
# key, secret and passphrase), so a "everything except..." rule is one typo
# away from mailing out credentials.
EXPORT_FILES = (
    "archive/observations.jsonl",     # the labeled learning dataset
    "archive/trades_history.jsonl",   # permanent trade archive
    "archive/trades_history.csv",
    "archive/decisions.jsonl",
    "archive/decisions.csv",
    "archive/tuning_log.jsonl",       # every tuning decision + reason
    "archive/arb_executions.jsonl",   # every live leg, its response and errors
    "archive/corrections.jsonl",      # reconciliation corrections
    "archive/order_events.jsonl",     # order lifecycle transitions
    "tuned_params.json",              # parameters currently in force
    "stats.json",
    "trades.json",
    "status.json",
    "supervisor.json",
)
# Only with ?full=1 — big, and rarely what you want over a slow link.
EXPORT_FILES_FULL = (
    "bot.log",
    "bot.log.1",
)


def _export_zip(tf: str | None, full: bool = False) -> tuple[bytes, list]:
    """Zip the research data for one timeframe. Returns (bytes, manifest)."""
    import io
    import zipfile

    base = _state_dir_for(tf)
    names = list(EXPORT_FILES) + (list(EXPORT_FILES_FULL) if full else [])
    manifest = []
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in names:
            src = os.path.join(base, *rel.split("/"))
            # Refuse anything that resolves outside the state dir, so a future
            # edit to the list cannot walk into the repo or the filesystem.
            if not os.path.abspath(src).startswith(os.path.abspath(base)):
                continue
            if not os.path.isfile(src):
                continue
            try:
                zf.write(src, rel)
                manifest.append({"file": rel, "bytes": os.path.getsize(src)})
            except OSError:
                continue
        zf.writestr("MANIFEST.json", json.dumps({
            "exported_at": time.time(),
            "timeframe": tf or "5m",
            "files": manifest,
        }, indent=2))
    return buf.getvalue(), manifest


def _state_dir_for(tf: str | None) -> str:
    """State directory for a timeframe. 5m keeps the original paths."""
    tf = (tf or "5m").strip().lower()
    if tf not in ALLOWED_TIMEFRAMES:
        tf = "5m"
    return paths.state_dir("" if tf == "5m" else tf)

_lock = threading.Lock()
# One process slot PER TIMEFRAME. A single shared slot meant you could not
# run 5m and 15m together, and — worse — switching the UI to 15m while a 5m
# bot ran left Stop enabled pointing at the 5m process with no indication.
_bots: dict = {}


def _slot(tf: str | None) -> dict:
    tf = (tf or "5m").strip().lower()
    if tf not in ALLOWED_TIMEFRAMES:
        tf = "5m"
    return _bots.setdefault(tf, {"proc": None, "cmd": None, "mode": None,
                                 "log_file": None, "timeframe": tf})


# ---------------------------------------------------------------------------
# Local .env helpers (wallet setup UI — localhost only)
# ---------------------------------------------------------------------------
_ENV_KEYS = (
    "MODE", "PRIVATE_KEY", "FUNDER_ADDRESS", "SIGNATURE_TYPE",
    "LIVE_CONFIRM", "FIXED_STAKE_USD", "DAILY_LOSS_LIMIT_USD",
    "MAX_TRADES_PER_DAY", "MAX_CONSECUTIVE_LOSSES",
)

# Tunables editable from the dashboard (never secrets).
# Each field: key, label, type (float|int|bool|str), default, group, hint
# Sum-to-one arb is the only strategy. Every directional one was retired on
# measurement: mid / late_scalp / late_every / hybrid on 2026-07-17, and
# late_momentum after 41 venue-confirmed trades at -$16.57.
# NOTE 2026-07-17: mid / late_scalp / late_every / hybrid strategies were
# retired (negative or simulation-inflated expectancy) — the bot now runs
# late_momentum plus a sum-to-one arb scanner. Their settings are gone too.
STRATEGY_FIELDS = [
    # --- Diffusion Model & Quantitative Gates (H-001 - H-005) ---
    {"key": "MOMENTUM_TRADE_ENABLED", "label": "Execute trades", "type": "bool",
     "default": "1", "group": "gates",
     "hint": "Master switch for executing terminal diffusion trades."},
    {"key": "MOMENTUM_OBSERVE_ONLY", "label": "Observe only", "type": "bool",
     "default": "0", "group": "gates",
     "hint": "When on, signals are evaluated and logged for research without placing orders."},
    {"key": "DIFFUSION_Z_MIN", "label": "Min Z-score hurdle", "type": "float",
     "default": "1.80", "group": "gates",
     "hint": "Minimum standardized displacement hurdle Z = |spot-strike|/sigma_tau (~98%+ empirical win rate)."},
    {"key": "DIFFUSION_Z_BREAKOUT", "label": "Breakout Z hurdle", "type": "float",
     "default": "2.20", "group": "gates",
     "hint": "High-conviction breakout threshold where chop pinning no longer degrades signal."},
    {"key": "DIFFUSION_MAX_CHOP_MARGINAL", "label": "Max chop crosses (marginal)", "type": "int",
     "default": "3", "group": "gates",
     "hint": "Max intra-slot strike crossings permitted when 1.8 <= Z < 2.2 (H-002 pin-risk filter)."},
    {"key": "DIFFUSION_ALPHA", "label": "S-curve alpha", "type": "float",
     "default": "1.75", "group": "gates",
     "hint": "Logistic steepness calibrated across 2,439 historical slots."},
    {"key": "TERMINAL_WINDOW_MAX_S", "label": "Terminal window max (s)", "type": "int",
     "default": "20", "group": "gates",
     "hint": "Seconds remaining before slot resolution when evaluation begins (default 20s)."},
    {"key": "TERMINAL_WINDOW_MIN_S", "label": "Terminal window min (s)", "type": "int",
     "default": "5", "group": "gates",
     "hint": "Seconds remaining when order placement cuts off (default 5s)."},
    {"key": "MOMENTUM_USE_REGIME_SIZING", "label": "Inter-slot regime sizing", "type": "bool",
     "default": "1", "group": "gates",
     "hint": "H-003: Scales stake up 1.20x after high-volatility slots (prev move >= $30)."},
    {"key": "MOMENTUM_REQUIRE_VERIFIED_STRIKE", "label": "Require RTDS strike", "type": "bool",
     "default": "1", "group": "gates",
     "hint": "H-005: Rejects exchange proxy strikes to prevent 9.6% settlement skew."},
    {"key": "LATE_MOMENTUM_MIN_ENTRY", "label": "Min entry price", "type": "float",
     "default": "0.50", "group": "gates",
     "hint": "Lowest contract ask price accepted."},
    {"key": "LATE_MOMENTUM_MAX_ENTRY", "label": "Max entry price", "type": "float",
     "default": "0.99", "group": "gates",
     "hint": "Highest contract ask price accepted (skips micro-payouts 0.995+)."},
    {"key": "LATE_MOMENTUM_MIN_EDGE", "label": "Min net edge", "type": "float",
     "default": "0.00", "group": "gates",
     "hint": "Minimum net edge after subtracting Polymarket taker fees."},
    # --- General & Execution ---
    {"key": "ASSET", "label": "Default asset", "type": "str",
     "default": "btc", "group": "general",
     "hint": "btc | eth | sol (Start menu can override)"},
    {"key": "TAKER_FEE_RATE", "label": "Taker fee rate", "type": "float",
     "default": "0.07", "group": "execution",
     "hint": "Polymarket crypto taker fee curve: 0.07 * p * (1-p) per share."},
    # --- Sizing & risk ---
    {"key": "PAPER_FILL_SLIPPAGE", "label": "Paper fill slippage", "type": "float",
     "default": "0.01", "group": "execution",
     "hint": "Added to the ask on simulated fills — both legs, so a paper pair costs 2c more than quoted"},
    {"key": "FIXED_STAKE_USD", "label": "Fixed stake $", "type": "float",
     "default": "1", "group": "sizing"},
    {"key": "BASE_TRADE_USD", "label": "Base trade $", "type": "float",
     "default": "1", "group": "sizing"},
    {"key": "MAX_TRADE_USD", "label": "Max trade $", "type": "float",
     "default": "1", "group": "sizing"},
    {"key": "MAX_EXPOSURE_USD", "label": "Max exposure $", "type": "float",
     "default": "1", "group": "sizing"},
    {"key": "STARTING_BANKROLL", "label": "Starting bankroll $", "type": "float",
     "default": "50", "group": "sizing"},
    {"key": "DAILY_LOSS_LIMIT_USD", "label": "Daily loss limit $", "type": "float",
     "default": "5", "group": "sizing"},
    {"key": "DAILY_LOSS_LIMIT_PCT", "label": "Daily loss limit % (0=off)", "type": "float",
     "default": "0", "group": "sizing"},
    {"key": "MAX_TRADES_PER_DAY", "label": "Max trades / day", "type": "int",
     "default": "50", "group": "sizing"},
    {"key": "MAX_CONSECUTIVE_LOSSES", "label": "Max consecutive losses", "type": "int",
     "default": "2", "group": "sizing"},
    {"key": "MIN_ORDER_SHARES", "label": "Min order shares", "type": "float",
     "default": "5", "group": "sizing"},
    {"key": "ENFORCE_MIN_SHARES", "label": "Enforce min shares", "type": "bool",
     "default": "1", "group": "sizing"},
    {"key": "ARB_MAX_EXPOSURE_USD", "label": "Max open exposure $", "type": "float",
     "default": "10.0", "group": "sizing",
     "hint": "Total across all open pairs. Separate from MAX_EXPOSURE_USD, which "
             "sizes a single directional leg \u2014 a hedged pair cannot lose the "
             "same way, so it earns its own limit."},
    # --- Execution ---
    {"key": "BOOK_FEED_ENABLED", "label": "WebSocket order book", "type": "bool",
     "default": "1", "group": "execution",
     "hint": "Gives BOTH sides from one instant. Two REST reads seconds apart can "
             "describe a pair that never simultaneously existed."},
    {"key": "BOOK_FEED_MAX_AGE_S", "label": "Max book age (s)", "type": "float",
     "default": "2.0", "group": "execution",
     "hint": "Older than this and the scan falls back to REST rather than trusting it"},
    {"key": "ARB_REQUIRE_ATOMIC_BOOK", "label": "Only trade socket-priced pairs",
     "type": "bool", "default": "1", "group": "execution",
     "hint": "REST reads the two legs in separate round trips, so a price move "
             "between them invents a gap. 236 of 236 logged signals came from "
             "REST and none from the socket — turning this off means trading "
             "quotes that may never have coexisted."},
    # --- Live safety ---
    {"key": "ARB_LIVE_ENABLED", "label": "Send REAL orders", "type": "bool",
     "default": "0", "group": "live",
     "hint": "With MODE=live this places real FOK orders on both legs. Off = live "
             "mode still only logs signals."},
    {"key": "LIVE_BLACKOUT_WINDOWS_UTC", "label": "Blackout windows (UTC)", "type": "str",
     "default": "", "group": "live",
     "hint": "HH:MM-HH:MM, comma separated. No live order is sent inside one \u2014 "
             "a 503 between the two legs is the one path that strands a naked leg."},
]

_SETTINGS_KEYS = {f["key"] for f in STRATEGY_FIELDS}
_SETTINGS_BY_KEY = {f["key"]: f for f in STRATEGY_FIELDS}


def _settings_payload() -> dict:
    """Current strategy settings from .env + schema for the UI."""
    env = _parse_env_file()
    groups = {
        "gates": "Gates",
        "sizing": "Sizing & risk",
        "execution": "Execution",
        "live": "Live safety",
        "general": "General",
    }
    fields = []
    for f in STRATEGY_FIELDS:
        val = env.get(f["key"], f.get("default", ""))
        fields.append({**f, "value": val})
    return {
        "ok": True,
        "groups": groups,
        "fields": fields,
        "note": "Saved to local .env. Restart the bot (Stop then Start) for changes to apply.",
    }


def _save_settings(body: dict) -> dict:
    """Write allowed strategy keys to .env. Rejects secrets."""
    if not isinstance(body, dict):
        return {"ok": False, "error": "Invalid body"}
    raw = body.get("settings") if isinstance(body.get("settings"), dict) else body
    updates = {}
    errors = []
    blocked = {"PRIVATE_KEY", "POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"}
    for k, v in raw.items():
        k = str(k).strip()
        if k in blocked:
            errors.append(f"{k} cannot be set here")
            continue
        if k not in _SETTINGS_KEYS:
            continue  # ignore unknown
        meta = _SETTINGS_BY_KEY[k]
        s = "" if v is None else str(v).strip()
        if meta["type"] == "bool":
            s = "1" if s.lower() in ("1", "true", "yes", "y", "on") else "0"
        elif meta["type"] == "int":
            try:
                s = str(int(float(s)))
            except ValueError:
                errors.append(f"{k}: need integer")
                continue
        elif meta["type"] == "float":
            try:
                s = str(float(s))
            except ValueError:
                errors.append(f"{k}: need number")
                continue
        elif k == "ASSET":
            s = s.lower()
            if s not in ("btc", "eth", "sol"):
                errors.append("ASSET must be btc, eth, or sol")
        updates[k] = s
    if errors and not updates:
        return {"ok": False, "error": "; ".join(errors)}
    if not updates:
        return {"ok": False, "error": "No valid settings to save"}
    try:
        _upsert_env(updates)
    except Exception as e:
        return {"ok": False, "error": f"Failed to write .env: {e}"}
    out = _settings_payload()
    out["ok"] = True
    out["saved"] = list(updates.keys())
    if errors:
        out["warnings"] = errors
    return out


def _parse_env_file(path: str = ENV_PATH) -> dict:
    """Parse KEY=VALUE lines from .env (best-effort; comments ignored)."""
    out = {}
    if not os.path.isfile(path):
        return out
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, _, v = s.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                # Strip inline comments: VALUE # comment
                if " #" in v:
                    v = v.split(" #", 1)[0].rstrip()
                if k:
                    out[k] = v
    except Exception:
        pass
    return out


def _upsert_env(updates: dict, path: str = ENV_PATH) -> None:
    """Update or append keys in .env without rewriting unrelated comments."""
    updates = {str(k): "" if v is None else str(v) for k, v in updates.items()}
    lines = []
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    found = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in updates:
                new_lines.append(f"{k}={updates[k]}\n")
                found.add(k)
                continue
        new_lines.append(line if line.endswith("\n") else line + "\n")
    missing = [k for k in updates if k not in found]
    if missing:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] += "\n"
        if new_lines and new_lines[-1].strip():
            new_lines.append("\n")
        new_lines.append("# --- updated by dashboard live wallet wizard ---\n")
        for k in missing:
            new_lines.append(f"{k}={updates[k]}\n")
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


def _mask_secret(val: str | None) -> str:
    if not val:
        return ""
    v = val.strip()
    if len(v) <= 10:
        return "********"
    return f"{v[:6]}…{v[-4:]}"


def _norm_addr(addr: str | None) -> str:
    if not addr:
        return ""
    a = addr.strip()
    if a.startswith("0x") or a.startswith("0X"):
        return "0x" + a[2:]
    if re.fullmatch(r"[0-9a-fA-F]{40}", a):
        return "0x" + a
    return a


def _wallet_setting(env: dict, key: str, default: str = "") -> str:
    """Read a wallet setting the way the BOT reads it: .env, then environment.

    The wizard originally looked only at the local .env file, which is correct
    for a laptop and wrong everywhere else. On a container platform .env is
    dockerignored and does not exist, while the real values arrive as service
    variables — so the dashboard reported "private key not set" and refused to
    check the balance on a deployment that was, in fact, fully configured. That
    is the one screen someone uses to confirm their setup before risking money,
    so it has to reflect what config.py actually sees.
    """
    val = (env.get(key) or "").strip()
    if val:
        return val
    return (os.getenv(key) or default).strip()


def _wallet_status() -> dict:
    env = _parse_env_file()
    pk = _wallet_setting(env, "PRIVATE_KEY")
    funder = _norm_addr(_wallet_setting(env, "FUNDER_ADDRESS"))
    try:
        # Default must match config.SIGNATURE_TYPE (3), not 0. The wizard
        # defaulted to 0 and so reported "Signature type set (0)" on a bot that
        # was actually authenticating with type 3 — sending the operator to
        # debug a setting that was already right.
        sig = int(_wallet_setting(env, "SIGNATURE_TYPE") or "3")
    except ValueError:
        sig = 3
    live_confirm = _wallet_setting(env, "LIVE_CONFIRM").lower() in (
        "yes", "y", "true", "1")
    pk_ok = bool(pk) and len(pk) >= 32
    funder_ok = bool(re.fullmatch(r"0x[0-9a-fA-F]{40}", funder or ""))
    sig_ok = sig in (0, 1, 3)
    ready = pk_ok and funder_ok and sig_ok and live_confirm

    checklist = [
        {"id": "private_key", "ok": pk_ok,
         "label": "Private key saved in local .env",
         "hint": "Signer key only — never share or commit it."},
        {"id": "funder", "ok": funder_ok,
         "label": "Funder address looks valid (0x…40 hex)",
         "hint": "Usually your Polymarket proxy / deposit address."},
        {"id": "signature_type", "ok": sig_ok,
         "label": f"Signature type set ({sig})",
         "hint": "0 = MetaMask EOA · 1 = email/Magic proxy · 3 = browser/proxy wallet"},
        {"id": "live_confirm", "ok": live_confirm,
         "label": "LIVE_CONFIRM=yes (kill-switch unlocked)",
         "hint": "Required before real-money mode can start."},
        {"id": "funds", "ok": None,
         "label": "Funded on Polymarket (USDC) + gas if EOA (POL)",
         "hint": "Check polymarket.com portfolio — bot cannot verify balance here."},
    ]
    guide = {
        "0": {
            "title": "EOA (type 0)",
            "when": "Funder address is exactly the same as your private-key address.",
            "steps": [
                "Funder = address derived from PRIVATE_KEY.",
                "Signature type = 0.",
                "Need USDC + POL for gas on that address.",
            ],
        },
        "1": {
            "title": "POLY_PROXY (type 1)",
            "when": "Some email/Magic proxy setups (if type 3 shows $0 balance).",
            "steps": [
                "Try type 3 first for polymarket.com deposit wallets.",
                "Only use type 1 if balance is wrong on type 3.",
            ],
        },
        "2": {
            "title": "Gnosis Safe (type 2)",
            "when": "Only if you trade through a Gnosis Safe on Polymarket.",
            "steps": ["Set funder to the Safe address.", "Signature type = 2."],
        },
        "3": {
            "title": "Deposit wallet / EIP-1271 (type 3) — usual for site accounts",
            "when": "Funder (deposit address) differs from your private-key EOA.",
            "steps": [
                "Funder = Polymarket profile/deposit address (holds USDC).",
                "Private key = controlling EOA.",
                "Signature type = 3 — required for correct CLOB collateral balance.",
                "Type 1 often shows $0.00 for the same funder.",
            ],
        },
    }
    return {
        "ok": True,
        "ready": ready,
        "private_key_set": pk_ok,
        "private_key_hint": _mask_secret(pk) if pk_ok else "",
        "funder_address": funder,
        "signature_type": sig,
        "live_confirm": live_confirm,
        "mode": env.get("MODE") or "paper_live",
        "fixed_stake_usd": env.get("FIXED_STAKE_USD") or "1",
        "daily_loss_limit_usd": env.get("DAILY_LOSS_LIMIT_USD") or "5",
        "max_trades_per_day": env.get("MAX_TRADES_PER_DAY") or "10",
        "checklist": checklist,
        "guide": guide,
        "env_path": ENV_PATH,
        "warnings": [
            "This wizard writes only to your local .env on this machine.",
            "Never paste your private key into chat, Discord, or GitHub.",
            "Start with $1 bets and a small deposit you can lose.",
            "Local bankroll is an estimate — Polymarket wallet is source of truth.",
        ],
    }


def _reload_config_from_env() -> None:
    """Push latest .env wallet fields into the in-process config module."""
    env = _parse_env_file()
    try:
        import config as cfg
        if env.get("PRIVATE_KEY"):
            cfg.PRIVATE_KEY = env.get("PRIVATE_KEY")
        if env.get("FUNDER_ADDRESS"):
            cfg.FUNDER_ADDRESS = _norm_addr(env.get("FUNDER_ADDRESS"))
        if env.get("SIGNATURE_TYPE") not in (None, ""):
            cfg.SIGNATURE_TYPE = int(env.get("SIGNATURE_TYPE"))
        if env.get("CLOB_HOST"):
            cfg.CLOB_HOST = env.get("CLOB_HOST")
        if env.get("CHAIN_ID"):
            cfg.CHAIN_ID = int(env.get("CHAIN_ID"))
        cfg.POLY_API_KEY = (env.get("POLY_API_KEY") or "").strip() or None
        cfg.POLY_API_SECRET = (env.get("POLY_API_SECRET") or "").strip() or None
        cfg.POLY_API_PASSPHRASE = (env.get("POLY_API_PASSPHRASE") or "").strip() or None
    except Exception:
        pass


_last_balance_ok = None  # cache last good reading across transient Windows socket errors
_balance_in_flight = False
_balance_lock = threading.Lock()


def _fetch_polymarket_balance() -> dict:
    """Live USDC collateral for the configured funder (needs private key).

    Transient CLOB timeouts are common on Windows; we return the last good
    balance when possible instead of flooding the UI/logs with errors.
    """
    global _last_balance_ok, _balance_in_flight
    env = _parse_env_file()
    # Same source order as the bot: .env first, then service variables. Reading
    # only .env made this endpoint refuse to check the balance on a deployment
    # whose credentials were set as environment variables — i.e. exactly the
    # pre-live sanity check it exists for.
    pk = _wallet_setting(env, "PRIVATE_KEY")
    funder = _norm_addr(_wallet_setting(env, "FUNDER_ADDRESS"))
    if not pk or not funder:
        return {
            "ok": False,
            "balance_usd": None,
            "allowance_usd": None,
            "funder": funder or None,
            "error": "No PRIVATE_KEY / FUNDER_ADDRESS in .env or the "
                     "environment.",
            "ts": __import__("time").time(),
        }
    # Collapse concurrent dashboard polls (15s interval can overlap with 30s timeout).
    with _balance_lock:
        if _balance_in_flight:
            if _last_balance_ok and _last_balance_ok.get("balance_usd") is not None:
                cached = dict(_last_balance_ok)
                cached["stale"] = True
                cached["ok"] = True
                cached["ts"] = __import__("time").time()
                return cached
            return {
                "ok": False,
                "balance_usd": None,
                "allowance_usd": None,
                "funder": funder,
                "error": "Balance request already in flight",
                "ts": __import__("time").time(),
            }
        _balance_in_flight = True
    try:
        last_err = None
        for attempt in range(2):
            try:
                _reload_config_from_env()
                from polymarket_client import PolymarketClient
                client = PolymarketClient(use_shared=True)
                # First poll may refresh chain allowance; retry is read-only.
                out = client.get_usdc_balance(refresh=(attempt == 0))
                out["ts"] = __import__("time").time()
                if out.get("raw") is not None:
                    out["raw_preview"] = str(out.get("raw"))[:240]
                    del out["raw"]
                if out.get("ok"):
                    _last_balance_ok = dict(out)
                    return out
                # Soft fail with stale cache
                last_err = out.get("error") or "balance unavailable"
                if attempt == 0 and (
                    "timeout" in str(last_err).lower()
                    or "timed out" in str(last_err).lower()
                    or "request exception" in str(last_err).lower()
                ):
                    __import__("time").sleep(1.2)
                    continue
                break
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if attempt == 0 and (
                    "10035" in msg or "timed out" in msg or "timeout" in msg
                    or "non-blocking" in msg or "would block" in msg
                    or "request exception" in msg
                ):
                    __import__("time").sleep(1.2)
                    continue
                break

        err = str(last_err or "balance fetch failed")
        if "API key create/derive failed" in err:
            err = (
                "API key create/derive failed. Sync Windows clock; check key/"
                "funder/signature type. See bot log for details."
            )
        elif ("10035" in err or "non-blocking" in err.lower()
              or "timeout" in err.lower() or "timed out" in err.lower()
              or "request exception" in err.lower()):
            err = "CLOB busy/timeout - showing last balance if known"
            if _last_balance_ok and _last_balance_ok.get("balance_usd") is not None:
                cached = dict(_last_balance_ok)
                cached["stale"] = True
                cached["error"] = err
                cached["ts"] = __import__("time").time()
                cached["ok"] = True
                return cached

        return {
            "ok": False,
            "balance_usd": None,
            "allowance_usd": None,
            "funder": funder,
            "error": err,
            "ts": __import__("time").time(),
        }
    finally:
        with _balance_lock:
            _balance_in_flight = False


def _save_wallet(body: dict) -> dict:
    """Persist wallet fields into .env. Empty private_key leaves existing value."""
    updates = {}
    if "funder_address" in body:
        fa = _norm_addr(body.get("funder_address"))
        if fa and not re.fullmatch(r"0x[0-9a-fA-F]{40}", fa):
            return {"ok": False, "error": "Funder address must be 0x + 40 hex chars."}
        updates["FUNDER_ADDRESS"] = fa
    if "signature_type" in body and body.get("signature_type") not in (None, ""):
        try:
            sig = int(body.get("signature_type"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "Signature type must be 0, 1, or 3."}
        if sig not in (0, 1, 2, 3):
            return {"ok": False, "error": "Signature type must be 0, 1, 2, or 3."}
        updates["SIGNATURE_TYPE"] = str(sig)
    pk = body.get("private_key")
    if pk is not None and str(pk).strip():
        pk = str(pk).strip()
        if pk.startswith("0x") or pk.startswith("0X"):
            pk_body = pk[2:]
        else:
            pk_body = pk
            pk = "0x" + pk
        if not re.fullmatch(r"[0-9a-fA-F]{64}", pk_body):
            return {"ok": False,
                    "error": "Private key must be 64 hex chars (optional 0x prefix)."}
        updates["PRIVATE_KEY"] = pk
    if "live_confirm" in body:
        updates["LIVE_CONFIRM"] = "yes" if body.get("live_confirm") else "no"
    for k_src, k_env in (
        ("fixed_stake_usd", "FIXED_STAKE_USD"),
        ("daily_loss_limit_usd", "DAILY_LOSS_LIMIT_USD"),
        ("max_trades_per_day", "MAX_TRADES_PER_DAY"),
    ):
        if body.get(k_src) not in (None, ""):
            updates[k_env] = str(body.get(k_src)).strip()
    if not updates:
        return {"ok": False, "error": "No wallet fields to save."}
    try:
        if not os.path.isfile(ENV_PATH):
            # seed from example if present
            ex = os.path.join(HERE, ".env.example")
            if os.path.isfile(ex):
                with open(ex, "r", encoding="utf-8") as f:
                    raw = f.read()
                with open(ENV_PATH, "w", encoding="utf-8") as f:
                    f.write(raw)
            else:
                with open(ENV_PATH, "w", encoding="utf-8") as f:
                    f.write("# created by dashboard\n")
        _upsert_env(updates)
    except Exception as e:
        return {"ok": False, "error": f"Failed to write .env: {e}"}
    st = _wallet_status()
    st["ok"] = True
    st["saved"] = list(updates.keys())
    # never echo private key
    return st


# ---------------------------------------------------------------------------
# Bot process management (same logic as serve.py + stdout logging)
# ---------------------------------------------------------------------------
def _running(tf: str | None = None) -> bool:
    """True if that timeframe's bot is alive (any bot when tf is None)."""
    if tf is None:
        return any(b.get("proc") is not None and b["proc"].poll() is None
                   for b in _bots.values())
    p = _slot(tf)["proc"]
    return p is not None and p.poll() is None


def _running_timeframes() -> list:
    return [tf for tf, b in _bots.items()
            if b.get("proc") is not None and b["proc"].poll() is None]


def _get_logs(tf: str | None = None) -> str:
    log_path = os.path.join(_state_dir_for(tf) if tf else STATE_DIR, "bot.log")
    if not os.path.exists(log_path):
        return "No logs found. Click 'Start' above to begin trading and generate logs."
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            return "".join(lines[-100:])  # last 100 lines
    except Exception as e:
        return f"Error reading logs: {e}"


def _start(opts: dict) -> dict:
    with _lock:
        # Guard PER TIMEFRAME: 5m and 15m are separate processes with separate
        # state, so blocking on "any bot running" needlessly prevented running
        # both series at once.
        _tf_req = str(opts.get("timeframe") or "5m").strip().lower()
        if _tf_req not in ALLOWED_TIMEFRAMES:
            _tf_req = "5m"
        if _running(_tf_req):
            return {"ok": False,
                    "error": f"A {_tf_req} run is already in progress \u2014 "
                             f"stop that one first."}
        mode = str(opts.get("mode", "paper_live"))
        if mode not in ALLOWED_MODES:
            return {"ok": False,
                    "error": "Mode must be paper_live or live."}

        # Real-money gates (UI + CLI both go through these checks when started here)
        if mode == "live":
            w = _wallet_status()
            if not w.get("private_key_set"):
                return {"ok": False,
                        "error": "Live blocked: save a private key in Live Wallet Setup first."}
            if not w.get("funder_address"):
                return {"ok": False,
                        "error": "Live blocked: set Funder address in Live Wallet Setup."}
            if not w.get("live_confirm"):
                return {"ok": False,
                        "error": "Live blocked: enable LIVE_CONFIRM in Live Wallet Setup."}
            if not opts.get("live_ack"):
                return {"ok": False,
                        "error": "Live blocked: check the real-money risk acknowledgement."}
            phrase = str(opts.get("live_phrase") or "").strip().upper()
            if phrase != "LIVE":
                return {"ok": False,
                        "error": "Live blocked: type LIVE in the confirm box to start."}
            # Force tiny default bet if caller forgot
            if opts.get("bet") in (None, ""):
                opts = dict(opts)
                opts["bet"] = w.get("fixed_stake_usd") or "1"

        # Use python -u for unbuffered output to make logs real-time
        cmd = [sys.executable, "-u", "bot.py", "--mode", mode]
        # TIMEFRAME must reach the child through the ENVIRONMENT: bot.py
        # resolves the slug prefix and its state paths at import time.
        timeframe = str(opts.get("timeframe") or "5m").strip().lower()
        if timeframe not in ALLOWED_TIMEFRAMES:
            timeframe = "5m"
        asset = str(opts.get("asset") or "btc").strip().lower()
        if asset not in ("btc", "eth", "sol"):
            asset = "btc"
        cmd += ["--asset", asset]

        # Persist asset choice for next load
        try:
            _upsert_env({"ASSET": asset})
        except Exception:
            pass
        try:
            rounds = int(opts.get("rounds") or 100)
            if mode == "live":
                rounds = min(rounds, 50)  # hard cap first live sessions via UI
            cmd += ["--rounds", str(max(1, rounds))]
            if opts.get("bankroll") not in (None, ""):
                cmd += ["--bankroll", str(float(opts["bankroll"]))]
            if opts.get("bet") not in (None, ""):
                bet = float(opts["bet"])
                if mode == "live" and bet > 5:
                    return {"ok": False,
                            "error": "Live UI cap: bet must be <= $5 (edit .env / CLI for more)."}
                cmd += ["--bet", str(bet)]
        except (TypeError, ValueError):
            return {"ok": False, "error": "Bankroll, bet, and rounds must be numbers."}
        if opts.get("reset"):
            cmd += ["--reset"]

        # Clear/initialize log file (per timeframe)
        run_state_dir = _state_dir_for(timeframe)
        os.makedirs(run_state_dir, exist_ok=True)
        log_path = os.path.join(run_state_dir, "bot.log")
        try:
            # Close only THIS timeframe's previous handle — closing a shared
            # one would have yanked the log out from under the other series.
            if _slot(timeframe)["log_file"]:
                _slot(timeframe)["log_file"].close()
        except Exception:
            pass

        try:
            log_file = open(log_path, "w", encoding="utf-8")
        except Exception as e:
            return {"ok": False, "error": f"Failed to open log file: {e}"}

        try:
            # Fresh env for child so .env reloads with python-dotenv in config
            child_env = os.environ.copy()
            child_env["TIMEFRAME"] = timeframe
            proc = subprocess.Popen(
                cmd, cwd=HERE, stdout=log_file, stderr=subprocess.STDOUT,
                env=child_env,
            )
        except Exception as e:
            log_file.close()
            return {"ok": False, "error": f"Failed to launch bot: {e}"}

        _slot(timeframe).update(proc=proc, cmd=cmd, mode=mode,
                                log_file=log_file, timeframe=timeframe)
        return {"ok": True, "running": True, "pid": proc.pid, "cmd": cmd,
                "mode": mode, "timeframe": timeframe}


def _stop(tf: str | None = None) -> dict:
    with _lock:
        b = _slot(tf)
        p = b["proc"]
        if p is None or p.poll() is not None:
            return {"ok": True, "running": False,
                    "message": f"Nothing was running on {b['timeframe']}."}
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        
        # Close the log file handle
        try:
            if b["log_file"]:
                b["log_file"].close()
        except Exception:
            pass
        b["log_file"] = None
        b["proc"] = None

        return {"ok": True, "running": False,
                "message": f"Stopped the {b['timeframe']} bot."}


def _supervised(tf: str | None = None) -> dict:
    """A bot the SUPERVISOR started, which app.py did not spawn and cannot see.

    On Railway the supervisor owns the bot process, so `_bots` is empty and the
    dashboard reported "idle" with Start enabled while real money was trading.
    Clicking Start there would have raced a second bot against the same state
    directory and lock. There is no child handle to check, so liveness comes
    from the two artefacts the running bot does maintain: the supervisor's
    recorded pid, and a status.json that is still being written.
    """
    state_dir = _state_dir_for(tf)
    try:
        with open(os.path.join(state_dir, "supervisor.json"), encoding="utf-8") as f:
            sup = json.load(f)
    except (OSError, ValueError):
        return {}
    pid = sup.get("bot_pid")
    if not pid:
        return {}
    try:
        with open(os.path.join(state_dir, "status.json"), encoding="utf-8") as f:
            age = time.time() - float(json.load(f).get("updated_at") or 0)
    except (OSError, ValueError, TypeError):
        return {}
    # Generous: a slot-end wait plus a restart is well under this.
    if age > 300:
        return {}
    return {"pid": pid, "mode": sup.get("mode"), "status_age_s": round(age, 1)}


def _state(tf: str | None = None) -> dict:
    b = _slot(tf)
    p = b["proc"]
    out = {
        "running": _running(b["timeframe"]),
        "pid": p.pid if p else None,
        "mode": b["mode"],
        "timeframe": b["timeframe"],
        "cmd": b["cmd"],
        "exit_code": (p.poll() if p else None),
        # So the UI can show that another series is still trading.
        "running_timeframes": _running_timeframes(),
        "managed_by": "dashboard" if _running(b["timeframe"]) else None,
    }
    if not out["running"]:
        sup = _supervised(b["timeframe"])
        if sup:
            out.update(running=True, managed_by="supervisor", pid=sup["pid"],
                       mode=sup["mode"], status_age_s=sup["status_age_s"])
    return out


# ---------------------------------------------------------------------------
# Embedded dashboard HTML
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Diffusion Engine · Polymarket</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #07090e;
  --bg-card: #0d121d;
  --bg-card-hover: #131a29;
  --bg-glass: rgba(13, 18, 29, 0.75);
  --border: rgba(255, 255, 255, 0.08);
  --border-focus: rgba(56, 189, 248, 0.4);
  --txt-main: #f8fafc;
  --txt-muted: #94a3b8;
  --txt-dim: #64748b;
  --green: #10b981;
  --green-dim: rgba(16, 185, 129, 0.12);
  --green-glow: rgba(16, 185, 129, 0.35);
  --red: #f43f5e;
  --red-dim: rgba(244, 63, 94, 0.12);
  --blue: #0284c7;
  --blue-dim: rgba(2, 132, 199, 0.12);
  --cyan: #06b6d4;
  --cyan-dim: rgba(6, 182, 212, 0.12);
  --amber: #f59e0b;
  --amber-dim: rgba(245, 158, 11, 0.12);
  --purple: #8b5cf6;
  --purple-dim: rgba(139, 92, 246, 0.12);
  --radius: 12px;
  --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  --font-mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background-color: var(--bg);
  color: var(--txt-main);
  font-family: var(--font-sans);
  font-size: 13.5px;
  line-height: 1.5;
  min-height: 100vh;
  padding: 16px 20px 40px;
  -webkit-font-smoothing: antialiased;
}

::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: rgba(0,0,0,0.2); }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.15); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.25); }

.mono { font-family: var(--font-mono); }
.container { max-width: 1360px; margin: 0 auto; display: flex; flex-direction: column; gap: 16px; }

.card {
  background: var(--bg-glass);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 16px 20px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
  transition: border-color 0.2s, box-shadow 0.2s;
}
.card:hover { border-color: rgba(255, 255, 255, 0.12); }

/* Header */
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 20px;
  background: linear-gradient(180deg, rgba(17, 24, 39, 0.8) 0%, rgba(13, 18, 29, 0.8) 100%);
}
.brand { display: flex; align-items: center; gap: 12px; }
.brand-icon {
  width: 32px; height: 32px; border-radius: 8px;
  background: linear-gradient(135deg, #0284c7, #8b5cf6);
  display: flex; align-items: center; justify-content: center;
  font-size: 17px; font-weight: 800; color: #fff;
  box-shadow: 0 0 16px rgba(2, 132, 199, 0.4);
}
.brand-text { font-size: 17px; font-weight: 800; letter-spacing: -0.02em; }
.brand-text span { color: var(--cyan); }
.brand-badge {
  font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 6px;
  background: var(--purple-dim); color: var(--purple); border: 1px solid rgba(139, 92, 246, 0.3);
}
.header-meta { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.badge {
  font-size: 11px; font-weight: 600; padding: 4px 10px; border-radius: 6px;
  display: inline-flex; align-items: center; gap: 6px;
  background: rgba(255,255,255,0.04); border: 1px solid var(--border); color: var(--txt-muted);
}
.badge.mode { font-family: var(--font-mono); color: var(--txt-main); font-weight: 700; }
.badge.mode.live { background: var(--red-dim); color: var(--red); border-color: rgba(244, 63, 94, 0.4); }
.badge.mode.paper { background: var(--cyan-dim); color: var(--cyan); border-color: rgba(6, 182, 212, 0.4); }
.conn-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 8px var(--green); }
.conn-dot.down { background: var(--red); box-shadow: 0 0 8px var(--red); }

/* Quick Controls */
.controls-bar {
  display: flex; align-items: center; justify-content: space-between; gap: 12px;
  padding: 12px 18px; flex-wrap: wrap;
}
.ctl-group { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.ctl-label { font-size: 11px; font-weight: 600; color: var(--txt-dim); text-transform: uppercase; letter-spacing: 0.05em; }
.ctl-input, .ctl-select {
  background: rgba(0,0,0,0.35); border: 1px solid var(--border); border-radius: 6px;
  color: var(--txt-main); font-family: var(--font-mono); font-size: 12.5px;
  padding: 6px 10px; outline: none; transition: border-color 0.2s;
}
.ctl-input:focus, .ctl-select:focus { border-color: var(--cyan); }
.ctl-input[type="number"] { width: 75px; }
.btn {
  padding: 7px 16px; border-radius: 6px; font-weight: 600; font-size: 12.5px;
  cursor: pointer; display: inline-flex; align-items: center; gap: 6px;
  border: none; outline: none; transition: all 0.2s;
}
.btn-primary { background: linear-gradient(135deg, #10b981, #059669); color: #fff; box-shadow: 0 2px 10px rgba(16, 185, 129, 0.3); }
.btn-primary:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 14px rgba(16, 185, 129, 0.4); }
.btn-stop { background: var(--red-dim); color: var(--red); border: 1px solid rgba(244, 63, 94, 0.3); }
.btn-stop:hover:not(:disabled) { background: rgba(244, 63, 94, 0.2); }
.btn-ghost { background: rgba(255,255,255,0.05); color: var(--txt-main); border: 1px solid var(--border); }
.btn-ghost:hover { background: rgba(255,255,255,0.08); border-color: rgba(255,255,255,0.15); }
.btn:disabled { opacity: 0.45; cursor: not-allowed; transform: none !important; }

/* Hero Live Slot Bar */
.hero-slot {
  background: linear-gradient(180deg, rgba(15, 23, 42, 0.9) 0%, rgba(13, 18, 29, 0.95) 100%);
  border: 1px solid rgba(56, 189, 248, 0.2);
  display: flex; flex-direction: column; gap: 14px;
}
.hero-head {
  display: flex; align-items: center; justify-content: space-between;
  border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 10px;
}
.hero-title { font-size: 14.5px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
.hero-title a { color: var(--txt-main); text-decoration: none; }
.hero-title a:hover { color: var(--cyan); text-decoration: underline; }
.hero-timer { font-size: 18px; font-weight: 800; color: var(--cyan); display: flex; align-items: center; gap: 6px; }
.hero-grid {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px;
}
.hero-stat {
  background: rgba(0, 0, 0, 0.25); border: 1px solid var(--border); border-radius: 8px;
  padding: 12px 14px; display: flex; flex-direction: column; gap: 4px;
}
.hero-stat .k { font-size: 11px; font-weight: 600; color: var(--txt-dim); text-transform: uppercase; }
.hero-stat .v { font-size: 20px; font-weight: 800; }
.hero-stat .s { font-size: 11.5px; color: var(--txt-muted); display: flex; align-items: center; gap: 4px; }
.pill-tag {
  font-size: 10.5px; font-weight: 700; padding: 2px 7px; border-radius: 4px; display: inline-block;
}
.pill-breakout { background: var(--green-dim); color: var(--green); border: 1px solid rgba(16,185,129,0.3); }
.pill-strong { background: var(--cyan-dim); color: var(--cyan); border: 1px solid rgba(6,182,212,0.3); }
.pill-marginal { background: var(--amber-dim); color: var(--amber); border: 1px solid rgba(245,158,11,0.3); }
.pill-quiet { background: rgba(255,255,255,0.05); color: var(--txt-dim); border: 1px solid var(--border); }

/* Performance KPI Tiles */
.tiles-grid {
  display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px;
}
.tile {
  background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 14px 16px; display: flex; flex-direction: column; gap: 4px;
}
.tile .k { font-size: 11px; font-weight: 600; color: var(--txt-dim); text-transform: uppercase; }
.tile .v { font-size: 22px; font-weight: 800; letter-spacing: -0.02em; }
.tile .s { font-size: 11.5px; color: var(--txt-muted); }
.pos { color: var(--green); }
.neg { color: var(--red); }

/* Main 2-Column Grid */
.main-grid {
  display: grid; grid-template-columns: 1.25fr 1fr; gap: 16px;
}
.col { display: flex; flex-direction: column; gap: 16px; }

/* Panel Headers */
.phead {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--border);
}
.phead span:first-child { font-size: 13.5px; font-weight: 700; letter-spacing: -0.01em; }
.phead span:last-child { font-size: 11.5px; color: var(--txt-muted); }

/* Chart */
.curve-wrap { width: 100%; height: 220px; position: relative; }

/* Blotter Table */
.table-wrap { overflow-x: auto; max-height: 380px; }
table { width: 100%; border-collapse: collapse; text-align: left; font-size: 12px; }
th {
  padding: 8px 10px; font-weight: 600; color: var(--txt-dim); border-bottom: 1px solid var(--border);
  position: sticky; top: 0; background: var(--bg-card); z-index: 2;
}
td { padding: 9px 10px; border-bottom: 1px solid rgba(255,255,255,0.03); }
tr:hover td { background: rgba(255,255,255,0.02); }
.side-up { color: var(--green); font-weight: 700; background: var(--green-dim); padding: 2px 6px; border-radius: 4px; }
.side-down { color: var(--red); font-weight: 700; background: var(--red-dim); padding: 2px 6px; border-radius: 4px; }
.out-won { color: var(--green); font-weight: 700; }
.out-lost { color: var(--red); font-weight: 700; }

/* Model Explanation Card */
.spec-box { display: flex; flex-direction: column; gap: 10px; font-size: 12.5px; line-height: 1.6; }
.spec-item { background: rgba(0,0,0,0.25); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; }
.spec-item strong { color: var(--txt-main); }

/* Live Terminal Log */
.console-box {
  background: #04060a; border: 1px solid var(--border); border-radius: 8px;
  font-family: var(--font-mono); font-size: 11.5px; line-height: 1.45; color: #cbd5e1;
  padding: 12px; height: 320px; overflow-y: auto; white-space: pre-wrap; word-break: break-all;
}

/* Settings Drawer (Slide-over) */
.drawer-overlay {
  position: fixed; inset: 0; background: rgba(0,0,0,0.7); backdrop-filter: blur(4px);
  display: none; z-index: 100;
}
.drawer-overlay.open { display: block; }
.drawer {
  position: fixed; top: 0; right: -480px; width: 460px; height: 100vh;
  background: #0d131f; border-left: 1px solid var(--border); z-index: 101;
  padding: 24px; display: flex; flex-direction: column; gap: 16px;
  box-shadow: -10px 0 30px rgba(0,0,0,0.6); transition: right 0.3s cubic-bezier(0.16, 1, 0.3, 1);
  overflow-y: auto;
}
.drawer.open { right: 0; }
.drawer-head { display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border); padding-bottom: 12px; }
.drawer-title { font-size: 16px; font-weight: 800; }
.field-row { display: flex; flex-direction: column; gap: 4px; margin-bottom: 10px; }
.field-row label { font-size: 11.5px; font-weight: 600; color: var(--txt-dim); }
.field-row input, .field-row select {
  background: rgba(0,0,0,0.4); border: 1px solid var(--border); border-radius: 6px;
  color: var(--txt-main); font-family: var(--font-mono); font-size: 12px; padding: 7px 10px;
}
.field-hint { font-size: 10.5px; color: var(--txt-dim); line-height: 1.35; }

@media (max-width: 1080px) {
  .hero-grid { grid-template-columns: repeat(2, 1fr); }
  .tiles-grid { grid-template-columns: repeat(3, 1fr); }
  .main-grid { grid-template-columns: 1fr; }
}
@media (max-width: 640px) {
  .tiles-grid { grid-template-columns: repeat(2, 1fr); }
  .drawer { width: 100%; right: -100%; }
}
</style>
</head>
<body>
<div class="container">

  <!-- ===== HEADER ===== -->
  <header class="card header">
    <div class="brand">
      <div class="brand-icon">&#9889;</div>
      <div>
        <div class="brand-text">BTC <span>DIFFUSION</span> ENGINE</div>
      </div>
      <span class="brand-badge">H-001 – H-005 Model</span>
    </div>
    <div class="header-meta">
      <span class="badge mode" id="mode-badge">MODE: —</span>
      <div class="badge"><div class="conn-dot" id="conn-dot"></div><span id="conn-txt">RTDS STREAM</span></div>
      <span class="badge" id="ws-badge">BOOK: WS ACTIVE</span>
      <span class="badge mono" id="clock-txt">--:--:-- ET</span>
    </div>
  </header>

  <!-- ===== QUICK CONTROLS BAR ===== -->
  <div class="card controls-bar">
    <div class="ctl-group">
      <span class="ctl-label">Mode</span>
      <select id="ctl-mode" class="ctl-select">
        <option value="paper_live">paper_live · Simulated Orders</option>
        <option value="live">live · REAL CAPITAL</option>
      </select>
      <span class="ctl-label">Timeframe</span>
      <select id="ctl-tf" class="ctl-select">
        <option value="5m">5m Slot</option>
        <option value="15m">15m Slot</option>
      </select>
      <span class="ctl-label">Stake $</span>
      <input type="number" id="ctl-bet" class="ctl-input" value="5" min="1" step="1">
      <span class="ctl-label">Bankroll $</span>
      <input type="number" id="ctl-bankroll" class="ctl-input" value="250" min="10" step="10">
      <label style="display:flex;align-items:center;gap:5px;font-size:11.5px;color:var(--txt-muted);cursor:pointer">
        <input type="checkbox" id="ctl-reset" checked> Reset stats
      </label>
    </div>
    <div class="ctl-group">
      <button id="btn-start" class="btn btn-primary">&#9654; Start Bot</button>
      <button id="btn-stop" class="btn btn-stop" disabled>&#9632; Stop</button>
      <button id="btn-settings" class="btn btn-ghost">&#9881; Settings</button>
      <span id="bot-state-txt" class="mono" style="font-size:11px;color:var(--txt-dim)">IDLE</span>
    </div>
  </div>

  <!-- ===== HERO LIVE SLOT BAR ===== -->
  <div class="card hero-slot">
    <div class="hero-head">
      <div class="hero-title">
        <span style="color:var(--cyan)">&#9679;</span>
        <span id="hero-market-name">Waiting for active slot…</span>
      </div>
      <div class="hero-timer">
        <span style="font-size:12px;color:var(--txt-dim);font-weight:600">CLOSES IN:</span>
        <span id="hero-countdown" class="mono">--:--</span>
      </div>
    </div>
    <div class="hero-grid">
      <div class="hero-stat">
        <span class="k">Spot vs Strike</span>
        <span class="v mono" id="hero-spot">$0.00</span>
        <span class="s mono" id="hero-strike-delta">Strike: $0.00 (Δ $0.00)</span>
      </div>
      <div class="hero-stat">
        <span class="k">Terminal Displacement</span>
        <div style="display:flex;align-items:center;gap:8px">
          <span class="v mono" id="hero-z">Z = 0.00</span>
          <span class="pill-tag pill-quiet" id="hero-z-pill">WAITING</span>
        </div>
        <span class="s mono" id="hero-sigma">σ_τ: $0.00 (window 20s..5s)</span>
      </div>
      <div class="hero-stat">
        <span class="k">Model Win Probability</span>
        <span class="v mono" id="hero-prob" style="color:var(--cyan)">50.0%</span>
        <span class="s mono" id="hero-edge">Net Edge: +0.0% vs Book</span>
      </div>
      <div class="hero-stat">
        <span class="k">Session &amp; Chop Gating</span>
        <span class="v" id="hero-session" style="font-size:14.5px">Asian Session</span>
        <span class="s mono" id="hero-chop">Chop: 0 crosses (Safe &le; 3)</span>
      </div>
    </div>
  </div>

  <!-- ===== 6 KPI METRIC TILES ===== -->
  <div class="tiles-grid">
    <div class="tile">
      <span class="k">Total Bankroll</span>
      <span class="v mono" id="kpi-bankroll">$250.00</span>
      <span class="s" id="kpi-bankroll-sub">Start: $250.00</span>
    </div>
    <div class="tile">
      <span class="k">Realized P&amp;L</span>
      <span class="v mono" id="kpi-pnl">$0.00</span>
      <span class="s" id="kpi-pnl-pct">+0.0%</span>
    </div>
    <div class="tile">
      <span class="k">Win Rate</span>
      <span class="v mono" id="kpi-winrate">—</span>
      <span class="s" id="kpi-wl-count">0 wins · 0 losses</span>
    </div>
    <div class="tile">
      <span class="k">Total Trades</span>
      <span class="v mono" id="kpi-trades">0</span>
      <span class="s" id="kpi-fees">Fees: $0.00</span>
    </div>
    <div class="tile">
      <span class="k">Avg Net Edge</span>
      <span class="v mono" id="kpi-edge">—</span>
      <span class="s">After Polymarket taker fee</span>
    </div>
    <div class="tile">
      <span class="k">Circuit Breaker</span>
      <span class="v mono pos" id="kpi-circuit">ARMED</span>
      <span class="s">Daily loss protection active</span>
    </div>
  </div>

  <!-- ===== MAIN 2-COLUMN SECTION ===== -->
  <div class="main-grid">
    
    <!-- LEFT COLUMN: Equity Chart & Trade Blotter -->
    <div class="col">
      <!-- Equity Curve -->
      <div class="card">
        <div class="phead">
          <span>Bankroll Equity Curve</span>
          <span id="chart-fills-count" class="mono">0 fills</span>
        </div>
        <div class="curve-wrap">
          <svg id="equity-svg" width="100%" height="100%" viewBox="0 0 700 220" preserveAspectRatio="none"></svg>
        </div>
      </div>

      <!-- Trade Blotter -->
      <div class="card">
        <div class="phead">
          <span>Directional Trade Blotter</span>
          <span id="blotter-sub">Newest First</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time (ET)</th>
                <th>Round</th>
                <th>Side</th>
                <th>Entry</th>
                <th>Stake</th>
                <th>Edge</th>
                <th>Outcome</th>
                <th>P&amp;L</th>
                <th>Slot Link</th>
              </tr>
            </thead>
            <tbody id="blotter-rows">
              <tr><td colspan="9" style="text-align:center;color:var(--txt-dim);padding:24px">Waiting for trades… start the bot above.</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- RIGHT COLUMN: Model Spec & Live Logs -->
    <div class="col">
      <!-- Model Specification -->
      <div class="card">
        <div class="phead">
          <span>Regime-Conditioned Terminal Diffusion</span>
          <span>H-001 – H-005 Spec</span>
        </div>
        <div class="spec-box">
          <div class="spec-item">
            <strong>1. Brownian Terminal Scaling (H-001)</strong><br>
            Standardized displacement <span class="mono" style="color:var(--cyan)">Z = |P_spot - P_strike| / σ_τ</span> where <span class="mono">σ_τ = P_spot &times; &radic;(var_sec &times; τ)</span>. In the terminal window (<span class="mono">τ &le; 15s</span>), moves <span class="mono">Z &ge; 2.20</span> achieve <strong style="color:var(--green)">99.4%–100% win rate</strong>.
          </div>
          <div class="spec-item">
            <strong>2. Pin-Risk Chop Gate (H-002)</strong><br>
            If displacement is marginal (<span class="mono">1.8 &le; Z &lt; 2.2</span>), slots with <span class="mono">&gt; 3</span> intra-slot strike crossings are rejected to eliminate pin-risk reversals.
          </div>
          <div class="spec-item">
            <strong>3. Volatility Clustering &amp; Verified Strike (H-003, H-005)</strong><br>
            Scales conviction <span class="mono">1.20x</span> after wide moves. Enforces official <strong>Chainlink RTDS ticks</strong> to eliminate the 9.64% settlement skew caused by exchange proxies.
          </div>
        </div>
      </div>

      <!-- Live Console -->
      <div class="card">
        <div class="phead">
          <span>Live Bot Terminal Logs</span>
          <span id="log-status" class="mono">streaming</span>
        </div>
        <div class="console-box" id="console-logs">Connecting to live log stream…</div>
      </div>
    </div>

  </div>

</div>

<!-- ===== SLIDE-OUT SETTINGS DRAWER ===== -->
<div class="drawer-overlay" id="drawer-overlay"></div>
<div class="drawer" id="drawer">
  <div class="drawer-head">
    <span class="drawer-title">Bot Configuration &amp; Gates</span>
    <button type="button" class="btn btn-ghost" id="drawer-close" style="padding:4px 8px">&times;</button>
  </div>
  
  <div style="display:flex;flex-direction:column;gap:12px">
    <div style="font-size:12px;font-weight:700;color:var(--cyan);text-transform:uppercase;letter-spacing:0.05em">Quantitative Model (H-001 – H-005)</div>
    <div class="field-row">
      <label>Min Z-Score Hurdle</label>
      <input type="number" step="0.05" id="cfg-DIFFUSION_Z_MIN" value="1.80">
      <span class="field-hint">Minimum displacement hurdle for ~98%+ empirical win rate.</span>
    </div>
    <div class="field-row">
      <label>Breakout Z Threshold</label>
      <input type="number" step="0.05" id="cfg-DIFFUSION_Z_BREAKOUT" value="2.20">
      <span class="field-hint">High-conviction threshold where chop filter is bypassed (100% win rate).</span>
    </div>
    <div class="field-row">
      <label>Max Chop Crosses (Marginal Z)</label>
      <input type="number" id="cfg-DIFFUSION_MAX_CHOP_MARGINAL" value="3">
      <span class="field-hint">Max intra-slot strike crossings permitted when 1.8 &le; Z &lt; 2.2.</span>
    </div>
    <div class="field-row">
      <label>S-Curve Steepness Alpha</label>
      <input type="number" step="0.05" id="cfg-DIFFUSION_ALPHA" value="1.75">
      <span class="field-hint">Empirical logistic calibration across 2,439 historical slots.</span>
    </div>
    <div class="field-row">
      <label>Terminal Window (Max..Min Seconds)</label>
      <div style="display:flex;gap:8px">
        <input type="number" id="cfg-TERMINAL_WINDOW_MAX_S" value="20" placeholder="Max (20s)">
        <input type="number" id="cfg-TERMINAL_WINDOW_MIN_S" value="5" placeholder="Min (5s)">
      </div>
      <span class="field-hint">Seconds before slot resolution when signals are executed.</span>
    </div>

    <div style="font-size:12px;font-weight:700;color:var(--cyan);text-transform:uppercase;letter-spacing:0.05em;margin-top:10px">Risk &amp; Wallet</div>
    <div class="field-row">
      <label>Daily Loss Limit ($)</label>
      <input type="number" id="cfg-DAILY_LOSS_LIMIT_USD" value="50">
    </div>
    <div class="field-row">
      <label>Max Consecutive Losses</label>
      <input type="number" id="cfg-MAX_CONSECUTIVE_LOSSES" value="2">
    </div>
    <div class="field-row">
      <label>Funder Address (Polymarket Deposit / Proxy)</label>
      <input type="text" id="cfg-FUNDER_ADDRESS" placeholder="0x…">
    </div>
    <div class="field-row">
      <label>Signer Private Key</label>
      <input type="password" id="cfg-PRIVATE_KEY" placeholder="0x… leave blank to keep existing">
    </div>
    <div class="field-row">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
        <input type="checkbox" id="cfg-LIVE_CONFIRM">
        <span style="color:var(--red);font-weight:700">Enable LIVE_CONFIRM=yes for real money</span>
      </label>
    </div>

    <div style="display:flex;gap:10px;margin-top:14px">
      <button class="btn btn-primary" id="btn-save-settings" style="flex:1">Save Settings</button>
      <button class="btn btn-ghost" id="btn-reload-settings">Reload</button>
    </div>
    <div id="settings-msg" style="font-size:12px;color:var(--green)"></div>
  </div>
</div>

<script>
let lastStats = {}, lastTrades = [], lastStatus = {};
let topRoundSeen = -1;

let currentEndTs = null;

function fmtUsd(v) {
  if (v == null || isNaN(v)) return '$0.00';
  return (v < 0 ? '-$' : '$') + Math.abs(v).toFixed(2);
}

function getSessionFromUtc() {
  const h = new Date().getUTCHours();
  if (h >= 0 && h < 8) return "Asian Session";
  if (h >= 8 && h < 13) return "London Session";
  if (h >= 13 && h < 17) return "US-Europe Overlap";
  if (h >= 17 && h < 22) return "US Afternoon Session";
  return "Asian Open Pre-Session";
}

function updateClock() {
  try {
    const et = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/New_York', hour12: true,
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
    document.getElementById('clock-txt').textContent = et + ' ET';
  } catch(e) {}
}
setInterval(updateClock, 1000);
updateClock();

function updateCountdown() {
  const el = document.getElementById('hero-countdown');
  if (!el) return;
  if (!currentEndTs) {
    el.textContent = '--:--';
    return;
  }
  const now = Date.now() / 1000;
  const rem = Math.max(0, Math.round(currentEndTs - now));
  const mm = String(Math.floor(rem / 60)).padStart(2, '0');
  const ss = String(rem % 60).padStart(2, '0');
  el.textContent = `${mm}:${ss}`;
  if (rem <= 20 && rem >= 5) {
    el.style.color = 'var(--green)';
  } else if (rem < 5) {
    el.style.color = 'var(--red)';
  } else {
    el.style.color = 'var(--cyan)';
  }
}
setInterval(updateCountdown, 500);

/* ===== DRAW SVG EQUITY CURVE ===== */
function drawCurve(trades, startBankroll) {
  const svg = document.getElementById('equity-svg');
  if (!svg) return;
  const W = 700, H = 220, pad = 20;
  if (!trades || trades.length < 1) {
    svg.innerHTML = `<line x1="${pad}" y1="${H/2}" x2="${W-pad}" y2="${H/2}" stroke="rgba(255,255,255,0.1)" stroke-dasharray="4 4"/>`;
    return;
  }
  const pts = [startBankroll, ...trades.map(t => t.bankroll_after != null ? t.bankroll_after : startBankroll)];
  const minV = Math.min(...pts) * 0.998;
  const maxV = Math.max(...pts) * 1.002;
  const range = (maxV - minV) || 1;

  const getX = i => pad + (i / (pts.length - 1 || 1)) * (W - pad * 2);
  const getY = v => H - pad - ((v - minV) / range) * (H - pad * 2);

  let pathD = `M ${getX(0)} ${getY(pts[0])}`;
  let areaD = `M ${getX(0)} ${H - pad} L ${getX(0)} ${getY(pts[0])}`;
  for (let i = 1; i < pts.length; i++) {
    pathD += ` L ${getX(i)} ${getY(pts[i])}`;
    areaD += ` L ${getX(i)} ${getY(pts[i])}`;
  }
  areaD += ` L ${getX(pts.length - 1)} ${H - pad} Z`;

  const isUp = pts[pts.length - 1] >= startBankroll;
  const strokeCol = isUp ? '#10b981' : '#f43f5e';

  svg.innerHTML = `
    <defs>
      <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="${strokeCol}" stop-opacity="0.25"/>
        <stop offset="100%" stop-color="${strokeCol}" stop-opacity="0.0"/>
      </linearGradient>
    </defs>
    <path d="${areaD}" fill="url(#eqGrad)"/>
    <path d="${pathD}" fill="none" stroke="${strokeCol}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${getX(pts.length - 1)}" cy="${getY(pts[pts.length - 1])}" r="4" fill="${strokeCol}" stroke="#07090e" stroke-width="2"/>
  `;
}

/* ===== MAIN RENDER LOOP ===== */
function render(stats, trades, status) {
  if (!stats) stats = {};
  if (!trades) trades = [];
  if (!status) status = {};
  lastStats = stats; lastTrades = trades; lastStatus = status;

  const mode = (stats.mode || status.mode || 'paper_live').toUpperCase();
  const isLive = mode === 'LIVE';
  const modeBadge = document.getElementById('mode-badge');
  modeBadge.textContent = 'MODE: ' + mode;
  modeBadge.className = 'badge mode ' + (isLive ? 'live' : 'paper');

  // Hero Status
  const q = status.question || status.slug || 'BTC 5-minute Up/Down (monitoring)';
  document.getElementById('hero-market-name').textContent = q;
  
  if (status.end_ts) {
    currentEndTs = status.end_ts;
  } else if (status.remaining_s != null) {
    currentEndTs = (Date.now() / 1000) + status.remaining_s;
  }
  updateCountdown();

  const spot = status.spot_price;
  const strike = status.strike_price;
  if (spot != null) {
    document.getElementById('hero-spot').textContent = fmtUsd(spot);
    if (strike != null && strike > 0) {
      const delta = spot - strike;
      const dSign = delta >= 0 ? '+' : '-';
      document.getElementById('hero-strike-delta').textContent = `Strike: ${fmtUsd(strike)} (${dSign}${fmtUsd(Math.abs(delta))})`;
      document.getElementById('hero-strike-delta').style.color = delta >= 0 ? 'var(--green)' : 'var(--red)';
    } else {
      document.getElementById('hero-strike-delta').textContent = 'Waiting for verified strike…';
      document.getElementById('hero-strike-delta').style.color = 'var(--txt-dim)';
    }
  } else {
    document.getElementById('hero-spot').textContent = 'Connecting…';
  }

  // Z-Score & Probability
  const z = status.z_score;
  if (z != null) {
    document.getElementById('hero-z').textContent = `Z = ${z.toFixed(2)}`;
    const pill = document.getElementById('hero-z-pill');
    if (z >= 2.20) {
      pill.textContent = 'BREAKOUT (99%+)';
      pill.className = 'pill-tag pill-breakout';
    } else if (z >= 1.80) {
      pill.textContent = 'STRONG EDGE';
      pill.className = 'pill-tag pill-strong';
    } else if (z >= 1.0) {
      pill.textContent = 'MARGINAL';
      pill.className = 'pill-tag pill-marginal';
    } else {
      pill.textContent = 'SUB-HURDLE';
      pill.className = 'pill-tag pill-quiet';
    }
  } else {
    document.getElementById('hero-z').textContent = 'Z = 0.00';
    document.getElementById('hero-z-pill').textContent = 'SCANNING';
    document.getElementById('hero-z-pill').className = 'pill-tag pill-quiet';
  }

  if (status.sigma_tau != null) {
    document.getElementById('hero-sigma').textContent = `σ_τ: ${fmtUsd(status.sigma_tau)} (scale collapse)`;
  } else {
    document.getElementById('hero-sigma').textContent = 'σ_τ: waiting for late window (≤60s)';
  }

  const prob = status.model_prob;
  if (prob != null) {
    document.getElementById('hero-prob').textContent = `${(prob * 100).toFixed(1)}%`;
  } else {
    document.getElementById('hero-prob').textContent = '50.0%';
  }

  if (status.edge_net != null) {
    const edgeSign = status.edge_net >= 0 ? '+' : '';
    document.getElementById('hero-edge').textContent = `Net Edge: ${edgeSign}${(status.edge_net * 100).toFixed(1)}% (post-fee)`;
  } else {
    document.getElementById('hero-edge').textContent = 'Net Edge: +0.0% vs Book';
  }

  // Session & Chop
  const sessName = status.session_name || getSessionFromUtc();
  document.getElementById('hero-session').textContent = sessName;
  if (status.chop_crosses != null) {
    const safeTxt = status.chop_crosses <= 3 ? 'Safe &le; 3' : 'Pin-Risk Gated';
    document.getElementById('hero-chop').innerHTML = `Chop: ${status.chop_crosses} crosses (${safeTxt})`;
    document.getElementById('hero-chop').style.color = status.chop_crosses <= 3 ? 'var(--txt-muted)' : 'var(--amber)';
  } else {
    document.getElementById('hero-chop').innerHTML = 'Chop: 0 crosses (Safe &le; 3)';
    document.getElementById('hero-chop').style.color = 'var(--txt-muted)';
  }

  // KPI Tiles
  const startBk = stats.starting_bankroll || 250;
  const curBk = (isLive && status.wallet_balance_usd != null) ? status.wallet_balance_usd : (stats.bankroll != null ? stats.bankroll : startBk);
  const pnl = stats.realized_pnl || (curBk - startBk);
  const pnlPct = startBk ? (100 * pnl / startBk) : 0;

  document.getElementById('kpi-bankroll').textContent = fmtUsd(curBk);
  document.getElementById('kpi-bankroll-sub').textContent = isLive && status.wallet_balance_usd != null ? 'Polymarket USDC Wallet' : `Start: ${fmtUsd(startBk)}`;

  const pnlEl = document.getElementById('kpi-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+' : '-') + fmtUsd(Math.abs(pnl));
  pnlEl.className = 'v mono ' + (pnl >= 0 ? 'pos' : 'neg');

  const pnlPctEl = document.getElementById('kpi-pnl-pct');
  pnlPctEl.textContent = (pnl >= 0 ? '+' : '') + pnlPct.toFixed(1) + '%';
  pnlPctEl.className = 's ' + (pnl >= 0 ? 'pos' : 'neg');

  const winRate = stats.win_rate != null ? stats.win_rate : (stats.trades ? (100 * stats.wins / stats.trades) : null);
  document.getElementById('kpi-winrate').textContent = winRate != null ? `${winRate.toFixed(1)}%` : '—';
  document.getElementById('kpi-wl-count').textContent = `${stats.wins || 0} wins · ${stats.losses || 0} losses`;

  document.getElementById('kpi-trades').textContent = stats.trades || 0;
  document.getElementById('kpi-fees').textContent = `Fees: ${fmtUsd(stats.fees_paid || 0)}`;

  if (stats.avg_edge != null && stats.avg_edge !== 0) {
    document.getElementById('kpi-edge').textContent = (stats.avg_edge > 0 ? '+' : '') + (stats.avg_edge * 100).toFixed(1) + '%';
  } else {
    document.getElementById('kpi-edge').textContent = '—';
  }

  const circuitEl = document.getElementById('kpi-circuit');
  if (stats.halted) {
    circuitEl.textContent = 'TRIPPED';
    circuitEl.className = 'v mono neg';
  } else {
    circuitEl.textContent = 'ARMED';
    circuitEl.className = 'v mono pos';
  }

  document.getElementById('chart-fills-count').textContent = `${trades.length} fills`;
  drawCurve(trades, startBk);

  // Render Blotter Rows
  const tbody = document.getElementById('blotter-rows');
  if (!trades || trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--txt-dim);padding:24px">Waiting for trades… monitoring slot gates.</td></tr>';
  } else {
    const sorted = [...trades].reverse();
    tbody.innerHTML = sorted.map(t => {
      const sideClass = t.side === 'UP' ? 'side-up' : 'side-down';
      const wonClass = t.won ? 'out-won' : 'out-lost';
      const wonTxt = t.won ? 'WON' : (t.resolved === false ? 'PENDING' : 'LOST');
      const timeTxt = t.time_et || (t.ts ? new Date(t.ts * 1000).toLocaleTimeString() : '—');
      const edgeTxt = t.edge_net != null ? `${(t.edge_net * 100).toFixed(1)}%` : (t.edge != null ? `${(t.edge * 100).toFixed(1)}%` : '—');
      const pnlTxt = t.pnl != null ? ((t.pnl >= 0 ? '+' : '-') + fmtUsd(Math.abs(t.pnl))) : '—';
      const slotLink = t.event_url ? `<a href="${t.event_url}" target="_blank" style="color:var(--cyan);text-decoration:none">&#128279; View</a>` : '—';
      return `<tr>
        <td class="mono">${timeTxt}</td>
        <td>R${t.round_id || '—'}</td>
        <td><span class="${sideClass}">${t.side}</span></td>
        <td class="mono">${t.entry_price != null ? t.entry_price.toFixed(3) : '—'}</td>
        <td class="mono">${fmtUsd(t.stake)}</td>
        <td class="mono">${edgeTxt}</td>
        <td><span class="${wonClass}">${wonTxt}</span></td>
        <td class="mono ${t.pnl >= 0 ? 'pos' : 'neg'}">${pnlTxt}</td>
        <td>${slotLink}</td>
      </tr>`;
    }).join('');
  }
}

/* ===== LOG STREAM POLLER ===== */
let prevLogLength = 0;
async function fetchLogs() {
  try {
    const res = await fetch('/api/logs?tf=' + document.getElementById('ctl-tf').value, { cache: 'no-store' });
    if (res.ok) {
      const text = await res.text();
      const el = document.getElementById('console-logs');
      el.textContent = text || 'Log stream initialized. Waiting for bot output…';
      if (text.length !== prevLogLength) {
        prevLogLength = text.length;
        el.scrollTop = el.scrollHeight;
      }
    }
  } catch(e) {}
}

/* ===== CONTROL STATE POLLER ===== */
async function pollState() {
  const tf = document.getElementById('ctl-tf').value;
  try {
    const [stRes, trRes, staRes, ctlRes] = await Promise.all([
      fetch(`/state/status.json?tf=${tf}&_=${Date.now()}`).then(r => r.ok ? r.json() : {}),
      fetch(`/state/trades.json?tf=${tf}&_=${Date.now()}`).then(r => r.ok ? r.json() : []),
      fetch(`/state/stats.json?tf=${tf}&_=${Date.now()}`).then(r => r.ok ? r.json() : {}),
      fetch(`/api/control?tf=${tf}&_=${Date.now()}`).then(r => r.ok ? r.json() : {}),
    ]);

    render(staRes, trRes, stRes);

    const isRunning = ctlRes.running || false;
    document.getElementById('btn-start').disabled = isRunning;
    document.getElementById('btn-stop').disabled = !isRunning;
    document.getElementById('bot-state-txt').textContent = isRunning ? `RUNNING (PID ${ctlRes.pid})` : 'IDLE';
    document.getElementById('bot-state-txt').style.color = isRunning ? 'var(--green)' : 'var(--txt-dim)';
  } catch(e) {}
}

setInterval(pollState, 1500);
setInterval(fetchLogs, 1500);
pollState();
fetchLogs();

/* ===== BUTTON ACTIONS ===== */
document.getElementById('btn-start').onclick = async () => {
  const mode = document.getElementById('ctl-mode').value;
  const tf = document.getElementById('ctl-tf').value;
  const stake = parseFloat(document.getElementById('ctl-bet').value) || 5;
  const bankroll = parseFloat(document.getElementById('ctl-bankroll').value) || 250;
  const reset = document.getElementById('ctl-reset').checked;

  document.getElementById('btn-start').disabled = true;
  document.getElementById('bot-state-txt').textContent = 'STARTING…';

  try {
    const res = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode, timeframe: tf, bet: stake, bankroll, reset, asset: 'btc' })
    });
    const data = await res.json();
    if (!data.ok) alert(data.error || 'Failed to start bot');
  } catch(e) {
    alert('Error connecting to start API: ' + e);
  }
  pollState();
};

document.getElementById('btn-stop').onclick = async () => {
  const tf = document.getElementById('ctl-tf').value;
  document.getElementById('btn-stop').disabled = true;
  document.getElementById('bot-state-txt').textContent = 'STOPPING…';
  try {
    await fetch('/api/stop', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timeframe: tf })
    });
  } catch(e) {}
  pollState();
};

/* ===== SETTINGS DRAWER ===== */
const drawer = document.getElementById('drawer');
const overlay = document.getElementById('drawer-overlay');
document.getElementById('btn-settings').onclick = () => {
  drawer.classList.add('open');
  overlay.classList.add('open');
  loadSettings();
};
document.getElementById('drawer-close').onclick = () => {
  drawer.classList.remove('open');
  overlay.classList.remove('open');
};
overlay.onclick = () => {
  drawer.classList.remove('open');
  overlay.classList.remove('open');
};

async function loadSettings() {
  try {
    const res = await fetch('/api/settings');
    if (!res.ok) return;
    const data = await res.json();
    for (const [k, v] of Object.entries(data)) {
      const el = document.getElementById('cfg-' + k);
      if (el) {
        if (el.type === 'checkbox') el.checked = (v === '1' || v === 'yes' || v === true);
        else el.value = v;
      }
    }
  } catch(e) {}
}

document.getElementById('btn-save-settings').onclick = async () => {
  const payload = {};
  document.querySelectorAll('[id^="cfg-"]').forEach(el => {
    const key = el.id.replace('cfg-', '');
    payload[key] = el.type === 'checkbox' ? (el.checked ? 'yes' : 'no') : el.value;
  });
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    const msg = document.getElementById('settings-msg');
    msg.textContent = data.ok ? 'Settings saved to .env (restarting bot will apply new values)' : (data.error || 'Failed to save');
    setTimeout(() => { msg.textContent = ''; }, 4000);
  } catch(e) {
    alert('Error saving settings: ' + e);
  }
};
document.getElementById('btn-reload-settings').onclick = loadSettings;
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTTP Handler
# ---------------------------------------------------------------------------
class Handler(http.server.BaseHTTPRequestHandler):
    """Serves the embedded dashboard, state JSON files, and control API."""

    # Browser disconnects mid-poll are normal; don't dump tracebacks.
    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError,
                TimeoutError, OSError):
            pass

    # -- auth -------------------------------------------------------------
    # Bound to loopback this server is trusted by definition. Bound to
    # 0.0.0.0 (Railway) it is on the public internet, and it can start/stop
    # the bot and write wallet settings into .env — so a token is mandatory
    # there. Pass it once as ?token=..., it is stored as a cookie afterwards.
    def _authed(self) -> bool:
        if not DASH_TOKEN:
            return True
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(self.path).query)
        supplied = (q.get("token") or [None])[0]
        if not supplied:
            supplied = self.headers.get("X-Dash-Token")
        if not supplied:
            for part in (self.headers.get("Cookie") or "").split(";"):
                name, _, val = part.strip().partition("=")
                if name == "dash_token":
                    supplied = val
                    break
        return bool(supplied) and hmac.compare_digest(str(supplied), DASH_TOKEN)

    def _deny(self):
        body = (b"<h1>401</h1><p>Add <code>?token=YOUR_DASH_TOKEN</code> to the "
                b"URL. Set DASH_TOKEN in the service variables.</p>")
        self.send_response(401)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except OSError:
            pass

    def _set_token_cookie(self):
        """Persist a freshly supplied ?token= so polling requests carry it."""
        from urllib.parse import urlparse, parse_qs
        if not DASH_TOKEN:
            return
        q = parse_qs(urlparse(self.path).query)
        if (q.get("token") or [None])[0]:
            self.send_header(
                "Set-Cookie",
                f"dash_token={DASH_TOKEN}; Path=/; HttpOnly; SameSite=Strict")

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
            pass

    def _html(self, html_str, code=200):
        body = html_str.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._set_token_cookie()
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self._json({"error": "File not found"}, 404)

    def do_GET(self):
        path = self.path.split("?")[0]

        # Unauthenticated liveness probe for the platform healthcheck.
        if path == "/healthz":
            return self._json({"ok": True, "ts": time.time()})

        if not self._authed():
            return self._deny()

        # Dashboard
        if path in ("/", "/dashboard"):
            return self._html(DASHBOARD_HTML)

        # Performance analysis (archive report; same engine as analyze.py)
        if path == "/api/analyze":
            from urllib.parse import urlparse, parse_qs
            import analyze
            q = parse_qs(urlparse(self.path).query)
            hours = None
            try:
                if q.get("hours") and float(q["hours"][0]) > 0:
                    hours = float(q["hours"][0])
            except (TypeError, ValueError):
                hours = None
            session = (q.get("session") or [None])[0]
            tf = (q.get("tf") or ["5m"])[0]
            era = (q.get("era") or ["0"])[0] in ("1", "true", "yes")
            try:
                return self._json(analyze.compute_report(
                    hours, session, state_dir=_state_dir_for(tf), era=era))
            except Exception as e:
                return self._json({"ok": False, "error": str(e)}, 500)

        # Research export: everything needed to analyse and tune locally,
        # as one zip. `?full=1` adds book recordings and the bot log.
        if path == "/api/export":
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            tf = (q.get("tf") or ["5m"])[0]
            full = (q.get("full") or ["0"])[0] in ("1", "true", "yes")
            try:
                blob, manifest = _export_zip(tf, full=full)
            except Exception as e:
                return self._json({"ok": False, "error": str(e)}, 500)
            if not manifest:
                return self._json(
                    {"ok": False,
                     "error": "no data files yet — has the bot recorded a slot?"},
                    404)
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            name = f"polybot-data-{tf}-{stamp}.zip"
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(blob)))
            self.send_header("Content-Disposition",
                             f'attachment; filename="{name}"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(blob)
            except OSError:
                pass
            return

        # Order lifecycle: what the VENUE said, per order. This is the
        # difference between "we sent an order" and "it filled".
        if path == "/api/orders":
            from urllib.parse import urlparse, parse_qs
            try:
                import order_state
                q = parse_qs(urlparse(self.path).query)
                rid = (q.get("round") or [None])[0]
                if rid is not None:
                    try:
                        return self._json({"ok": True,
                                           "trail": order_state.trail(int(rid))})
                    except ValueError:
                        return self._json({"ok": False,
                                           "error": "bad round id"}, 400)
                return self._json({
                    "ok": True,
                    "summary": order_state.summary(),
                    "recent": order_state.load(limit=60),
                })
            except Exception as e:
                return self._json({"ok": False, "error": str(e)}, 500)

        # Learning state: dataset size, parameters in force, recent decisions.
        # Without this the self-tuning loop is invisible unless you SSH in.
        if path == "/api/learning":
            # The naked-position alert is the highest-severity thing on the
            # whole dashboard, and it used to be computed INSIDE the same try
            # block as the tuner import — so any tuner error took the alert down
            # with it, silently, exactly when something had gone wrong enough to
            # throw. The arb half now stands on its own.
            out = {"ok": True, "arb_alerts": [], "arb_exec": {}}
            try:
                import arb_executor
                out["arb_alerts"] = arb_executor.open_alerts()
                out["arb_exec"] = arb_executor.stats()
            except Exception as e:
                out["arb_error"] = str(e)

            try:
                with open(os.path.join(STATE_DIR, "supervisor.json"),
                          encoding="utf-8") as f:
                    out["supervisor"] = json.load(f)
            except (OSError, ValueError):
                out["supervisor"] = {}

            try:
                import tuner
                usable, census = tuner.load_attempts()
                out.update({
                    "dataset": census,
                    "usable_attempts": len(usable),
                    "needed_to_tune": tuner.MIN_USABLE_ROWS,
                    "active_params": tuner.active_params(),
                    "defaults": tuner.DEFAULTS,
                    "tuned": tuner.load_tuned(),
                    "journal": list(reversed(tuner.read_journal()[-20:])),
                })
            except Exception as e:
                out["tuner_error"] = str(e)

            # Observations are the retired momentum dataset. Kept because the
            # archive is still worth counting, but nothing tunes on it now.
            try:
                import observations
                rows = observations.load()
                out["observations"] = len(rows)
                out["labeled"] = sum(
                    1 for r in rows if r.get("outcome") in ("UP", "DOWN"))
            except Exception:
                out["observations"] = out["labeled"] = 0

            return self._json(out)

        # Control API
        if path == "/api/control":
            from urllib.parse import urlparse, parse_qs
            _tf = (parse_qs(urlparse(self.path).query).get("tf") or ["5m"])[0]
            return self._json(_state(_tf))

        # Live wallet readiness (never returns full private key)
        if path == "/api/wallet":
            return self._json(_wallet_status())

        # Strategy bands / risk settings (safe keys only)
        if path == "/api/settings":
            return self._json(_settings_payload())

        # Live Polymarket USDC balance (authenticated CLOB)
        if path == "/api/balance":
            return self._json(_fetch_polymarket_balance())

        # Logs API
        if path == "/api/logs":
            from urllib.parse import urlparse, parse_qs
            _tf = (parse_qs(urlparse(self.path).query).get("tf") or ["5m"])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            body = _get_logs(_tf).encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # State JSON files
        if path.startswith("/state/") and path.endswith(".json"):
            # Sanitize: only allow simple filenames
            from urllib.parse import urlparse, parse_qs
            fname = os.path.basename(path)
            if fname in ("stats.json", "trades.json", "status.json"):
                _tf = (parse_qs(urlparse(self.path).query).get("tf") or ["5m"])[0]
                fpath = os.path.join(_state_dir_for(_tf), fname)
                # A NUL-filled/partial file (process killed mid-write) would
                # otherwise be served as garbage and break the page render.
                try:
                    with open(fpath, "r", encoding="utf-8") as _fh:
                        _raw = _fh.read()
                    if not _raw or _raw[:1] == "\x00":
                        raise ValueError("state file not readable yet")
                    json.loads(_raw)
                except FileNotFoundError:
                    pass
                except Exception:
                    self.send_response(404)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"state file unreadable")
                    return
                return self._serve_file(fpath, "application/json")

        # Fallback 404
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Not Found")

    def do_POST(self):
        if not self._authed():
            return self._deny()
        route = self.path.split("?")[0]
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            opts = json.loads(raw or b"{}")
        except ValueError:
            return self._json({"ok": False, "error": "Invalid JSON body."}, 400)
        if route == "/api/start":
            return self._json(_start(opts))
        if route == "/api/stop":
            _tf = (opts or {}).get("timeframe") if isinstance(opts, dict) else None
            return self._json(_stop(_tf))
        if route == "/api/wallet":
            return self._json(_save_wallet(opts if isinstance(opts, dict) else {}))
        if route == "/api/settings":
            return self._json(_save_settings(opts if isinstance(opts, dict) else {}))
        return self._json({"ok": False, "error": "Unknown endpoint."}, 404)

    def log_message(self, fmt, *args):
        # Suppress default logging to keep terminal clean
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    url = f"http://localhost:{PORT}"

    # Ensure state directory exists
    os.makedirs(STATE_DIR, exist_ok=True)

    httpd = http.server.ThreadingHTTPServer((HOST, PORT), Handler)

    print()
    print("  +------------------------------------------------------+")
    print("  |          POLYMARKET BTC BOT                           |")
    print("  |          All-in-One Dashboard Server                  |")
    print("  +------------------------------------------------------+")
    print(f"  |  Dashboard:    {url:<39}|")
    print(f"  |  API start:    POST /api/start                       |")
    print(f"  |  API stop:     POST /api/stop                        |")
    print(f"  |  API control:  GET  /api/control                     |")
    print(f"  |  API wallet:   GET/POST /api/wallet                  |")
    print(f"  |  API settings: GET/POST /api/settings                |")
    print(f"  |  API balance:  GET  /api/balance                     |")
    print("  +------------------------------------------------------+")
    print("  |  Edit late/mid bands in Strategy settings on the UI.  |")
    print("  |  Live money: use Live wallet setup on the dashboard.  |")
    print("  |  Ctrl+C to shut down.                                 |")
    print("  +------------------------------------------------------+")
    print()

    # Auto-open browser (never on a headless host — there is no browser there)
    if HOST in ("127.0.0.1", "localhost") and not os.getenv("NO_BROWSER"):
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        _stop()
        print("\n  Goodbye! Bot stopped.\n")
