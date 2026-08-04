"""All-in-one dashboard server + bot control API.

Combines serve.py and a premium embedded dashboard into a single file.
Run:
    python app.py          # opens http://localhost:8000 automatically

Endpoints:
    GET  /               -> embedded dashboard
    GET  /dashboard      -> embedded dashboard
    GET  /api/control    -> {running, pid, mode, cmd, exit_code}
    POST /api/start      -> launch bot.py (paper / paper_live / live with gates)
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

ALLOWED_MODES = {"paper", "paper_live", "live"}
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
    "tuned_params.json",              # parameters currently in force
    "stats.json",
    "trades.json",
    "status.json",
    "supervisor.json",
)
# Only with ?full=1 — big, and rarely what you want over a slow link.
EXPORT_FILES_FULL = (
    "archive/book_recordings.jsonl",
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
# NOTE 2026-07-17: mid / late_scalp / late_every / hybrid strategies were
# retired (negative or simulation-inflated expectancy) — the bot now runs
# late_momentum plus a sum-to-one arb scanner. Their settings are gone too.
STRATEGY_FIELDS = [
    # --- General ---
    {"key": "ASSET", "label": "Default asset", "type": "str",
     "default": "btc", "group": "general",
     "hint": "btc | eth | sol (Start menu can override)"},
    {"key": "STRATEGY", "label": "Strategy", "type": "str",
     "default": "late_momentum", "group": "general",
     "hint": "late_momentum is the only strategy (others retired 2026-07-17)"},
    # --- Late momentum strategy ---
    {"key": "LATE_MOMENTUM_MIN_MOVE", "label": "Min move $ vs strike", "type": "float",
     "default": "10.0", "group": "late_momentum",
     "hint": "Required lead at 20s left; auto-scales with sqrt(time left)"},
    {"key": "LATE_MOMENTUM_MAX_CHOP", "label": "Max strike crossings (0=off)", "type": "int",
     "default": "3", "group": "late_momentum",
     "hint": "Skip choppy slots where price keeps flipping across the strike"},
    {"key": "LATE_MOMENTUM_TIME_LIMIT", "label": "Window opens (s left)", "type": "int",
     "default": "40", "group": "late_momentum",
     "hint": "Start checking when seconds left drops under this"},
    {"key": "LATE_MOMENTUM_MIN_REMAINING", "label": "Window closes (s left)", "type": "int",
     "default": "8", "group": "late_momentum",
     "hint": "Stop trading below this many seconds"},
    {"key": "LATE_MOMENTUM_BIDIRECTIONAL", "label": "Bidirectional", "type": "bool",
     "default": "1", "group": "late_momentum",
     "hint": "1=bet UP on pump / DOWN on dump, 0=only UP"},
    {"key": "LATE_MOMENTUM_MIN_ENTRY", "label": "Min entry ask", "type": "float",
     "default": "0.55", "group": "late_momentum",
     "hint": "Archive: entries under ~0.92 carried the profit"},
    {"key": "LATE_MOMENTUM_MAX_ENTRY", "label": "Max entry ask", "type": "float",
     "default": "0.88", "group": "late_momentum",
     "hint": "0.80-0.90 is breakeven (n=92) and gives trade flow; 0.90+ loses -$0.12/trade"},
    {"key": "LATE_MOMENTUM_MIN_EDGE", "label": "Edge floor (0 = off)", "type": "float",
     "default": "-0.20", "group": "late_momentum",
     "hint": "Skip trades the model prices as strongly overpriced (edge < -0.20 "
             "lost -$0.14/trade over n=69). Middle edges are fine; 0 = floor off"},
    {"key": "LATE_MOMENTUM_MAX_EDGE", "label": "Edge ceiling (0=off)", "type": "float",
     "default": "0.10", "group": "late_momentum",
     "hint": "Skip when model is FAR above book — stale strike, book usually right"},
    {"key": "LATE_SCALP_REQUIRE_CHAINLINK", "label": "Require Chainlink strike", "type": "bool",
     "default": "1", "group": "late_momentum",
     "hint": "Refuse exchange-proxy strikes (resolution risk)"},
    {"key": "LATE_SCALP_REQUOTE", "label": "Requote before fill", "type": "bool",
     "default": "1", "group": "late_momentum",
     "hint": "Re-check the book before committing; skip on ask jumps"},
    {"key": "LATE_SCALP_MAX_SPREAD", "label": "Max spread", "type": "float",
     "default": "0.10", "group": "late_momentum"},
    {"key": "LATE_SCALP_MAX_STAKE_USD", "label": "Max stake $", "type": "float",
     "default": "5.00", "group": "late_momentum"},
    {"key": "LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", "label": "Pause windows after loss", "type": "int",
     "default": "2", "group": "late_momentum"},
    {"key": "LATE_SCALP_USE_EDGE_SIZING", "label": "Edge-scaled sizing", "type": "bool",
     "default": "1", "group": "late_momentum",
     "hint": "Scale stake by model edge (fractional Kelly)"},
    # --- Maker-first execution ---
    {"key": "MAKER_FIRST_ENABLED", "label": "Maker-first entry", "type": "bool",
     "default": "1", "group": "late_momentum",
     "hint": "Rest under the ask before crossing — makers pay ZERO fee here"},
    {"key": "MAKER_IMPROVE_TICKS", "label": "Ticks under ask", "type": "int",
     "default": "1", "group": "late_momentum",
     "hint": "Deeper = better price but lower fill rate"},
    {"key": "MAKER_WAIT_SECONDS", "label": "Maker wait (s)", "type": "float",
     "default": "3.0", "group": "late_momentum",
     "hint": "How long to wait for the market to come to us before crossing"},
    {"key": "MAKER_FIRST_LIVE", "label": "Maker-first in LIVE", "type": "bool",
     "default": "0", "group": "late_momentum",
     "hint": "Keep 0 until paper shows a solid maker fill rate"},
    # --- Sum-to-one arbitrage ---
    {"key": "ARB_TRADE_ENABLED", "label": "Arb scanner + paper execution", "type": "bool",
     "default": "1", "group": "arb",
     "hint": "Buy BOTH sides when ask_UP+ask_DOWN < 1 − min edge. Paper-only; live logs signals."},
    {"key": "ARB_MIN_EDGE", "label": "Min arb edge (raw)", "type": "float",
     "default": "0.04", "group": "arb",
     "hint": "Required 1 − (ask_UP + ask_DOWN) before slippage"},
    {"key": "ARB_MIN_LOCKED_EDGE", "label": "Min locked edge / pair", "type": "float",
     "default": "0.02", "group": "arb",
     "hint": "Required profit per pair AFTER slippage + taker fees"},
    {"key": "ARB_MAX_EDGE", "label": "Max believable edge (0=off)", "type": "float",
     "default": "0.15", "group": "arb",
     "hint": "Bigger gaps = makers pulling quotes, not fillable arbs — skip"},
    {"key": "TAKER_FEE_RATE", "label": "Taker fee rate", "type": "float",
     "default": "0.07", "group": "risk",
     "hint": "Verified live: rate × min(p,1−p)/share; Gamma per-market rate overrides"},
    {"key": "ARB_MAX_STAKE_USD", "label": "Max arb stake $", "type": "float",
     "default": "10.0", "group": "arb",
     "hint": "Total for both legs of one pair"},
    {"key": "ARB_MIN_REMAINING", "label": "Min seconds left to scan", "type": "int",
     "default": "20", "group": "arb",
     "hint": "Skip arbs too close to slot end"},
    # --- Risk ---
    {"key": "PAPER_FILL_SLIPPAGE", "label": "Paper fill slippage", "type": "float",
     "default": "0.01", "group": "risk",
     "hint": "Added to ask on simulated fills (both arb legs too)"},
    {"key": "FIXED_STAKE_USD", "label": "Fixed stake $", "type": "float",
     "default": "1", "group": "risk"},
    {"key": "BASE_TRADE_USD", "label": "Base trade $", "type": "float",
     "default": "1", "group": "risk"},
    {"key": "MAX_TRADE_USD", "label": "Max trade $", "type": "float",
     "default": "1", "group": "risk"},
    {"key": "MAX_EXPOSURE_USD", "label": "Max exposure $", "type": "float",
     "default": "1", "group": "risk"},
    {"key": "STARTING_BANKROLL", "label": "Starting bankroll $", "type": "float",
     "default": "50", "group": "risk"},
    {"key": "DAILY_LOSS_LIMIT_USD", "label": "Daily loss limit $", "type": "float",
     "default": "5", "group": "risk"},
    {"key": "DAILY_LOSS_LIMIT_PCT", "label": "Daily loss limit % (0=off)", "type": "float",
     "default": "0", "group": "risk"},
    {"key": "MAX_TRADES_PER_DAY", "label": "Max trades / day", "type": "int",
     "default": "50", "group": "risk"},
    {"key": "MAX_CONSECUTIVE_LOSSES", "label": "Max consecutive losses", "type": "int",
     "default": "2", "group": "risk"},
    {"key": "MIN_ORDER_SHARES", "label": "Min order shares", "type": "float",
     "default": "5", "group": "risk"},
    {"key": "ENFORCE_MIN_SHARES", "label": "Enforce min shares", "type": "bool",
     "default": "1", "group": "risk"},
]

_SETTINGS_KEYS = {f["key"] for f in STRATEGY_FIELDS}
_SETTINGS_BY_KEY = {f["key"]: f for f in STRATEGY_FIELDS}


def _settings_payload() -> dict:
    """Current strategy settings from .env + schema for the UI."""
    env = _parse_env_file()
    groups = {
        "late_momentum": "Late Momentum strategy",
        "arb": "Sum-to-one arbitrage",
        "risk": "Risk & size",
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
        elif k == "STRATEGY":
            # Only strategy left; coerce legacy values instead of erroring.
            s = "late_momentum"
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


def _wallet_status() -> dict:
    env = _parse_env_file()
    pk = (env.get("PRIVATE_KEY") or "").strip()
    funder = _norm_addr(env.get("FUNDER_ADDRESS"))
    try:
        sig = int(env.get("SIGNATURE_TYPE") or "0")
    except ValueError:
        sig = 0
    live_confirm = (env.get("LIVE_CONFIRM") or "").strip().lower() in (
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
    pk = (env.get("PRIVATE_KEY") or "").strip()
    funder = _norm_addr(env.get("FUNDER_ADDRESS"))
    if not pk or not funder:
        return {
            "ok": False,
            "balance_usd": None,
            "allowance_usd": None,
            "funder": funder or None,
            "error": "Save private key + funder in Live Wallet Setup first.",
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
                    "error": "Mode must be paper, paper_live, or live."}

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
        strategy = "late_momentum"  # only strategy (others retired)
        asset = str(opts.get("asset") or "btc").strip().lower()
        if asset not in ("btc", "eth", "sol"):
            asset = "btc"
        cmd += ["--asset", asset]

        # Persist asset choice for next load
        try:
            _upsert_env({"ASSET": asset, "STRATEGY": strategy})
        except Exception:
            pass
        cmd += ["--strategy", strategy]
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


_recorders: dict = {}   # timeframe -> {proc, log_file}


def _rec_slot(tf: str | None) -> dict:
    tf = (tf or "5m").strip().lower()
    if tf not in ALLOWED_TIMEFRAMES:
        tf = "5m"
    return _recorders.setdefault(tf, {"proc": None, "log_file": None, "tf": tf})


def _rec_running(tf: str | None) -> bool:
    p = _rec_slot(tf)["proc"]
    return p is not None and p.poll() is None


def _rec_count(tf: str | None) -> int:
    path = os.path.join(_state_dir_for(tf), "archive", "book_recordings.jsonl")
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            return sum(1 for ln in f if ln.strip())
    except Exception:
        return 0


def _rec_start(tf: str | None) -> dict:
    with _lock:
        b = _rec_slot(tf)
        if _rec_running(b["tf"]):
            return {"ok": False, "error": f"{b['tf']} recorder already running"}
        state_dir = _state_dir_for(b["tf"])
        os.makedirs(os.path.join(state_dir, "archive"), exist_ok=True)
        log_path = os.path.join(state_dir, "recorder.log")
        try:
            lf = open(log_path, "w", encoding="utf-8")
            env = os.environ.copy()
            env["TIMEFRAME"] = b["tf"]
            p = subprocess.Popen(
                [sys.executable, "-u", "record_books.py"],
                cwd=HERE, stdout=lf, stderr=subprocess.STDOUT, env=env)
        except Exception as e:
            return {"ok": False, "error": f"failed to launch recorder: {e}"}
        b.update(proc=p, log_file=lf)
        return {"ok": True, "running": True, "pid": p.pid, "timeframe": b["tf"]}


def _rec_stop(tf: str | None) -> dict:
    with _lock:
        b = _rec_slot(tf)
        p = b["proc"]
        if p is None or p.poll() is not None:
            return {"ok": True, "running": False}
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        try:
            if b["log_file"]:
                b["log_file"].close()
        except Exception:
            pass
        b["log_file"] = None
        b["proc"] = None
        return {"ok": True, "running": False}


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


def _state(tf: str | None = None) -> dict:
    b = _slot(tf)
    p = b["proc"]
    return {
        "running": _running(b["timeframe"]),
        "pid": p.pid if p else None,
        "mode": b["mode"],
        "timeframe": b["timeframe"],
        "cmd": b["cmd"],
        "exit_code": (p.poll() if p else None),
        # So the UI can show that another series is still trading.
        "running_timeframes": _running_timeframes(),
    }


# ---------------------------------------------------------------------------
# Embedded dashboard HTML
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Momentum Desk — Polymarket 5m Bot</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ===== DESIGN TOKENS (2026-07 refresh: graphite + violet/mint) ===== */
:root {
  --bg:        #07090f;
  --bg2:       #0b0e18;
  --panel:     rgba(17, 21, 36, 0.82);
  --panel2:    rgba(11, 14, 26, 0.92);
  --glass:     rgba(22, 27, 48, 0.55);
  --glass-border: rgba(255,255,255,0.07);
  --line:      rgba(255,255,255,0.07);
  --line2:     rgba(255,255,255,0.035);
  --dim:       #667089;
  --txt:       #e8ecf4;
  --txt-soft:  #a4adc2;
  --accent:    #8b7bff;
  --accent-dim:rgba(139,123,255,0.16);
  --accent2:   #38e1c4;
  --green:     #2eea9b;
  --green-dim: rgba(46,234,155,0.12);
  --red:       #ff5468;
  --red-dim:   rgba(255,84,104,0.12);
  --amber:     #ffb454;
  --amber-dim: rgba(255,180,84,0.12);
  --radius:    14px;
  --radius-sm: 9px;
}

/* ===== RESET ===== */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { font-size: 15px; }
body {
  background: var(--bg);
  color: var(--txt);
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  min-height: 100vh;
  overflow-x: hidden;
}
body::before {
  content: '';
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background:
    radial-gradient(ellipse 70% 55% at 15% 0%, rgba(139,123,255,0.07), transparent),
    radial-gradient(ellipse 55% 45% at 85% 90%, rgba(56,225,196,0.05), transparent),
    linear-gradient(rgba(255,255,255,0.012) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,0.012) 1px, transparent 1px);
  background-size: auto, auto, 44px 44px, 44px 44px;
  pointer-events: none; z-index: 0;
}

.mono { font-family: 'JetBrains Mono', 'SF Mono', 'Consolas', monospace; font-variant-numeric: tabular-nums; }

/* ===== LAYOUT ===== */
.wrap { position: relative; z-index: 1; max-width: 1440px; margin: 0 auto; padding: 16px 20px 50px; }

/* ===== ANIMATIONS ===== */
@keyframes fadeInUp {
  from { opacity: 0; transform: translateY(12px); }
  to   { opacity: 1; transform: translateY(0); }
}
@keyframes pulse-glow {
  0%, 100% { box-shadow: 0 0 8px var(--green), 0 0 2px var(--green); }
  50%      { box-shadow: 0 0 16px var(--green), 0 0 4px var(--green); }
}
@keyframes pulse-dot {
  0%, 100% { opacity: 1; }
  50%      { opacity: 0.35; }
}
@keyframes gradient-shift {
  0%   { background-position: 0% 50%; }
  50%  { background-position: 100% 50%; }
  100% { background-position: 0% 50%; }
}
@keyframes flash-row {
  0%   { background: rgba(139,123,255,0.28); }
  40%  { background: rgba(46,234,155,0.18); }
  100% { background: transparent; }
}
@keyframes page-flash {
  0%   { box-shadow: inset 0 0 0 0 rgba(46,234,155,0); }
  15%  { box-shadow: inset 0 0 0 3px rgba(46,234,155,0.85), 0 0 40px rgba(46,234,155,0.25); }
  100% { box-shadow: inset 0 0 0 0 rgba(46,234,155,0); }
}
@keyframes page-flash-down {
  0%   { box-shadow: inset 0 0 0 0 rgba(255,84,104,0); }
  15%  { box-shadow: inset 0 0 0 3px rgba(255,84,104,0.85), 0 0 40px rgba(255,84,104,0.25); }
  100% { box-shadow: inset 0 0 0 0 rgba(255,84,104,0); }
}
@keyframes toast-in {
  0%   { opacity: 0; transform: translateY(12px) scale(0.96); }
  12%  { opacity: 1; transform: translateY(0) scale(1); }
  80%  { opacity: 1; transform: translateY(0) scale(1); }
  100% { opacity: 0; transform: translateY(-8px) scale(0.98); }
}
@keyframes shimmer {
  0%   { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}
.animate-in { animation: fadeInUp 0.5s ease-out both; }
.delay-1 { animation-delay: 0.05s; }
.delay-2 { animation-delay: 0.10s; }
.delay-3 { animation-delay: 0.15s; }
.delay-4 { animation-delay: 0.20s; }
.delay-5 { animation-delay: 0.25s; }
.delay-6 { animation-delay: 0.30s; }

/* ===== GLASSMORPHISM CARD ===== */
.glass-card {
  background: var(--glass);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  border: 1px solid var(--glass-border);
  border-radius: var(--radius);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

/* ===== HEADER ===== */
.header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 18px;
  margin-bottom: 14px;
  animation: fadeInUp 0.4s ease-out both;
}
.brand { display: flex; align-items: center; gap: 10px; font-weight: 700; font-size: 14px; letter-spacing: 0.5px; }
.brand-icon { font-size: 18px; filter: drop-shadow(0 0 6px var(--accent)); }
.brand-text { color: var(--txt); }
.brand-text span { color: var(--accent); }
.header-right { display: flex; align-items: center; gap: 10px; }
.conn-dot {
  width: 8px; height: 8px; border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 8px var(--green);
  animation: pulse-dot 1.6s ease-in-out infinite;
  transition: background 0.3s, box-shadow 0.3s;
}
.conn-dot.stale { background: var(--red); box-shadow: 0 0 8px var(--red); }
.badge {
  display: inline-flex; align-items: center; gap: 6px;
  font-size: 11px; font-weight: 500; letter-spacing: 0.4px;
  padding: 4px 10px; border-radius: 6px;
  border: 1px solid var(--glass-border);
  background: var(--panel2);
  color: var(--dim);
}
.badge.mode { color: var(--accent); border-color: rgba(139,123,255,0.2); background: rgba(139,123,255,0.06); }
.badge.conn-badge { }
.badge.clk { color: var(--green); font-family: 'JetBrains Mono', monospace; font-size: 11px; }

/* ===== CONTROL PANEL ===== */
.control-panel {
  padding: 14px 18px;
  margin-bottom: 14px;
  position: relative;
  overflow: hidden;
  animation: fadeInUp 0.4s ease-out 0.05s both;
}
.control-panel::before {
  content: '';
  position: absolute; top: -1px; left: -1px; right: -1px; bottom: -1px;
  border-radius: var(--radius);
  background: linear-gradient(135deg, rgba(139,123,255,0.15), transparent 40%, transparent 60%, rgba(46,234,155,0.1));
  z-index: -1;
  pointer-events: none;
}
.ctl-row { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 10px; }
.ctl-field { display: flex; flex-direction: column; gap: 5px; }
.ctl-label {
  font-size: 9.5px; font-weight: 600; letter-spacing: 0.8px;
  color: var(--dim); text-transform: uppercase;
}
.ctl-input, .ctl-select {
  background: var(--bg);
  color: var(--txt);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 7px 10px;
  font-family: 'Inter', sans-serif;
  font-size: 12.5px;
  min-width: 100px;
  transition: border-color 0.2s, box-shadow 0.2s;
  outline: none;
}
.ctl-input:focus, .ctl-select:focus {
  border-color: var(--accent);
  box-shadow: 0 0 0 2px rgba(139,123,255,0.15);
}
.ctl-check {
  display: flex; flex-direction: row; align-items: center; gap: 6px;
  font-size: 11.5px; color: var(--txt-soft); cursor: pointer;
  align-self: center;
}
.ctl-check input[type="checkbox"] {
  width: 15px; height: 15px; accent-color: var(--accent); cursor: pointer;
}
.btn {
  display: inline-flex; align-items: center; gap: 5px;
  border: none; border-radius: 8px;
  padding: 8px 16px;
  font-family: 'Inter', sans-serif;
  font-size: 12.5px; font-weight: 600;
  cursor: pointer;
  transition: transform 0.15s, box-shadow 0.2s, filter 0.2s;
  outline: none;
}
.btn:active { transform: scale(0.96); }
.btn:disabled { opacity: 0.35; cursor: not-allowed; transform: none; }
.btn-start {
  background: linear-gradient(135deg, #2eea9b, #1fc985);
  color: #042010;
  box-shadow: 0 2px 12px rgba(46,234,155,0.25);
}
.btn-start:hover:not(:disabled) { box-shadow: 0 4px 20px rgba(46,234,155,0.4); filter: brightness(1.1); }
.btn-stop {
  background: linear-gradient(135deg, #ff5468, #e03040);
  color: #fff;
  box-shadow: 0 2px 12px rgba(255,84,104,0.25);
}
.btn-stop:hover:not(:disabled) { box-shadow: 0 4px 20px rgba(255,84,104,0.4); filter: brightness(1.1); }
.ctl-status {
  font-size: 11px; color: var(--dim);
  margin-left: auto; align-self: center;
  font-family: 'JetBrains Mono', monospace;
}
.ctl-status.on { color: var(--green); }
.ctl-msg { font-size: 11px; color: var(--dim); margin-top: 8px; min-height: 16px; }
.ctl-msg.ok  { color: var(--green); }
.ctl-msg.err { color: var(--red); }

/* ===== LIVE STATUS BAR ===== */
.status-bar {
  display: flex; align-items: stretch; gap: 1px;
  border-radius: var(--radius);
  overflow: hidden;
  margin-bottom: 14px;
  position: relative;
  animation: fadeInUp 0.4s ease-out 0.1s both;
}
.status-bar::before {
  content: '';
  position: absolute; inset: 0;
  border-radius: var(--radius);
  padding: 1px;
  background: linear-gradient(90deg, var(--accent), var(--green), var(--amber), var(--accent));
  background-size: 300% 100%;
  animation: gradient-shift 6s linear infinite;
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;
  pointer-events: none; z-index: 1;
}
.sb-item {
  background: var(--panel2);
  padding: 10px 16px;
  display: flex; flex-direction: column; gap: 4px; min-width: 0;
}
.sb-grow { flex: 1; }
.sb-k {
  font-size: 9px; font-weight: 600; letter-spacing: 0.8px;
  color: var(--dim); text-transform: uppercase;
}
.sb-v {
  font-size: 14px; font-weight: 600;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.sb-grow .sb-v { white-space: normal; font-size: 12.5px; font-weight: 500; color: var(--txt-soft); }
.sb-v.big { font-size: 20px; font-family: 'JetBrains Mono', monospace; }

/* phase pills */
.phase {
  display: inline-block; padding: 3px 10px; border-radius: 6px;
  font-size: 11px; font-weight: 700; letter-spacing: 0.6px;
  border: 1px solid var(--glass-border);
}
.ph-analyzing, .ph-idle   { color: var(--green); border-color: rgba(46,234,155,0.25); background: var(--green-dim); }
.ph-holding                { color: var(--amber); border-color: rgba(255,180,84,0.25); background: var(--amber-dim); animation: pulse-dot 2s ease-in-out infinite; }
.ph-resolved               { color: var(--green); border-color: rgba(46,234,155,0.25); background: var(--green-dim); }
.ph-waiting, .ph-searching, .ph-starting, .ph-stopped { color: var(--dim); }
.ph-halted, .ph-error      { color: var(--red);   border-color: rgba(255,84,104,0.25); background: var(--red-dim); }

.count-live { color: var(--amber); }
.count-soon { color: var(--red); text-shadow: 0 0 8px rgba(255,84,104,0.5); }

/* ===== STAT TILES ===== */
/* auto-fit: render() emits 7 tiles — the old fixed 6-col grid orphaned one */
.tiles {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 10px;
  margin-bottom: 14px;
}
.tile {
  padding: 14px 16px;
  position: relative; overflow: hidden;
  transition: transform 0.2s ease, box-shadow 0.25s ease;
}
.tile:hover {
  transform: translateY(-3px);
  box-shadow: 0 8px 24px rgba(0,0,0,0.3);
}
.tile::after {
  content: '';
  position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: linear-gradient(90deg, transparent, var(--accent), transparent);
  opacity: 0;
  transition: opacity 0.3s;
}
.tile:hover::after { opacity: 1; }
.tile .k {
  font-size: 9.5px; font-weight: 600; letter-spacing: 0.8px;
  color: var(--dim); text-transform: uppercase;
}
.tile .v {
  font-size: 22px; font-weight: 700; margin-top: 8px;
  font-family: 'JetBrains Mono', monospace;
}
.tile .s { font-size: 10px; color: var(--dim); margin-top: 5px; }
.pos { color: var(--green); }
.neg { color: var(--red); }
.amb { color: var(--amber); }

/* ===== TWO-COLUMN GRID ===== */
.grid {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 12px;
  margin-bottom: 14px;
}
.panel {
  overflow: hidden;
}
.phead {
  display: flex; justify-content: space-between; align-items: center;
  padding: 10px 16px;
  border-bottom: 1px solid var(--line);
  font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
  color: var(--dim); text-transform: uppercase;
}
.pbody { padding: 14px 16px; }

/* equity curve */
.curve-wrap svg { display: block; }

/* strategy panel */
.strat .lbl {
  font-size: 9.5px; font-weight: 600; color: var(--dim);
  letter-spacing: 0.8px; text-transform: uppercase;
  margin-top: 14px;
}
.strat .lbl:first-child { margin-top: 0; }
.strat p {
  margin: 6px 0 0; font-size: 12.5px; line-height: 1.6;
  color: var(--txt-soft);
}
.strat .up   { color: var(--green); font-weight: 600; }
.strat .down { color: var(--red);   font-weight: 600; }

/* ===== LOG CONSOLE ===== */
.console-panel { animation: fadeInUp 0.4s ease-out 0.25s both; margin-bottom: 14px; }
.console-body {
  background: #040710;
  border: 1px solid rgba(139,123,255,0.05);
  border-radius: var(--radius-sm);
  padding: 12px 14px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: #2eea9b;
  line-height: 1.5;
  height: 150px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-word;
  box-shadow: inset 0 2px 8px rgba(0,0,0,0.8);
}
.console-body::-webkit-scrollbar { width: 6px; }
.console-body::-webkit-scrollbar-track { background: rgba(255,255,255,0.01); }
.console-body::-webkit-scrollbar-thumb { background: rgba(139,123,255,0.15); border-radius: 3px; }
.console-body::-webkit-scrollbar-thumb:hover { background: rgba(139,123,255,0.3); }

/* ===== TRADE BLOTTER ===== */
.blotter { animation: fadeInUp 0.4s ease-out 0.3s both; }
.blotter .slot-link a { color: var(--accent, #6ea8fe); text-decoration: none; }
.blotter .slot-link a:hover { text-decoration: underline; }
table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
thead th {
  color: var(--dim); font-weight: 600; text-align: right;
  padding: 9px 14px; border-bottom: 1px solid var(--line);
  text-transform: uppercase; font-size: 9.5px; letter-spacing: 0.6px;
  position: sticky; top: 0; background: var(--glass);
  backdrop-filter: blur(8px);
}
thead th:first-child, tbody td:first-child { text-align: left; }
tbody td {
  padding: 8px 14px; border-bottom: 1px solid var(--line2);
  text-align: right;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
}
tbody tr { transition: background 0.2s; }
tbody tr:nth-child(even) { background: rgba(255,255,255,0.015); }
tbody tr:hover { background: rgba(139,123,255,0.05); }
.flash-new { animation: flash-row 2.2s ease-out; }
body.trade-flash-up { animation: page-flash 1.4s ease-out; }
body.trade-flash-down { animation: page-flash-down 1.4s ease-out; }
body.trade-flash-win { animation: page-flash 1.6s ease-out; }
body.trade-flash-lose { animation: page-flash-down 1.6s ease-out; }
#toast-stack {
  position: fixed; right: 18px; bottom: 18px; z-index: 9999;
  display: flex; flex-direction: column; gap: 10px;
  pointer-events: none; max-width: min(380px, calc(100vw - 32px));
}
.toast {
  pointer-events: auto;
  background: rgba(10, 16, 28, 0.94);
  border: 1px solid rgba(139,123,255,0.35);
  border-left: 4px solid #8b7bff;
  border-radius: 10px;
  padding: 12px 14px;
  box-shadow: 0 12px 40px rgba(0,0,0,0.45);
  animation: toast-in 4.5s ease forwards;
  font-size: 13px;
  color: #e8eef9;
}
.toast.up { border-left-color: #2eea9b; }
.toast.down { border-left-color: #ff5468; }
.toast.won { border-left-color: #2eea9b; }
.toast.lost { border-left-color: #ff5468; }
.toast .t-title { font-weight: 700; letter-spacing: 0.3px; margin-bottom: 4px; }
.toast .t-body { color: #9aa8c0; font-size: 12px; line-height: 1.4; }
.toast .t-body a { color: #6ea8fe; }
.sb-item.ping-trade .sb-v {
  animation: flash-row 1.8s ease-out;
}

/* ===== LIVE WALLET WIZARD ===== */
.live-wizard {
  margin-top: 12px; padding: 16px 18px; border: 1px solid rgba(255,84,104,0.25);
  background: linear-gradient(180deg, rgba(255,84,104,0.06), rgba(14,20,35,0.4));
}
.live-wizard .lw-head {
  display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;
  margin-bottom: 12px;
}
.live-wizard .lw-title {
  font-weight: 700; font-size: 14px; letter-spacing: 0.3px;
}
.live-wizard .lw-title span { color: var(--red); }
.live-wizard .lw-badge {
  font-size: 11px; font-weight: 700; padding: 4px 10px; border-radius: 999px;
  border: 1px solid var(--line);
}
.live-wizard .lw-badge.ready { color: var(--green); border-color: rgba(46,234,155,0.35); background: var(--green-dim); }
.live-wizard .lw-badge.blocked { color: var(--amber); border-color: rgba(255,180,84,0.35); background: var(--amber-dim); }
.lw-grid {
  display: grid; grid-template-columns: 1.1fr 1fr; gap: 16px;
}
@media (max-width: 980px) { .lw-grid { grid-template-columns: 1fr; } }
.lw-card {
  background: rgba(0,0,0,0.22); border: 1px solid var(--line); border-radius: var(--radius-sm);
  padding: 12px 14px;
}
.lw-card h4 { font-size: 12px; color: var(--txt-soft); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 10px; }
.lw-steps { list-style: none; display: flex; flex-direction: column; gap: 8px; }
.lw-steps li {
  display: flex; gap: 10px; align-items: flex-start; font-size: 12.5px; color: var(--txt-soft); line-height: 1.4;
}
.lw-steps .dot {
  width: 18px; height: 18px; border-radius: 50%; flex: 0 0 18px; margin-top: 1px;
  display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700;
  border: 1px solid var(--line); color: var(--dim);
}
.lw-steps .dot.ok { color: var(--green); border-color: rgba(46,234,155,0.4); background: var(--green-dim); }
.lw-steps .dot.na { color: var(--amber); border-color: rgba(255,180,84,0.35); }
.lw-field { margin-bottom: 10px; }
.lw-field label { display: block; font-size: 11px; color: var(--dim); margin-bottom: 4px; font-weight: 600; }
.lw-field input, .lw-field select, .lw-field textarea {
  width: 100%; background: rgba(0,0,0,0.35); border: 1px solid var(--line);
  color: var(--txt); border-radius: 8px; padding: 8px 10px; font-size: 13px;
  font-family: 'JetBrains Mono', monospace;
}
.lw-field input:focus, .lw-field select:focus { outline: none; border-color: rgba(139,123,255,0.45); }
.lw-row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.lw-help {
  font-size: 12px; color: var(--txt-soft); line-height: 1.45; margin-top: 8px;
  padding: 10px; border-radius: 8px; background: rgba(139,123,255,0.05); border: 1px solid rgba(139,123,255,0.12);
}
.lw-help ol { margin: 6px 0 0 18px; }
.lw-help li { margin: 3px 0; }
.lw-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; align-items: center; }
.btn-live {
  background: linear-gradient(135deg, #ff5468, #c0392b); color: #fff; border: none;
  font-weight: 700; padding: 9px 14px; border-radius: 8px; cursor: pointer;
}
.btn-live:disabled { opacity: 0.45; cursor: not-allowed; }
.btn-save {
  background: var(--accent-dim); color: var(--accent); border: 1px solid rgba(139,123,255,0.35);
  font-weight: 600; padding: 9px 14px; border-radius: 8px; cursor: pointer;
}
.btn-ghost {
  background: transparent; color: var(--txt-soft); border: 1px solid var(--line);
  padding: 9px 12px; border-radius: 8px; cursor: pointer; font-size: 12px;
}
.lw-warn { font-size: 11.5px; color: var(--amber); margin-top: 8px; line-height: 1.4; }
.lw-msg { font-size: 12px; margin-top: 8px; min-height: 1.2em; }
.lw-msg.ok { color: var(--green); }
.lw-msg.err { color: var(--red); }
.lw-ack { display: flex; gap: 8px; align-items: flex-start; font-size: 12px; color: var(--txt-soft); margin-top: 8px; }
.lw-ack input { margin-top: 3px; }
.mode-live-opt { color: #ff8a95; }
.live-gates { display: none; margin-top: 10px; padding-top: 10px; border-top: 1px dashed rgba(255,84,104,0.25); }
.live-gates.on { display: block; }

/* ===== STRATEGY SETTINGS ===== */
.settings-panel {
  margin-top: 12px; padding: 16px 18px;
  border: 1px solid rgba(139,123,255,0.18);
  background: linear-gradient(180deg, rgba(139,123,255,0.05), rgba(14,20,35,0.35));
}
.settings-panel .sp-head {
  display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;
  margin-bottom: 12px;
}
.settings-panel .sp-title { font-weight: 700; font-size: 14px; letter-spacing: 0.3px; }
.settings-panel .sp-title span { color: var(--accent); }
.sp-tabs { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
.sp-tab {
  background: rgba(0,0,0,0.3); border: 1px solid var(--line); color: var(--txt-soft);
  padding: 6px 12px; border-radius: 999px; cursor: pointer; font-size: 12px; font-weight: 600;
}
.sp-tab.on { color: var(--accent); border-color: rgba(139,123,255,0.45); background: var(--accent-dim); }
.sp-group { display: none; }
.sp-group.on { display: block; }
.sp-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(210px, 1fr)); gap: 10px 12px;
}
.sp-field label {
  display: block; font-size: 10.5px; color: var(--dim); margin-bottom: 4px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.3px;
}
.sp-field input, .sp-field select {
  width: 100%; background: rgba(0,0,0,0.35); border: 1px solid var(--line);
  color: var(--txt); border-radius: 8px; padding: 8px 10px; font-size: 13px;
  font-family: 'JetBrains Mono', monospace;
}
.sp-field input:focus, .sp-field select:focus { outline: none; border-color: rgba(139,123,255,0.45); }
.sp-field .hint { font-size: 10.5px; color: var(--dim); margin-top: 3px; line-height: 1.3; min-height: 1.2em; }
.sp-field.bool { display: flex; flex-direction: column; justify-content: flex-end; }
.sp-field.bool .bool-row { display: flex; align-items: center; gap: 8px; padding: 8px 0; }
.sp-field.bool input[type=checkbox] { width: 16px; height: 16px; accent-color: var(--accent); }
.sp-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; align-items: center; }
.sp-msg { font-size: 12px; margin-top: 8px; min-height: 1.2em; }
.sp-msg.ok { color: var(--green); }
.sp-msg.err { color: var(--red); }
.sp-note { font-size: 11.5px; color: var(--amber); margin-top: 6px; }

.up    { color: var(--green); }
.down  { color: var(--red); }
.arb   { color: var(--accent2); font-weight: 600; }
.won   { color: var(--green); }
.lost  { color: var(--red); }

/* analysis panel */
.an-verdict {
  padding: 10px 14px; border-radius: var(--radius-sm); margin-bottom: 12px;
  font-size: 13px; font-weight: 600; border: 1px solid var(--line);
  background: rgba(0,0,0,0.25); color: var(--txt-soft);
}
.an-verdict.good { color: var(--green); border-color: rgba(46,234,155,0.35); background: var(--green-dim); }
.an-verdict.bad  { color: var(--red);   border-color: rgba(255,84,104,0.35); background: var(--red-dim); }
.an-verdict.warn { color: var(--amber); border-color: rgba(255,180,84,0.35); background: var(--amber-dim); }
.an-verdict small { display: block; font-weight: 400; font-size: 11px; margin-top: 3px; color: var(--txt-soft); }
.empty-msg { color: var(--dim); font-size: 12.5px; padding: 28px; text-align: center; font-family: 'Inter', sans-serif; }
.err-line { color: var(--red); font-size: 11px; margin-top: 8px; }

/* ===== RESPONSIVE ===== */
@media (max-width: 1100px) {
  .tiles { grid-template-columns: repeat(3, 1fr); }
  .grid  { grid-template-columns: 1fr; }
}
@media (max-width: 700px) {
  .tiles { grid-template-columns: repeat(2, 1fr); }
  .header { flex-direction: column; gap: 8px; }
  .status-bar { flex-wrap: wrap; }
  .sb-item { flex: 1 1 45%; }
  .ctl-row { flex-direction: column; align-items: stretch; }
  .ctl-status { margin-left: 0; }
}
</style>
</head>
<body>
<div class="wrap">

  <!-- ===== HEADER ===== -->
  <div class="header glass-card">
    <div class="brand">
      <span class="brand-icon">&#9889;</span>
      <span class="brand-text">MOMENTUM <span>DESK</span></span>
      <span class="badge" title="mid / late_scalp / late_every / hybrid retired 2026-07-17">late_momentum + arb</span>
    </div>
    <div class="header-right">
      <span class="badge mode" id="mode">MODE: —</span>
      <div class="conn-dot" id="conn"></div>
      <span class="badge conn-badge" id="conntxt">CONNECTING…</span>
      <span class="badge clk mono" id="clk">--:--:--</span>
    </div>
  </div>

  <!-- ===== CONTROL PANEL ===== -->
  <div class="control-panel glass-card">
    <div class="ctl-row">
      <div class="ctl-field">
        <span class="ctl-label">Mode</span>
        <select id="c-mode" class="ctl-select">
          <option value="paper_live">paper_live · real data, fake fills</option>
          <option value="paper">paper · offline sim</option>
          <option value="live" class="mode-live-opt">live · REAL MONEY</option>
        </select>
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Asset</span>
        <select id="c-asset" class="ctl-select">
          <option value="btc">BTC · Bitcoin</option>
          <option value="eth">ETH · Ethereum</option>
          <option value="sol">SOL · Solana</option>
        </select>
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Timeframe</span>
        <select id="c-timeframe" class="ctl-select">
          <option value="5m">5m · 12 slots/hr · deepest book</option>
          <option value="15m">15m · 4 slots/hr · ~4x thinner</option>
        </select>
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Strategy</span>
        <select id="c-strategy" class="ctl-select">
          <option value="late_momentum">late_momentum · move vs strike, last 40s (+ arb scanner)</option>
        </select>
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Start Balance $</span>
        <input id="c-bankroll" class="ctl-input" type="number" value="50" min="1" step="1">
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Bet / Trade $</span>
        <input id="c-bet" class="ctl-input" type="number" value="1" min="0.1" step="0.1" placeholder="Kelly">
      </div>
      <div class="ctl-field">
        <span class="ctl-label">Rounds</span>
        <input id="c-rounds" class="ctl-input" type="number" value="24" min="1" step="1">
      </div>
      <label class="ctl-check"><input id="c-reset" type="checkbox" checked> Reset data first</label>
      <button id="c-start" class="btn btn-start">&#9654; Start</button>
      <button id="c-stop"  class="btn btn-stop" disabled>&#9632; Stop</button>
      <span id="c-status" class="ctl-status">—</span>
    </div>
    <div id="live-gates" class="live-gates">
      <label class="lw-ack">
        <input type="checkbox" id="c-live-ack">
        <span>I understand this spends <b>real money</b> on Polymarket, there is <b>no profit guarantee</b>, and I can lose the full stake.</span>
      </label>
      <div class="lw-field" style="max-width:220px;margin-top:8px">
        <label>Type LIVE to unlock start</label>
        <input id="c-live-phrase" class="ctl-input" type="text" placeholder="LIVE" autocomplete="off">
      </div>
    </div>
    <div id="c-msg" class="ctl-msg"></div>
  </div>

  <!-- ===== STRATEGY SETTINGS (bands, risk, mid/late) ===== -->
  <div class="settings-panel glass-card" id="settings-panel">
    <div class="sp-head">
      <div class="sp-title">Strategy settings · <span>bands &amp; risk</span></div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <button type="button" class="btn-ghost" id="sp-toggle">Hide</button>
      </div>
    </div>
    <div id="sp-body">
      <div class="sp-tabs" id="sp-tabs">
        <button type="button" class="sp-tab on" data-g="late_momentum">Late Momentum</button>
        <button type="button" class="sp-tab" data-g="arb">Arbitrage</button>
        <button type="button" class="sp-tab" data-g="risk">Risk</button>
        <button type="button" class="sp-tab" data-g="general">General</button>
      </div>
      <div id="sp-groups"></div>
      <div class="sp-actions">
        <button type="button" class="btn-save" id="sp-save">Save settings to .env</button>
        <button type="button" class="btn-ghost" id="sp-reload">Reload from .env</button>
        <button type="button" class="btn-ghost" id="sp-preset-momentum">Preset: momentum · archive-tuned</button>
      </div>
      <div class="sp-msg" id="sp-msg"></div>
      <p class="sp-note" id="sp-note">Changes apply after you <b>Stop</b> and <b>Start</b> the bot (child process reads .env on launch).</p>
    </div>
  </div>

  <!-- ===== LIVE WALLET SETUP ===== -->
  <div class="live-wizard glass-card" id="live-wizard">
    <div class="lw-head">
      <div class="lw-title">Live wallet setup · <span>real money</span></div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span class="lw-badge blocked" id="lw-ready">Checking…</span>
        <button type="button" class="btn-ghost" id="lw-toggle">Hide guide</button>
      </div>
    </div>
    <div id="lw-body">
      <div class="lw-grid">
        <div class="lw-card">
          <h4>Readiness checklist</h4>
          <ul class="lw-steps" id="lw-checklist"></ul>
          <div class="lw-help" id="lw-guide">
            Pick a wallet type on the right to see steps for EOA vs Polymarket proxy.
          </div>
          <p class="lw-warn" id="lw-warnings"></p>
        </div>
        <div class="lw-card">
          <h4>Wallet mapping</h4>
          <div class="lw-field">
            <label>Wallet type (SIGNATURE_TYPE)</label>
            <select id="w-sig">
              <option value="3">3 · Deposit wallet / EIP-1271 (most polymarket.com accounts)</option>
              <option value="1">1 · POLY_PROXY (try if type 3 balance is wrong)</option>
              <option value="0">0 · EOA (funder must equal private-key address)</option>
              <option value="2">2 · Gnosis Safe</option>
            </select>
          </div>
          <div class="lw-field">
            <label>Funder address (holds USDC on Polymarket)</label>
            <input id="w-funder" type="text" spellcheck="false" placeholder="0x… your Polymarket deposit / proxy address">
          </div>
          <div class="lw-field">
            <label>Private key (signer · leave blank to keep existing)</label>
            <input id="w-pk" type="password" spellcheck="false" placeholder="0x… only paste on this local machine" autocomplete="off">
          </div>
          <div class="lw-row">
            <div class="lw-field">
              <label>Fixed stake $</label>
              <input id="w-stake" type="number" min="0.1" step="0.1" value="1">
            </div>
            <div class="lw-field">
              <label>Daily loss limit $</label>
              <input id="w-dailyloss" type="number" min="1" step="1" value="5">
            </div>
          </div>
          <div class="lw-row">
            <div class="lw-field">
              <label>Max trades / day</label>
              <input id="w-maxtrades" type="number" min="1" step="1" value="10">
            </div>
            <div class="lw-field">
              <label>Saved key</label>
              <input id="w-pk-hint" type="text" readonly placeholder="(none)" style="opacity:0.8">
            </div>
          </div>
          <label class="lw-ack">
            <input type="checkbox" id="w-live-confirm">
            <span>Set <b>LIVE_CONFIRM=yes</b> in local .env (required kill-switch)</span>
          </label>
          <div class="lw-field" style="margin-top:4px">
            <label>Live Polymarket USDC (from CLOB)</label>
            <input id="w-balance" type="text" readonly placeholder="Save wallet, then Refresh balance" style="opacity:0.95;color:var(--green)">
          </div>
          <div class="lw-actions">
            <button type="button" class="btn-save" id="w-save">Save to .env</button>
            <button type="button" class="btn-ghost" id="w-refresh">Refresh status</button>
            <button type="button" class="btn-ghost" id="w-bal">Refresh balance</button>
            <button type="button" class="btn-live" id="w-golive" disabled>Use live mode in Start</button>
          </div>
          <div class="lw-msg" id="w-msg"></div>
        </div>
      </div>
    </div>
  </div>
  <div id="toast-stack" aria-live="polite"></div>

  <!-- ===== LIVE STATUS BAR ===== -->
  <div class="status-bar" id="status-bar">
    <div class="sb-item">
      <span class="sb-k">Status</span>
      <span class="sb-v"><span class="phase" id="sb-phase">—</span></span>
    </div>
    <div class="sb-item sb-grow">
      <span class="sb-k">Current Market</span>
      <span class="sb-v" id="sb-market">Waiting for the bot…</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">Resolves In</span>
      <span class="sb-v big mono" id="sb-count">--:--</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">Spot Price</span>
      <span class="sb-v big mono" id="sb-btc">—</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">Signal</span>
      <span class="sb-v" id="sb-signal">—</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">Time Guard</span>
      <span class="sb-v" id="sb-time-suitability">—</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">PM Wallet</span>
      <span class="sb-v big mono" id="sb-wallet">—</span>
    </div>
  </div>

  <!-- ===== STATS TILES ===== -->
  <div class="tiles" id="tiles">
    <div class="tile glass-card animate-in delay-1"><div class="k">Bankroll</div><div class="v mono">—</div><div class="s">—</div></div>
    <div class="tile glass-card animate-in delay-2"><div class="k">Realized P&L</div><div class="v mono">—</div><div class="s">—</div></div>
    <div class="tile glass-card animate-in delay-3"><div class="k">Win Rate</div><div class="v mono">—</div><div class="s">—</div></div>
    <div class="tile glass-card animate-in delay-4"><div class="k">Trades</div><div class="v mono">—</div><div class="s">—</div></div>
    <div class="tile glass-card animate-in delay-5"><div class="k">Max Drawdown</div><div class="v mono neg">—</div><div class="s">—</div></div>
    <div class="tile glass-card animate-in delay-6"><div class="k">Circuit Breaker</div><div class="v mono">—</div><div class="s">—</div></div>
  </div>

  <!-- ===== EQUITY CURVE + STRATEGY ===== -->
  <div class="grid">
    <div class="panel glass-card animate-in delay-3">
      <div class="phead"><span>Equity Curve · Bankroll</span><span id="fills">0 fills</span></div>
      <div class="pbody curve-wrap">
        <svg id="curve" width="100%" height="240" viewBox="0 0 900 240" preserveAspectRatio="none"></svg>
      </div>
    </div>
    <div class="panel glass-card strat animate-in delay-4">
      <div class="phead"><span>Strategy</span><span>Momentum + Arb</span></div>
      <div class="pbody">
        <div class="lbl">Late Momentum</div>
        <p>In the last ~40s of each 5-min slot: if spot moved past the Chainlink strike by the min move, buy <span class="up">UP</span>/<span class="down">DOWN</span> in the 0.55–0.92 ask band. Edge floor <i>and</i> ceiling — a model far above the book usually means a stale strike.</p>
        <div class="lbl">Sum-to-one Arb</div>
        <p>Whenever <span class="arb">ask UP + ask DOWN &lt; $1</span>, buy both sides — one leg pays $1/share either way (paper-only execution; live logs signals).</p>
        <div class="lbl">Risk</div>
        <p>Edge-scaled sizing · exposure caps · loss cooldown · daily-loss circuit breaker. Background settlement keeps the loop watching every slot.</p>
        <div class="lbl">Data Source</div>
        <p id="datanote">Chainlink Data Streams (Polymarket's oracle) + real order books. Paper fills pay ask + slippage.</p>
      </div>
    </div>
  </div>

  <!-- ===== LOG CONSOLE ===== -->
  <div class="panel glass-card console-panel">
    <div class="phead"><span>Bot Console Logs</span><span id="log-sub" class="mono">idle</span></div>
    <div class="pbody">
      <div class="console-body" id="console-logs">No logs found. Click 'Start' above to begin trading.</div>
    </div>
  </div>

  <!-- ===== OPEN POSITIONS (placed, not yet settled) ===== -->
  <div class="panel glass-card" id="open-panel" style="display:none;margin-bottom:14px">
    <div class="phead">
      <span>Open Positions · live</span>
      <span id="open-sub" class="mono">—</span>
    </div>
    <div style="overflow-x:auto"><table id="open-rows"></table></div>
  </div>

  <!-- ===== TRADE BLOTTER ===== -->
  <div class="panel glass-card blotter">
    <div class="phead"><span>Trade Blotter · Newest First</span><span id="blsub">—</span></div>
    <table>
      <thead><tr>
        <th>Time (ET)</th><th>Round</th><th>Side</th><th>Entry</th><th>Stake</th>
        <th>Edge</th><th>Outcome</th><th>W/L</th><th>P&amp;L</th><th>Bankroll</th><th>Slot / Link</th>
      </tr></thead>
      <tbody id="rows"><tr><td class="empty-msg" colspan="11">Waiting for the first trade… start the bot above.</td></tr></tbody>
    </table>
  </div>

  <!-- ===== LEARNING & DATA ===== -->
  <div class="panel glass-card" id="learn-panel" style="margin-top:14px">
    <div class="phead">
      <span>Self-Tuning &amp; Data Export</span>
      <span style="display:flex;gap:8px;align-items:center">
        <button type="button" class="btn-ghost" id="ln-refresh" style="padding:4px 10px;font-size:11px">Refresh</button>
      </span>
    </div>
    <div class="pbody">
      <div id="ln-summary" class="an-verdict">Loading learning state…</div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 4px">
        <a class="btn-save" id="ln-dl" href="/api/export" download
           style="text-decoration:none;display:inline-block">&#11015; Download research data (.zip)</a>
        <a class="btn-ghost" id="ln-dl-full" href="/api/export?full=1" download
           style="text-decoration:none;display:inline-block">&#11015; Download everything (+ books &amp; logs)</a>
      </div>
      <p style="margin-top:6px;font-size:11.5px;color:var(--txt-soft);line-height:1.5">
        The zip holds the labeled observation log, the trade archive, the decision
        journal and every tuning decision. Credentials are never included.
        Analyse a download locally with
        <code>STATE_ROOT=&lt;folder&gt; python analyze.py</code>.
      </p>
      <div style="overflow-x:auto;margin-top:10px"><table id="ln-params"></table></div>
      <div style="overflow-x:auto;margin-top:12px"><table id="ln-journal"></table></div>
    </div>
  </div>

  <!-- ===== PERFORMANCE ANALYSIS (archive) ===== -->
  <div class="panel glass-card" id="analysis-panel" style="margin-top:14px">
    <div class="phead">
      <span>Performance Analysis · archive</span>
      <span style="display:flex;gap:8px;align-items:center">
        <select id="an-range" class="ctl-select" style="min-width:0;padding:4px 8px;font-size:11px">
          <option value="era" selected>Current config (the n=100 sample)</option>
          <option value="">All history (incl. retired strategies)</option>
          <option value="24">Last 24h</option>
          <option value="12">Last 12h</option>
          <option value="6">Last 6h</option>
        </select>
        <button type="button" class="btn-ghost" id="an-refresh" style="padding:4px 10px;font-size:11px">Refresh</button>
      </span>
    </div>
    <div class="pbody">
      <div id="an-verdict" class="an-verdict">Loading analysis…</div>
      <div style="overflow-x:auto"><table id="an-groups"></table></div>
      <div id="an-buckets-wrap" style="display:none;margin-top:12px">
        <div class="lbl" style="font-size:9.5px;font-weight:600;color:var(--dim);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:6px">Momentum by entry price</div>
        <div style="overflow-x:auto"><table id="an-buckets"></table></div>
      </div>
      <p id="an-notes" style="margin-top:10px;font-size:11.5px;color:var(--txt-soft);line-height:1.5"></p>
    </div>
  </div>

  <!-- ===== MARKET-MAKING RESEARCH (paper only) ===== -->
  <div class="panel glass-card" id="mm-panel" style="margin-top:14px">
    <div class="phead">
      <span>Market-Making Research · <span style="color:var(--accent2)">paper, no risk</span></span>
      <span id="mm-recstatus" class="mono">—</span>
    </div>
    <div class="pbody">
      <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:12px">
        <button type="button" class="btn-save" id="mm-rec-toggle">Start recorder</button>
        <span style="font-size:11.5px;color:var(--txt-soft)">records both-side books (no orders, no wallet) toward the backtest</span>
        <span style="margin-left:auto;font-size:11px;color:var(--dim)">edge under ask</span>
        <input id="mm-edge" class="ctl-input" type="number" value="0.02" min="0.01" step="0.01" style="width:80px;padding:6px 8px">
        <span style="font-size:11px;color:var(--dim)">shares</span>
        <input id="mm-size" class="ctl-input" type="number" value="5" min="1" step="1" style="width:70px;padding:6px 8px">
        <button type="button" class="btn-ghost" id="mm-run">Run backtest</button>
      </div>
      <div id="mm-verdict" class="an-verdict">Start the recorder, let it collect slots, then run the backtest.</div>
      <div style="overflow-x:auto"><table id="mm-table"></table></div>
      <p style="margin-top:10px;font-size:11.5px;color:var(--txt-soft);line-height:1.5">
        Rests bids on both UP and DOWN below the ask; a bid fills only when the market trades through it (honest, no queue model). Maker fills pay zero fee. <b>One-sided fills leave you naked-directional</b> — the risk row is the number that matters, not the PnL, until n≥100.
      </p>
    </div>
  </div>

  <div id="err" class="err-line"></div>
</div>

<script>
/* ===== CONFIG ===== */
const REFRESH_MS = 2000;
let lastTopRound = null;
let slotEndTs = null;
let lastStatusTs = 0;
let botBtcPrice = null;
let currentPhase = '';
let serverTimeOffset = 0;
let lastPollTime = 0;
let lastTradeEventId = null;
// MUST be declared here, before everySecond() first runs: it calls
// renderOpenPositions(), and a `let` read before its declaration throws a
// ReferenceError that kills the rest of the script (whole UI dead).
let openPositions = [];

/* ===== HELPERS ===== */
function curTf() {
  const el = document.getElementById('c-timeframe');
  return el ? el.value : '5m';
}
function tfq(sep) { return (sep || '?') + 'tf=' + curTf(); }
function ck(name) { return 'cache_' + name + '_' + curTf(); }
function fmtUsd(n) {
  const sign = n < 0 ? '-' : '';
  return sign + '$' + Math.abs(n).toLocaleString(undefined, {
    minimumFractionDigits: 2, maximumFractionDigits: 2
  });
}

/* ===== CLIENT-SIDE SPOT TICKER (asset-aware — was hardcoded to BTC) ===== */
const PYTH_FEEDS = {
  btc: '0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43',
  eth: '0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0aec',
  sol: '0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d',
};
function currentAsset() {
  const el = document.getElementById('c-asset');
  const a = el ? String(el.value).toLowerCase() : 'btc';
  return PYTH_FEEDS[a] ? a : 'btc';
}
async function fetchClientBtcPrice() {
  try {
    const r = await fetch('https://hermes.pyth.network/v2/updates/price/latest?ids[]=' + PYTH_FEEDS[currentAsset()]);
    if (r.ok) {
      const res = await r.json();
      const parsed = res.parsed[0].price;
      const price = parseFloat(parsed.price) * Math.pow(10, parsed.expo);
      // Update UI directly for live price feel
      const el = document.getElementById('sb-btc');
      if (el) {
        el.textContent = '$' + price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      }
    }
  } catch (e) {
    // Fail silently, fallback to bot's last reported price
    if (botBtcPrice) {
      const el = document.getElementById('sb-btc');
      if (el) el.textContent = '$' + Number(botBtcPrice).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
  }
}
// Poll Pyth price every 2 seconds
setInterval(fetchClientBtcPrice, 2000);
fetchClientBtcPrice();

/* ===== CLOCK + COUNTDOWN ===== */
function everySecond() {
  const d = new Date();
  document.getElementById('clk').textContent = d.toTimeString().slice(0, 8);

  const el = document.getElementById('sb-count');
  if (slotEndTs) {
    let rem = Math.round(slotEndTs - (Date.now() / 1000 + serverTimeOffset));
    if (rem > 0) {
      const m = Math.floor(rem / 60), s = rem % 60;
      el.textContent = (m < 10 ? '0' : '') + m + ':' + (s < 10 ? '0' : '') + s;
      el.className = 'sb-v big mono ' + (rem <= 30 ? 'count-soon' : 'count-live');
    } else {
      el.textContent = 'resolving…';
      el.className = 'sb-v big mono count-soon';
    }
  } else {
    el.textContent = '--:--';
    el.className = 'sb-v big mono';
  }

  renderOpenPositions();  // keep open-position countdowns ticking

  // connection freshness (measure if server is responding to polling)
  const stale = (Date.now() / 1000 - lastPollTime) > 8 || !lastPollTime;
  const dot = document.getElementById('conn');
  const ct  = document.getElementById('conntxt');
  if (stale) {
    dot.classList.add('stale');
    ct.textContent = lastPollTime ? 'STALE' : 'WAITING';
  } else {
    dot.classList.remove('stale');
    ct.textContent = 'LIVE';
  }
}
setInterval(everySecond, 1000);
everySecond();

/* ===== TRADE ALERTS (toast + blink + desktop notification) ===== */
function ensureNotifyPermission() {
  if (!('Notification' in window)) return;
  if (Notification.permission === 'default') {
    try { Notification.requestPermission(); } catch (e) {}
  }
}

function playTradeBeep(kind) {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const o = ctx.createOscillator();
    const g = ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    const up = kind === 'placed' || kind === 'won' || kind === 'up';
    o.type = 'sine';
    o.frequency.value = up ? 880 : 420;
    g.gain.value = 0.0001;
    const t0 = ctx.currentTime;
    g.gain.exponentialRampToValueAtTime(0.08, t0 + 0.02);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.28);
    o.start(t0);
    o.stop(t0 + 0.3);
    if (kind === 'won') {
      const o2 = ctx.createOscillator();
      const g2 = ctx.createGain();
      o2.connect(g2); g2.connect(ctx.destination);
      o2.type = 'sine'; o2.frequency.value = 1175;
      g2.gain.value = 0.0001;
      g2.gain.exponentialRampToValueAtTime(0.07, t0 + 0.12);
      g2.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.4);
      o2.start(t0 + 0.1); o2.stop(t0 + 0.42);
    }
    setTimeout(() => { try { ctx.close(); } catch (e) {} }, 600);
  } catch (e) { /* audio optional */ }
}

function blinkPage(kind) {
  const cls =
    kind === 'won' ? 'trade-flash-win' :
    kind === 'lost' ? 'trade-flash-lose' :
    kind === 'down' ? 'trade-flash-down' : 'trade-flash-up';
  document.body.classList.remove('trade-flash-up', 'trade-flash-down', 'trade-flash-win', 'trade-flash-lose');
  // reflow so animation restarts
  void document.body.offsetWidth;
  document.body.classList.add(cls);
  setTimeout(() => document.body.classList.remove(cls), 1600);
  const bar = document.getElementById('status-bar');
  if (bar) {
    bar.classList.remove('ping-trade');
    void bar.offsetWidth;
    bar.classList.add('ping-trade');
    setTimeout(() => bar.classList.remove('ping-trade'), 2000);
  }
}

function showToast(title, bodyHtml, cls) {
  const stack = document.getElementById('toast-stack');
  if (!stack) return;
  const el = document.createElement('div');
  el.className = 'toast ' + (cls || '');
  el.innerHTML = `<div class="t-title">${title}</div><div class="t-body">${bodyHtml}</div>`;
  stack.appendChild(el);
  setTimeout(() => { try { el.remove(); } catch (e) {} }, 4600);
}

function desktopNotify(title, body) {
  if (!('Notification' in window) || Notification.permission !== 'granted') return;
  try {
    const n = new Notification(title, {
      body: body,
      tag: 'pm-btc-bot-' + String(title).slice(0, 24),
      silent: false,
    });
    setTimeout(() => { try { n.close(); } catch (e) {} }, 6000);
  } catch (e) {}
}

function handleTradeEvent(st) {
  if (!st || !st.trade_event || !st.trade_event_id) return;
  const evId = String(st.trade_event_id);
  // Read per-timeframe so switching series cannot replay the other's event.
  let seen = lastTradeEventId;
  if (!seen) {
    try { seen = localStorage.getItem(ck('last_trade_event_id')); } catch (e) {}
  }
  if (evId === String(seen)) return;
  lastTradeEventId = evId;
  try { localStorage.setItem(ck('last_trade_event_id'), evId); } catch (e) {}
  // status.json keeps the LAST trade event indefinitely, so a fresh browser
  // (empty localStorage) fired a "WON" popup for a trade that settled ages
  // ago — with "@ —" because the main loop had since cleared mkt. Ids look
  // like "resolved-<round>-<unix_ts>": mark old ones seen, but stay silent.
  const evTs = parseInt(evId.split('-').pop(), 10);
  if (Number.isFinite(evTs) && (Date.now() / 1000 - evTs) > 90) return;

  const side = (st.side || '').toUpperCase();
  const mkt = st.mkt != null ? Number(st.mkt).toFixed(2) : '—';
  const stake = st.stake != null ? ('$' + Number(st.stake).toFixed(2)) : '';
  const q = st.question || st.slug || 'BTC 5m';
  const link = st.event_url
    ? ` <a href="${st.event_url}" target="_blank" rel="noopener">open market</a>`
    : '';
  const when = st.time_et || st.time_utc || '';

  if (st.trade_event === 'placed') {
    const title = `TRADE PLACED · ${side || '?'}`;
    const body = `${side} @ ${mkt} ${stake}<br>${q}${when ? '<br>' + when : ''}${link}`;
    showToast(title, body, side === 'DOWN' ? 'down' : 'up');
    blinkPage(side === 'DOWN' ? 'down' : 'up');
    playTradeBeep('placed');
    desktopNotify(title, `${side} @ ${mkt} ${stake} · ${q}`);
  } else if (st.trade_event === 'resolved') {
    const res = (st.last_result || '').toUpperCase();
    const pnl = st.last_pnl != null
      ? ((st.last_pnl >= 0 ? '+' : '') + Number(st.last_pnl).toFixed(2))
      : '';
    const title = res === 'WON' ? `WON · ${side}` : (res === 'LOST' ? `LOST · ${side}` : `RESOLVED · ${side}`);
    const body = `${res} ${pnl} · entry ${mkt}<br>${q}${link}`;
    showToast(title, body, res === 'WON' ? 'won' : (res === 'LOST' ? 'lost' : ''));
    blinkPage(res === 'WON' ? 'won' : (res === 'LOST' ? 'lost' : (side === 'DOWN' ? 'down' : 'up')));
    playTradeBeep(res === 'WON' ? 'won' : 'lost');
    desktopNotify(title, `${res} ${pnl} · ${side} @ ${mkt}`);
  }
}

/* ===== OPEN POSITIONS (placed but not yet settled) ===== */
function renderOpenPositions() {
  const panel = document.getElementById('open-panel');
  const tbl = document.getElementById('open-rows');
  const sub = document.getElementById('open-sub');
  if (!panel || !tbl) return;
  if (!openPositions.length) { panel.style.display = 'none'; return; }
  panel.style.display = '';
  const now = Date.now() / 1000 + serverTimeOffset;
  let atRisk = 0;
  tbl.innerHTML =
    '<thead><tr><th>Placed (ET)</th><th>Round</th><th>Side</th><th>Entry</th>' +
    '<th>Shares</th><th>Stake</th><th>Fill</th><th>Resolves in</th><th>Market</th>' +
    '</tr></thead><tbody>' +
    openPositions.map(p => {
      atRisk += Number(p.stake_usd || 0);
      const cls = p.side === 'ARB' ? 'arb' : (p.side === 'UP' ? 'up' : 'down');
      let rem = p.end_ts ? Math.round(p.end_ts - now) : null;
      let remTxt, remCls = '';
      if (rem === null) { remTxt = '—'; }
      else if (rem > 0) {
        const m = Math.floor(rem / 60), sec = rem % 60;
        remTxt = (m < 10 ? '0' : '') + m + ':' + (sec < 10 ? '0' : '') + sec;
        remCls = rem <= 30 ? 'count-soon' : 'count-live';
      } else { remTxt = 'resolving…'; remCls = 'count-soon'; }
      const label = p.question || p.slug || '—';
      const link = p.event_url
        ? `<a href="${p.event_url}" target="_blank" rel="noopener">${label.length > 38 ? label.slice(0, 36) + '…' : label}</a>`
        : (label.length > 38 ? label.slice(0, 36) + '…' : label);
      return `<tr><td class="mono">${(p.time_et || '').replace(/^.*?(\d{1,2}:\d{2}:\d{2})/, '$1') || '—'}</td>` +
        `<td>R${p.round_id}</td><td class="${cls}">${p.side}</td>` +
        `<td>${Number(p.entry_price).toFixed(3)}</td>` +
        `<td>${Number(p.shares).toFixed(2)}</td>` +
        `<td>${fmtUsd(p.stake_usd)}</td>` +
        `<td>${p.is_maker ? '<span class="won">maker</span>' : 'taker'}</td>` +
        `<td class="mono ${remCls}">${remTxt}</td>` +
        `<td class="slot-link">${link}</td></tr>`;
    }).join('') + '</tbody>';
  sub.textContent = openPositions.length + ' open · ' + fmtUsd(atRisk) + ' at risk';
}

/* ===== STATUS RENDER ===== */
function renderStatus(st) {
  if (!st) return;
  lastStatusTs = st.updated_at || 0;
  const phase = st.phase || '—';
  currentPhase = phase;
  const pe = document.getElementById('sb-phase');
  pe.textContent = phase.toUpperCase();
  pe.className = 'phase ph-' + phase;
  document.getElementById('sb-market').textContent = st.question || st.message || '—';

  botBtcPrice = st.btc_price;
  // If we don't have client price yet, use bot price as fallback
  if (botBtcPrice && document.getElementById('sb-btc').textContent === '—') {
    document.getElementById('sb-btc').textContent = '$' + Number(botBtcPrice).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  const sig = document.getElementById('sb-signal');
  if (st.side) {
    const cls = st.side === 'ARB' ? 'arb' : (st.side === 'UP' ? 'up' : 'down');
    const edge = (st.edge != null) ? (' ' + (st.edge >= 0 ? '+' : '') + Number(st.edge).toFixed(2) + 'e') : '';
    sig.innerHTML = `<span class="${cls}">${st.side}</span>` +
      (st.mkt != null ? ` @ ${Number(st.mkt).toFixed(2)}` : '') +
      `<span class="amb">${edge}</span>`;
  } else if (st.last_result) {
    const cls = st.last_result === 'WON' ? 'won' : 'lost';
    sig.innerHTML = `<span class="${cls}">${st.last_result} ${(st.last_pnl >= 0 ? '+' : '') + Number(st.last_pnl || 0).toFixed(2)}</span>`;
  } else {
    sig.textContent = '—';
  }

  const tsEl = document.getElementById('sb-time-suitability');
  if (tsEl) {
    const suit = st.time_suitability || '—';
    if (suit === '—') {
      tsEl.textContent = '—';
    } else {
      const idx = suit.indexOf(':');
      const action = idx !== -1 ? suit.substring(0, idx).trim() : suit;
      const reason = idx !== -1 ? suit.substring(idx + 1).trim() : '';
      let cls = 'dim';
      if (action === 'GOOD') cls = 'won';
      else if (action === 'AVOID') cls = 'lost';
      else if (action === 'CAUTION') cls = 'amb';
      tsEl.innerHTML = `<span class="${cls}" style="font-weight:bold">${action}</span> <span style="font-size:10px;opacity:0.8">${reason}</span>`;
    }
  }

  openPositions = Array.isArray(st.open_positions) ? st.open_positions : [];
  renderOpenPositions();
  slotEndTs = (st.end_ts && ['analyzing', 'holding', 'waiting', 'resolving'].includes(st.phase)) ? st.end_ts : null;
  handleTradeEvent(st);
  everySecond();
}

/* ===== EQUITY CURVE ===== */
function drawCurve(trades, start) {
  const svg = document.getElementById('curve');
  const W = 900, H = 240, pad = 8;
  let eq = [start];
  trades.forEach(t => eq.push(t.bankroll_after));
  if (eq.length < 2) {
    svg.innerHTML = `<text x="${W/2}" y="${H/2}" fill="#667089" font-family="Inter" font-size="13" text-anchor="middle">Equity data will appear here…</text>`;
    return;
  }
  const min = Math.min(...eq), max = Math.max(...eq), span = (max - min) || 1;
  const x = i => pad + i * (W - 2 * pad) / (eq.length - 1);
  const y = v => pad + (1 - (v - min) / span) * (H - 2 * pad);
  const pts = eq.map((v, i) => x(i) + ',' + y(v)).join(' ');
  const last = eq[eq.length - 1], up = last >= start;
  const col = up ? '#2eea9b' : '#ff5468';
  const colDim = up ? 'rgba(46,234,155,0.10)' : 'rgba(255,84,104,0.10)';
  const area = `${pad},${H - pad} ${pts} ${W - pad},${H - pad}`;
  const baseY = y(start);
  const gradId = 'eq-grad';
  svg.innerHTML =
    `<defs>` +
    `<linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">` +
    `<stop offset="0%" stop-color="${col}" stop-opacity="0.25"/>` +
    `<stop offset="100%" stop-color="${col}" stop-opacity="0.02"/>` +
    `</linearGradient>` +
    `</defs>` +
    `<polygon points="${area}" fill="url(#${gradId})"/>` +
    `<line x1="${pad}" y1="${baseY}" x2="${W - pad}" y2="${baseY}" ` +
    `stroke="#2a3040" stroke-dasharray="6 4" stroke-width="1"/>` +
    `<polyline points="${pts}" fill="none" stroke="${col}" stroke-width="2.5" ` +
    `stroke-linejoin="round" stroke-linecap="round"/>` +
    `<circle cx="${x(eq.length-1)}" cy="${y(last)}" r="4" fill="${col}" ` +
    `stroke="#07090f" stroke-width="2">` +
    `<animate attributeName="r" values="4;6;4" dur="2s" repeatCount="indefinite"/>` +
    `</circle>`;
}

/* ===== MAX DRAWDOWN ===== */
function maxDrawdown(trades, start) {
  let peak = start, mdd = 0;
  trades.forEach(t => {
    peak = Math.max(peak, t.bankroll_after);
    mdd = Math.min(mdd, t.bankroll_after - peak);
  });
  return mdd;
}

/* ===== MAIN RENDER ===== */
function render(stats, trades) {
  if (!stats || typeof stats !== 'object') stats = {};
  if (!Array.isArray(trades)) trades = [];
  document.getElementById('mode').textContent = 'MODE: ' + (stats.mode || '—').toUpperCase();
  if ((stats.mode || '') === 'paper') {
    document.getElementById('datanote').textContent =
      'Offline random-walk price model (not real market data).';
  }

  const start = stats.starting_bankroll || 100;
  const pnl = stats.realized_pnl || 0;
  const mdd = maxDrawdown(trades, start);
  const pnlPct = start ? (100 * pnl / start) : 0;

  const pmBal = (window._pmWallet && window._pmWallet.ok && window._pmWallet.balance_usd != null)
    ? window._pmWallet.balance_usd : null;
  const pmSub = (window._pmWallet && window._pmWallet.ok)
    ? ('Polymarket USDC · ' + ((window._pmWallet.funder || '').slice(0, 6) + '…' + (window._pmWallet.funder || '').slice(-4)))
    : ((window._pmWallet && window._pmWallet.error)
      ? String(window._pmWallet.error).slice(0, 48)
      : 'save wallet · polls CLOB');
  const tilesData = [
    { k: 'PM Wallet USDC',  v: pmBal != null ? fmtUsd(pmBal) : '—',
      s: pmSub, cls: pmBal != null ? 'pos' : '' },
    { k: 'Local Bankroll',  v: fmtUsd(stats.bankroll || start), s: 'bot tracker · start ' + fmtUsd(start) },
    { k: 'Realized P&L',    v: fmtUsd(pnl),                    s: pnlPct.toFixed(1) + '% vs start', cls: pnl >= 0 ? 'pos' : 'neg' },
    // Momentum-only win rate vs its breakeven — the blended number counted
    // structural ARB wins as skill and read ~10pp too good.
    { k: 'Momentum WR',
      v: (stats.momentum_trades ? (stats.momentum_win_rate || 0) + '%' : '—'),
      s: (stats.momentum_trades
          ? ('needs ' + (stats.momentum_breakeven_wr || 0) + '% · n=' + stats.momentum_trades)
          : 'no directional trades yet'),
      cls: (stats.momentum_trades &&
            stats.momentum_win_rate > stats.momentum_breakeven_wr) ? 'pos' : 'neg' },
    { k: 'Momentum / Arb P&L',
      v: fmtUsd(stats.momentum_pnl || 0),
      s: 'arb ' + fmtUsd(stats.arb_pnl || 0) + ' · fees ' + fmtUsd(stats.fees_paid || 0)
         + ' · maker ' + (stats.maker_fill_rate || 0) + '%',
      cls: (stats.momentum_pnl || 0) >= 0 ? 'pos' : 'neg' },
    { k: 'Trades',          v: stats.trades || 0,               s: (stats.wins || 0) + 'W / ' + (stats.losses || 0) + 'L total' },
    { k: 'Max Drawdown',    v: fmtUsd(mdd),                     s: 'peak-to-trough', cls: 'neg' },
    { k: 'Circuit Breaker', v: stats.halted ? 'TRIPPED' : 'ARMED',
      s: stats.halted ? (stats.halt_reason || 'halted') : 'daily loss stop',
      cls: stats.halted ? 'neg' : 'pos' },
  ];
  document.getElementById('tiles').innerHTML = tilesData.map((t, i) =>
    `<div class="tile glass-card">` +
    `<div class="k">${t.k}</div>` +
    `<div class="v mono ${t.cls || ''}">${t.v}</div>` +
    `<div class="s">${t.s}</div></div>`
  ).join('');

  document.getElementById('fills').textContent = trades.length + ' fills';
  document.getElementById('blsub').textContent = (stats.mode || '').toUpperCase() + ' fills';
  drawCurve(trades, start);

  // trade blotter
  const rows = [...trades].reverse();
  const tb = document.getElementById('rows');
  if (!rows.length) {
    tb.innerHTML = '<tr><td class="empty-msg" colspan="11">Waiting for the first trade…</td></tr>';
    return;
  }
  tb.innerHTML = rows.map((t, i) => {
    const sideCls = t.side === 'ARB' ? 'arb' : (t.side === 'UP' ? 'up' : 'down');
    const outCls  = t.outcome === 'UP' ? 'up' : 'down';
    const wl      = t.won ? 'won' : 'lost';
    const flash   = (i === 0 && t.round_id !== lastTopRound) ? 'flash-new' : '';
    // Prefer stored ET string; else format unix ts client-side
    let timeEt = t.time_et || '';
    if (!timeEt && t.ts) {
      try {
        timeEt = new Date(t.ts * 1000).toLocaleString('en-US', {
          timeZone: 'America/New_York', hour12: true,
          year: 'numeric', month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit', second: '2-digit'
        }) + ' ET';
      } catch (e) { timeEt = t.time_utc || String(t.ts); }
    }
    const slotLabel = t.question || t.slug || '—';
    const link = t.event_url
      ? `<a href="${t.event_url}" target="_blank" rel="noopener" title="${slotLabel}">${slotLabel.length > 42 ? slotLabel.slice(0, 40) + '…' : slotLabel}</a>`
      : (slotLabel.length > 42 ? slotLabel.slice(0, 40) + '…' : slotLabel);
    return `<tr class="${flash}">` +
      `<td class="mono" title="${t.time_utc || ''}">${timeEt || '—'}</td>` +
      `<td>R${t.round_id}</td><td class="${sideCls}">${t.side}</td>` +
      `<td>${Number(t.entry_price).toFixed(2)}</td><td>${fmtUsd(t.stake_usd)}</td>` +
      `<td>${(t.edge >= 0 ? '+' : '') + Number(t.edge).toFixed(2)}</td>` +
      `<td class="${outCls}">${t.outcome || '—'}</td>` +
      `<td class="${wl}">${t.won ? 'WON' : 'LOST'}</td>` +
      `<td class="${t.pnl >= 0 ? 'won' : 'lost'}">${(t.pnl >= 0 ? '+' : '') + Number(t.pnl).toFixed(2)}</td>` +
      `<td>${fmtUsd(t.bankroll_after)}</td>` +
      `<td class="slot-link">${link}</td></tr>`;
  }).join('');
  if (rows.length) lastTopRound = rows[0].round_id;
}

/* ===== POLLING ===== */
async function poll() {
  let st = null;
  try {
    st = await fetch('state/status.json' + tfq(), { cache: 'no-store' }).then(r => r.ok ? r.json() : null);
  } catch (e) { /* optional file */ }
  if (st) {
    lastPollTime = Date.now() / 1000; // update connection freshness
    // Only trust the server clock when the snapshot is FRESH. A stale
    // status.json (bot mid-sleep) made this offset equal the staleness
    // itself, which then corrupted every countdown on the page.
    if (st.updated_at && parseFloat(st.updated_at) !== parseFloat(lastStatusTs)) {
      const skew = parseFloat(st.updated_at) - (Date.now() / 1000);
      if (Math.abs(skew) < 15) serverTimeOffset = skew;
    }
    try { localStorage.setItem(ck('status'), JSON.stringify(st)); } catch(e){}
    renderStatus(st);
  }

  try {
    const [s, t] = await Promise.all([
      fetch('state/stats.json' + tfq(),  { cache: 'no-store' }).then(r => r.json()),
      fetch('state/trades.json' + tfq(), { cache: 'no-store' }).then(r => r.json()),
    ]);
    lastPollTime = Date.now() / 1000; // update connection freshness
    try {
      localStorage.setItem(ck('stats'), JSON.stringify(s));
      localStorage.setItem(ck('trades'), JSON.stringify(t));
    } catch(e){}
    document.getElementById('err').textContent = '';
    render(s, t);
  } catch (e) {
    document.getElementById('err').textContent =
      'Waiting for state files (start the bot)… ' + e;
  }

  // Poll real-time logs from server
  try {
    const logs = await fetch('/api/logs' + tfq(), { cache: 'no-store' }).then(r => r.ok ? r.text() : '');
    const logEl = document.getElementById('console-logs');
    if (logEl) {
      // Check if user is scrolled to bottom (with 25px threshold)
      const isAtBottom = logEl.scrollHeight - logEl.clientHeight <= logEl.scrollTop + 25;
      logEl.textContent = logs || "No logs found. Click 'Start' above to begin trading.";
      if (isAtBottom) {
        logEl.scrollTop = logEl.scrollHeight;
      }
    }
    const logSub = document.getElementById('log-sub');
    if (logSub) {
      logSub.textContent = st && st.phase ? st.phase.toUpperCase() : 'ACTIVE';
      logSub.className = 'mono ' + (st && st.phase ? 'pos' : 'dim');
    }
  } catch (e) {
    // Fail silently
  }
}
// Load cached values immediately on page load to prevent blank states and jumping timers
try {
  const cachedStatus = localStorage.getItem(ck('status'));
  const cachedStats  = localStorage.getItem(ck('stats'));
  const cachedTrades = localStorage.getItem(ck('trades'));
  
  if (cachedStatus) {
    const cachedSt = JSON.parse(cachedStatus);
    if (cachedSt) {
      slotEndTs = (cachedSt.end_ts && ['analyzing', 'holding', 'waiting'].includes(cachedSt.phase)) ? cachedSt.end_ts : null;
      renderStatus(cachedSt);
    }
  }
  if (cachedStats && cachedTrades) {
    const cachedS = JSON.parse(cachedStats);
    const cachedT = JSON.parse(cachedTrades);
    if (cachedS && cachedT) {
      render(cachedS, cachedT);
    }
  }
} catch (e) {
  // Ignore localStorage read/parse failures
}

poll();
setInterval(poll, REFRESH_MS);

/* ===== POLYMARKET WALLET BALANCE ===== */
window._pmWallet = null;
async function pollWalletBalance() {
  try {
    const b = await fetch('/api/balance', { cache: 'no-store' }).then(r => r.json());
    window._pmWallet = b;
    const el = document.getElementById('sb-wallet');
    if (el) {
      if (b && b.ok && b.balance_usd != null) {
        el.textContent = '$' + Number(b.balance_usd).toLocaleString(undefined, {
          minimumFractionDigits: 2, maximumFractionDigits: 2
        });
        el.title = 'Funder ' + (b.funder || '') +
          (b.allowance_usd != null ? (' · allowance $' + Number(b.allowance_usd).toFixed(2)) : '');
        el.style.color = 'var(--green)';
      } else {
        el.textContent = '—';
        el.style.color = '';
        el.title = (b && b.error) ? b.error : 'Wallet not configured';
      }
    }
    // Refresh tile strip if stats already rendered
    try {
      const s = JSON.parse(localStorage.getItem(ck('stats')) || 'null');
      const t = JSON.parse(localStorage.getItem(ck('trades')) || 'null');
      if (s && t) render(s, t);
    } catch (e) {}
  } catch (e) {
    const el = document.getElementById('sb-wallet');
    if (el) { el.textContent = '—'; el.title = String(e); }
  }
}
pollWalletBalance();
// 45s — CLOB balance can take 10-30s; 15s polls overlapped and spammed timeouts
setInterval(pollWalletBalance, 45000);

/* ===== CONTROL PANEL ===== */
function setCtl(running, s) {
  const startBtn = document.getElementById('c-start');
  const stopBtn  = document.getElementById('c-stop');
  const stEl     = document.getElementById('c-status');
  if (running === null) {
    startBtn.disabled = true;
    stopBtn.disabled = true;
    stEl.textContent = 'control API unavailable';
    stEl.className = 'ctl-status';
    return;
  }
  startBtn.disabled = running;
  stopBtn.disabled = !running;
  // Status is per timeframe; note any OTHER series still trading so Stop
  // is never ambiguous about which bot it targets.
  const others = ((s && s.running_timeframes) || []).filter(t => t !== curTf());
  const extra = others.length ? (' · also running: ' + others.join(', ')) : '';
  stEl.textContent = (running
    ? (curTf() + ' running · pid ' + ((s && s.pid) || '?'))
    : (curTf() + ' idle')) + extra;
  stEl.className = 'ctl-status ' + (running ? 'on' : '');
}

async function ctlState() {
  try {
    const s = await fetch('/api/control' + tfq(), { cache: 'no-store' }).then(r => r.json());
    setCtl(!!s.running, s);
  } catch (e) {
    setCtl(null);
  }
}

function syncLiveGates() {
  const mode = document.getElementById('c-mode').value;
  const gates = document.getElementById('live-gates');
  if (gates) gates.classList.toggle('on', mode === 'live');
  const startBtn = document.getElementById('c-start');
  if (mode === 'live') {
    startBtn.textContent = '▶ Start LIVE';
    startBtn.style.background = 'linear-gradient(135deg,#ff5468,#c0392b)';
  } else {
    startBtn.textContent = '▶ Start';
    startBtn.style.background = '';
  }
}
document.getElementById('c-mode').addEventListener('change', syncLiveGates);
syncLiveGates();

document.getElementById('c-start').onclick = async () => {
  ensureNotifyPermission();
  const mode = document.getElementById('c-mode').value;
  const body = {
    mode,
    asset:    document.getElementById('c-asset').value,
    timeframe: curTf(),
    strategy: document.getElementById('c-strategy').value,
    bankroll: document.getElementById('c-bankroll').value,
    bet:      document.getElementById('c-bet').value,
    rounds:   document.getElementById('c-rounds').value,
    reset:    document.getElementById('c-reset').checked,
    live_ack: document.getElementById('c-live-ack').checked,
    live_phrase: document.getElementById('c-live-phrase').value,
  };
  if (mode === 'live') {
    if (!body.live_ack) {
      const msg = document.getElementById('c-msg');
      msg.textContent = '⚠ Check the real-money acknowledgement first.';
      msg.className = 'ctl-msg err';
      return;
    }
    if (String(body.live_phrase || '').trim().toUpperCase() !== 'LIVE') {
      const msg = document.getElementById('c-msg');
      msg.textContent = '⚠ Type LIVE in the confirm box to start real-money mode.';
      msg.className = 'ctl-msg err';
      return;
    }
    if (!window.confirm('Start REAL MONEY live mode? Orders will hit Polymarket.')) {
      return;
    }
  }
  const msg = document.getElementById('c-msg');
  msg.textContent = 'starting…';
  msg.className = 'ctl-msg';
  try {
    const r = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(r => r.json());
    if (!r.ok) {
      msg.textContent = '⚠ ' + r.error;
      msg.className = 'ctl-msg err';
    } else {
      if (body.reset) {
        // Reset-start: drop cached renders of the OLD session immediately —
        // otherwise stale trades/bankroll linger until the first new trade
        // writes fresh state files (looked like the reset was broken).
        try {
          localStorage.removeItem(ck('status'));
          localStorage.removeItem(ck('stats'));
          localStorage.removeItem(ck('trades'));
        } catch (e) {}
        slotEndTs = null;
        lastTopRound = null;
        document.getElementById('rows').innerHTML =
          '<tr><td class="empty-msg" colspan="11">Fresh session — waiting for the first trade…</td></tr>';
        render({}, []);
      }
      msg.textContent = (mode === 'live' ? '🔴 LIVE started: ' : '▶ started: ')
        + 'bot.py ' + r.cmd.slice(2).join(' ');
      msg.className = 'ctl-msg ok';
      if ('Notification' in window && Notification.permission === 'denied') {
        msg.textContent += ' · desktop alerts blocked by browser';
      }
    }
  } catch (e) {
    msg.textContent = '⚠ could not reach server — ' + e;
    msg.className = 'ctl-msg err';
  }
  ctlState();
};

document.getElementById('c-stop').onclick = async () => {
  const msg = document.getElementById('c-msg');
  msg.textContent = 'stopping…';
  msg.className = 'ctl-msg';
  try {
    const r = await fetch('/api/stop', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timeframe: curTf() }),
    }).then(r => r.json());
    msg.textContent = r.message || 'stopped';
    msg.className = 'ctl-msg';
  } catch (e) {
    msg.textContent = '⚠ ' + e;
    msg.className = 'ctl-msg err';
  }
  ctlState();
};

/* ===== LIVE WALLET WIZARD ===== */
let walletState = null;

function renderWallet(st) {
  walletState = st;
  if (!st || !st.ok) return;
  const badge = document.getElementById('lw-ready');
  if (st.ready) {
    badge.textContent = 'READY for live';
    badge.className = 'lw-badge ready';
  } else {
    badge.textContent = 'Not ready';
    badge.className = 'lw-badge blocked';
  }
  const ul = document.getElementById('lw-checklist');
  ul.innerHTML = (st.checklist || []).map(c => {
    const cls = c.ok === true ? 'ok' : (c.ok === false ? '' : 'na');
    const mark = c.ok === true ? '✓' : (c.ok === false ? '!' : '?');
    return `<li><span class="dot ${cls}">${mark}</span><div><b style="color:var(--txt)">${c.label}</b><br>${c.hint || ''}</div></li>`;
  }).join('');
  const sig = String(st.signature_type ?? 3);
  const g = (st.guide && st.guide[sig]) || (st.guide && st.guide['3']);
  if (g) {
    document.getElementById('lw-guide').innerHTML =
      `<b>${g.title}</b><div style="margin-top:4px;color:var(--dim)">${g.when || ''}</div>` +
      `<ol>${(g.steps || []).map(s => `<li>${s}</li>`).join('')}</ol>`;
  }
  document.getElementById('lw-warnings').textContent = (st.warnings || []).join(' · ');
  document.getElementById('w-sig').value = sig;
  if (st.funder_address) document.getElementById('w-funder').value = st.funder_address;
  document.getElementById('w-pk-hint').value = st.private_key_hint || '(none saved)';
  document.getElementById('w-stake').value = st.fixed_stake_usd || 1;
  document.getElementById('w-dailyloss').value = st.daily_loss_limit_usd || 5;
  document.getElementById('w-maxtrades').value = st.max_trades_per_day || 10;
  document.getElementById('w-live-confirm').checked = !!st.live_confirm;
  document.getElementById('w-golive').disabled = !st.ready;
}

async function loadWallet() {
  try {
    const st = await fetch('/api/wallet', { cache: 'no-store' }).then(r => r.json());
    renderWallet(st);
  } catch (e) {
    document.getElementById('w-msg').textContent = 'Could not load wallet status: ' + e;
    document.getElementById('w-msg').className = 'lw-msg err';
  }
}

document.getElementById('w-sig').addEventListener('change', () => {
  if (!walletState || !walletState.guide) return;
  const sig = document.getElementById('w-sig').value;
  const g = walletState.guide[sig];
  if (g) {
    document.getElementById('lw-guide').innerHTML =
      `<b>${g.title}</b><div style="margin-top:4px;color:var(--dim)">${g.when || ''}</div>` +
      `<ol>${(g.steps || []).map(s => `<li>${s}</li>`).join('')}</ol>`;
  }
});

document.getElementById('w-save').onclick = async () => {
  const msg = document.getElementById('w-msg');
  msg.textContent = 'saving…';
  msg.className = 'lw-msg';
  const body = {
    funder_address: document.getElementById('w-funder').value,
    signature_type: document.getElementById('w-sig').value,
    private_key: document.getElementById('w-pk').value,
    live_confirm: document.getElementById('w-live-confirm').checked,
    fixed_stake_usd: document.getElementById('w-stake').value,
    daily_loss_limit_usd: document.getElementById('w-dailyloss').value,
    max_trades_per_day: document.getElementById('w-maxtrades').value,
  };
  try {
    const r = await fetch('/api/wallet', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(r => r.json());
    if (!r.ok && r.error) {
      msg.textContent = '⚠ ' + r.error;
      msg.className = 'lw-msg err';
      return;
    }
    document.getElementById('w-pk').value = ''; // never keep key in the form
    renderWallet(r);
    msg.textContent = 'Saved to local .env' + (r.saved ? ' (' + r.saved.join(', ') + ')' : '');
    msg.className = 'lw-msg ok';
  } catch (e) {
    msg.textContent = '⚠ ' + e;
    msg.className = 'lw-msg err';
  }
};

document.getElementById('w-refresh').onclick = () => loadWallet();
document.getElementById('w-bal').onclick = async () => {
  const msg = document.getElementById('w-msg');
  const box = document.getElementById('w-balance');
  msg.textContent = 'fetching Polymarket balance…';
  msg.className = 'lw-msg';
  box.value = '…';
  try {
    await pollWalletBalance();
    const b = window._pmWallet;
    if (b && b.ok && b.balance_usd != null) {
      box.value = '$' + Number(b.balance_usd).toFixed(2) +
        (b.allowance_usd != null ? ('  (allowance $' + Number(b.allowance_usd).toFixed(2) + ')') : '');
      msg.textContent = 'Balance OK · funder ' + (b.funder || '');
      msg.className = 'lw-msg ok';
    } else {
      box.value = '—';
      msg.textContent = '⚠ ' + ((b && b.error) || 'balance unavailable');
      msg.className = 'lw-msg err';
    }
  } catch (e) {
    box.value = '—';
    msg.textContent = '⚠ ' + e;
    msg.className = 'lw-msg err';
  }
};
document.getElementById('w-golive').onclick = () => {
  document.getElementById('c-mode').value = 'live';
  syncLiveGates();
  document.getElementById('c-bet').value = document.getElementById('w-stake').value || 1;
  document.getElementById('c-rounds').value = Math.min(Number(document.getElementById('c-rounds').value) || 5, 10);
  document.getElementById('c-msg').textContent = 'Live mode selected — acknowledge risk, type LIVE, then Start.';
  document.getElementById('c-msg').className = 'ctl-msg ok';
  window.scrollTo({ top: 0, behavior: 'smooth' });
};
document.getElementById('lw-toggle').onclick = () => {
  const body = document.getElementById('lw-body');
  const btn = document.getElementById('lw-toggle');
  const hide = body.style.display !== 'none';
  body.style.display = hide ? 'none' : '';
  btn.textContent = hide ? 'Show guide' : 'Hide guide';
};

/* ===== STRATEGY SETTINGS UI ===== */
let settingsSchema = null;
let settingsActiveGroup = 'late_momentum';

function _spVal(field) {
  const el = document.getElementById('sf-' + field.key);
  if (!el) return field.value;
  if (field.type === 'bool') return el.checked ? '1' : '0';
  return el.value;
}

function renderSettings(data) {
  settingsSchema = data;
  if (!data || !data.ok) return;
  const host = document.getElementById('sp-groups');
  const groups = data.groups || {};
  const byG = {};
  (data.fields || []).forEach(f => {
    (byG[f.group] = byG[f.group] || []).push(f);
  });
  host.innerHTML = Object.keys(groups).map(g => {
    const fields = byG[g] || [];
    const on = g === settingsActiveGroup ? 'on' : '';
    const cells = fields.map(f => {
      const id = 'sf-' + f.key;
      const hint = f.hint ? `<div class="hint">${f.hint}</div>` : '<div class="hint"></div>';
      if (f.type === 'bool') {
        const checked = ['1','true','yes','y','on'].includes(String(f.value).toLowerCase());
        return `<div class="sp-field bool">
          <label>${f.label}</label>
          <div class="bool-row"><input type="checkbox" id="${id}" ${checked ? 'checked' : ''}>
          <span style="font-size:12px;color:var(--txt-soft)">${f.key}</span></div>
          ${hint}</div>`;
      }
      if (f.key === 'STRATEGY') {
        return `<div class="sp-field"><label>${f.label}</label>
          <select id="${id}">
            <option value="late_momentum" selected>late_momentum (only strategy)</option>
          </select>${hint}</div>`;
      }
      if (f.key === 'ASSET') {
        const v = String(f.value || 'btc').toLowerCase();
        return `<div class="sp-field"><label>${f.label}</label>
          <select id="${id}">
            <option value="btc" ${v==='btc'?'selected':''}>btc</option>
            <option value="eth" ${v==='eth'?'selected':''}>eth</option>
            <option value="sol" ${v==='sol'?'selected':''}>sol</option>
          </select>${hint}</div>`;
      }
      const step = f.type === 'int' ? '1' : 'any';
      return `<div class="sp-field"><label>${f.label}</label>
        <input id="${id}" type="number" step="${step}" value="${f.value ?? ''}" data-key="${f.key}">
        ${hint}</div>`;
    }).join('');
    return `<div class="sp-group ${on}" data-g="${g}"><div class="sp-grid">${cells}</div></div>`;
  }).join('');
  document.querySelectorAll('.sp-tab').forEach(t => {
    t.classList.toggle('on', t.dataset.g === settingsActiveGroup);
  });
  if (data.note) document.getElementById('sp-note').textContent = data.note;
  // Sync Start panel asset menu
  const assetField = (data.fields || []).find(f => f.key === 'ASSET');
  let asset = assetField ? String(assetField.value || 'btc').toLowerCase() : 'btc';
  const cAsset = document.getElementById('c-asset');
  if (cAsset && [...cAsset.options].some(o => o.value === asset)) {
    cAsset.value = asset;
  }
}

async function loadSettings() {
  const msg = document.getElementById('sp-msg');
  try {
    const data = await fetch('/api/settings', { cache: 'no-store' }).then(r => r.json());
    renderSettings(data);
    msg.textContent = '';
    msg.className = 'sp-msg';
  } catch (e) {
    msg.textContent = 'Could not load settings: ' + e;
    msg.className = 'sp-msg err';
  }
}

function collectSettings() {
  if (!settingsSchema || !settingsSchema.fields) return {};
  const out = {};
  settingsSchema.fields.forEach(f => { out[f.key] = _spVal(f); });
  return out;
}

async function saveSettings(partial) {
  const msg = document.getElementById('sp-msg');
  msg.textContent = 'saving…';
  msg.className = 'sp-msg';
  const settings = partial || collectSettings();
  try {
    const r = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ settings }),
    }).then(r => r.json());
    if (!r.ok) {
      msg.textContent = '⚠ ' + (r.error || 'save failed');
      msg.className = 'sp-msg err';
      return false;
    }
    renderSettings(r);
    msg.textContent = 'Saved ' + (r.saved ? r.saved.length : '') + ' keys to .env — Stop + Start bot to apply.';
    msg.className = 'sp-msg ok';
    // Sync strategy dropdown if present
    if (settings.STRATEGY && document.getElementById('c-strategy')) {
      document.getElementById('c-strategy').value = settings.STRATEGY;
    }
    // Sync asset dropdown if present
    if (settings.ASSET && document.getElementById('c-asset')) {
      document.getElementById('c-asset').value = settings.ASSET.toLowerCase();
    }
    return true;
  } catch (e) {
    msg.textContent = '⚠ ' + e;
    msg.className = 'sp-msg err';
    return false;
  }
}

document.getElementById('sp-tabs').onclick = (ev) => {
  const t = ev.target.closest('.sp-tab');
  if (!t) return;
  settingsActiveGroup = t.dataset.g;
  document.querySelectorAll('.sp-tab').forEach(x => x.classList.toggle('on', x === t));
  document.querySelectorAll('.sp-group').forEach(g => {
    g.classList.toggle('on', g.dataset.g === settingsActiveGroup);
  });
};
document.getElementById('sp-save').onclick = () => saveSettings();
document.getElementById('sp-reload').onclick = () => loadSettings();
document.getElementById('sp-toggle').onclick = () => {
  const body = document.getElementById('sp-body');
  const btn = document.getElementById('sp-toggle');
  const hide = body.style.display !== 'none';
  body.style.display = hide ? 'none' : '';
  btn.textContent = hide ? 'Show' : 'Hide';
};
// Values the trade-archive bucket analysis supported (2026-07-17):
// entries <= 0.92 carried the profit; huge model edges were anti-predictive.
document.getElementById('sp-preset-momentum').onclick = () => saveSettings({
  STRATEGY: 'late_momentum',
  LATE_MOMENTUM_MIN_MOVE: '10.0',
  LATE_MOMENTUM_TIME_LIMIT: '40',
  LATE_MOMENTUM_MIN_REMAINING: '8',
  LATE_MOMENTUM_BIDIRECTIONAL: '1',
  LATE_MOMENTUM_MIN_ENTRY: '0.55',
  LATE_MOMENTUM_MAX_ENTRY: '0.92',
  LATE_MOMENTUM_MIN_EDGE: '0.0',
  LATE_MOMENTUM_MAX_EDGE: '0.10',
  LATE_SCALP_REQUIRE_CHAINLINK: '1',
  LATE_SCALP_REQUOTE: '1',
  LATE_SCALP_MAX_SPREAD: '0.10',
  LATE_SCALP_MAX_STAKE_USD: '5.0',
  LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS: '2',
  ARB_TRADE_ENABLED: '1',
  ARB_MIN_EDGE: '0.02',
  ARB_MAX_STAKE_USD: '10.0',
  ARB_MIN_REMAINING: '20',
});

/* ===== PERFORMANCE ANALYSIS PANEL ===== */
function anCls(v) { return v > 0 ? 'won' : (v < 0 ? 'lost' : ''); }

function renderAnalysis(rep) {
  const verdict = document.getElementById('an-verdict');
  const groups = document.getElementById('an-groups');
  const bwrap = document.getElementById('an-buckets-wrap');
  const notes = document.getElementById('an-notes');
  if (!rep || !rep.ok) {
    verdict.className = 'an-verdict';
    verdict.textContent = (rep && rep.error) || 'No trades in this range yet.';
    groups.innerHTML = ''; bwrap.style.display = 'none'; notes.textContent = '';
    return;
  }
  // Verdict banner (momentum vs breakeven — the number that decides live)
  const m = rep.momentum;
  if (m) {
    const above = m.above_breakeven;
    verdict.className = 'an-verdict ' + (above ? (m.sample_ok ? 'good' : 'warn')
                                               : 'bad');
    verdict.innerHTML =
      `Momentum: ${m.achieved_wr}% win rate vs ~${m.breakeven_wr}% breakeven ` +
      `→ <b>${above ? 'ABOVE' : 'BELOW'} breakeven</b> (n=${m.n})` +
      `<small>${m.sample_ok
        ? 'Sample size OK — this verdict is meaningful.'
        : 'Small sample — target ' + m.target_n + '+ momentum trades before trusting this (or going live).'}</small>`;
  } else {
    verdict.className = 'an-verdict';
    verdict.textContent = 'No momentum trades in this range (arb only).';
  }
  // Groups table
  const rows = (rep.groups || []).filter(g => g.n > 0);
  groups.innerHTML =
    '<thead><tr><th>Group</th><th>N</th><th>Win %</th><th>PnL $</th>' +
    '<th>Per trade</th><th>Avg entry</th><th>Fees $</th></tr></thead><tbody>' +
    rows.map(g =>
      `<tr><td>${g.label}</td><td>${g.n}</td><td>${g.win_rate}%</td>` +
      `<td class="${anCls(g.pnl)}">${g.pnl >= 0 ? '+' : ''}${g.pnl.toFixed(2)}</td>` +
      `<td class="${anCls(g.expectancy)}">${g.expectancy >= 0 ? '+' : ''}${g.expectancy.toFixed(3)}</td>` +
      `<td>${g.avg_entry.toFixed(3)}</td><td>${(g.fees || 0).toFixed(2)}</td></tr>`
    ).join('') + '</tbody>';
  // Buckets
  const buckets = (m && m.buckets || []).filter(b => b.n > 0);
  if (buckets.length) {
    bwrap.style.display = '';
    document.getElementById('an-buckets').innerHTML =
      '<thead><tr><th>Entry range</th><th>N</th><th>Win %</th><th>PnL $</th>' +
      '<th>Per trade</th></tr></thead><tbody>' +
      buckets.map(b =>
        `<tr><td>${b.label}</td><td>${b.n}</td><td>${b.win_rate}%</td>` +
        `<td class="${anCls(b.pnl)}">${b.pnl >= 0 ? '+' : ''}${b.pnl.toFixed(2)}</td>` +
        `<td class="${anCls(b.expectancy)}">${b.expectancy >= 0 ? '+' : ''}${b.expectancy.toFixed(3)}</td></tr>`
      ).join('') + '</tbody>';
  } else {
    bwrap.style.display = 'none';
  }
  // Notes: outlier + stream health
  const o = rep.outlier;
  let txt = '';
  if (o) {
    txt += `Biggest trade: ${o.side} ${o.pnl >= 0 ? '+' : ''}${o.pnl} ` +
      `(${o.pct_of_total}% of total). Without it: ` +
      `${o.total_without >= 0 ? '+' : ''}${o.total_without} vs ` +
      `${o.total >= 0 ? '+' : ''}${o.total}.`;
    if (o.dominates) txt += ' ⚠ One trade dominates — headline unproven.';
  }
  if (rep.stream_warning) {
    txt += ' ⚠ High proxy_strike skips — Chainlink stream missing slot opens (momentum starved).';
  }
  notes.textContent = txt;
}

async function loadAnalysis() {
  const val = document.getElementById('an-range').value;
  const extra = val === 'era' ? '&era=1' : (val ? ('&hours=' + val) : '');
  try {
    const rep = await fetch('/api/analyze' + tfq() + extra,
                            { cache: 'no-store' }).then(r => r.json());
    renderAnalysis(rep);
  } catch (e) {
    renderAnalysis({ ok: false, error: 'Analysis unavailable: ' + e });
  }
}
document.getElementById('c-timeframe').addEventListener('change', () => {
  // Repaint for the newly selected series: clear the view, then re-poll its
  // own state files (each timeframe has separate trades/stats/logs).
  slotEndTs = null; lastTopRound = null; lastStatusTs = 0;
  document.getElementById('rows').innerHTML =
    '<tr><td class="empty-msg" colspan="11">Loading ' + curTf() + ' data…</td></tr>';
  poll(); loadAnalysis(); ctlState();
});
document.getElementById('an-refresh').onclick = loadAnalysis;
document.getElementById('an-range').addEventListener('change', loadAnalysis);
setInterval(loadAnalysis, 60000);
loadAnalysis();

/* ===== SELF-TUNING & DATA EXPORT PANEL ===== */
function renderLearning(d) {
  const sum = document.getElementById('ln-summary');
  const ptab = document.getElementById('ln-params');
  const jtab = document.getElementById('ln-journal');
  if (!d || !d.ok) {
    sum.textContent = 'Learning state unavailable: ' + ((d && d.error) || '?');
    ptab.innerHTML = ''; jtab.innerHTML = '';
    return;
  }
  const need = d.needed_to_tune || 0;
  const pct = need ? Math.min(100, Math.round(100 * d.labeled / need)) : 0;
  const sup = d.supervisor || {};
  let s = d.labeled >= need
    ? `Dataset ready: ${d.labeled} labeled slots of ${d.observations} recorded. `
    : `Collecting data: ${d.labeled} labeled of ${need} needed to tune (${pct}%). `;
  if (sup.bot_restarts != null) s += `Bot restarts: ${sup.bot_restarts}. `;
  const lt = sup.last_tune;
  if (lt && lt.action) {
    const when = lt.at ? new Date(lt.at * 1000).toISOString().slice(0, 16).replace('T', ' ') : '';
    s += `Last tuning cycle ${when}Z: ${lt.action}.`;
  }
  sum.textContent = s;

  // Which parameters are in force, and whether they came from the tuner.
  const act = d.active_params || {}, def = d.defaults || {};
  let rows = '<thead><tr><th>Parameter</th><th>In force</th><th>Default</th><th></th></tr></thead><tbody>';
  Object.keys(act).forEach(k => {
    const moved = String(act[k]) !== String(def[k]);
    rows += `<tr><td class="mono">${k.replace('LATE_MOMENTUM_', '')}</td>` +
            `<td class="mono">${act[k]}</td><td class="mono">${def[k]}</td>` +
            `<td>${moved ? '<span class="badge">learned</span>' : ''}</td></tr>`;
  });
  ptab.innerHTML = rows + '</tbody>';

  const j = d.journal || [];
  if (!j.length) { jtab.innerHTML = ''; return; }
  let jr = '<thead><tr><th>When (UTC)</th><th>Decision</th><th>Detail</th></tr></thead><tbody>';
  j.slice(0, 8).forEach(e => {
    let detail = '';
    if (e.action === 'apply' && e.changes) {
      detail = Object.keys(e.changes).map(k =>
        `${k.replace('LATE_MOMENTUM_', '')} ${e.changes[k].from}→${e.changes[k].to}`).join(', ');
    } else if (e.action === 'reject') {
      detail = (e.rejected_for || []).join('; ');
    } else if (e.action === 'revert') {
      detail = 'rolled back — realised results were negative and worse';
    } else if (e.action === 'hold') {
      detail = 'no change cleared the guardrails';
    }
    jr += `<tr><td class="mono">${(e.time_utc || '').replace(' UTC', '')}</td>` +
          `<td>${e.action}</td><td style="font-size:11px">${detail}</td></tr>`;
  });
  jtab.innerHTML = jr + '</tbody>';
}

async function loadLearning() {
  try {
    const d = await fetch('/api/learning', { cache: 'no-store' }).then(r => r.json());
    renderLearning(d);
  } catch (e) {
    renderLearning({ ok: false, error: String(e) });
  }
}
document.getElementById('ln-refresh').onclick = loadLearning;
setInterval(loadLearning, 60000);
loadLearning();

/* ===== MARKET-MAKING RESEARCH PANEL ===== */
let mmRecRunning = false;

function renderMM(d) {
  const rs = document.getElementById('mm-recstatus');
  const toggle = document.getElementById('mm-rec-toggle');
  const verdict = document.getElementById('mm-verdict');
  const tbl = document.getElementById('mm-table');
  if (!d || !d.ok) { rs.textContent = 'unavailable'; return; }
  mmRecRunning = !!d.recorder_running;
  rs.textContent = (mmRecRunning ? '● recording' : '○ stopped')
    + ' · ' + (d.recorded_slots || 0) + ' slots recorded';
  rs.style.color = mmRecRunning ? 'var(--green)' : 'var(--dim)';
  toggle.textContent = mmRecRunning ? 'Stop recorder' : 'Start recorder';

  const b = d.backtest || {};
  if (!b.ok) {
    verdict.className = 'an-verdict';
    verdict.textContent = b.error || 'No backtest yet.';
    tbl.innerHTML = '';
    return;
  }
  const enough = b.sample_ok;
  const positive = b.pnl_per_slot > 0;
  verdict.className = 'an-verdict ' + (enough ? (positive ? 'good' : 'bad') : 'warn');
  verdict.innerHTML = enough
    ? `Two-sided quoting <b>${b.verdict}</b> — ${(b.pnl_per_slot>=0?'+':'')}${b.pnl_per_slot}/slot over ${b.usable_slots} slots`
    : `${b.usable_slots} usable slots (of ${b.recorded_slots} recorded) — <b>not a verdict yet</b>, keep recording toward 100+`;
  tbl.innerHTML =
    '<thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>' +
    `<tr><td>Both bids filled (spread capture)</td><td>${b.both_filled_pct}%</td></tr>` +
    `<tr><td>One-sided (naked directional)</td><td class="${b.one_filled_pct>b.both_filled_pct?'lost':''}">${b.one_filled_pct}%</td></tr>` +
    `<tr><td>Neither filled</td><td>${b.none_filled_pct}%</td></tr>` +
    `<tr><td>Total PnL (maker fee = 0)</td><td class="${b.total_pnl>=0?'won':'lost'}">${b.total_pnl>=0?'+':''}${b.total_pnl}</td></tr>` +
    `<tr><td>Return on deployed</td><td class="${b.return_on_deployed_pct>=0?'won':'lost'}">${b.return_on_deployed_pct}%</td></tr>` +
    `<tr><td>Gross directional shares (RISK)</td><td class="amb">${b.gross_directional_shares}</td></tr>` +
    '</tbody>';
}

async function loadMM() {
  const edge = document.getElementById('mm-edge').value || '0.02';
  const size = document.getElementById('mm-size').value || '5';
  try {
    const d = await fetch('/api/mm' + tfq() + '&edge=' + edge + '&size=' + size,
                          { cache: 'no-store' }).then(r => r.json());
    renderMM(d);
  } catch (e) { document.getElementById('mm-recstatus').textContent = 'error: ' + e; }
}
document.getElementById('mm-rec-toggle').onclick = async () => {
  const url = mmRecRunning ? '/api/mm/record/stop' : '/api/mm/record/start';
  try {
    await fetch(url, { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timeframe: curTf() }) });
  } catch (e) {}
  setTimeout(loadMM, 400);
};
document.getElementById('mm-run').onclick = loadMM;
loadMM();
setInterval(loadMM, 20000);

loadSettings();
loadWallet();
ctlState();
setInterval(ctlState, 4000);
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

        # Learning state: dataset size, parameters in force, recent decisions.
        # Without this the self-tuning loop is invisible unless you SSH in.
        if path == "/api/learning":
            try:
                import observations
                import tuner
                rows = observations.load()
                labeled = [r for r in rows if r.get("outcome") in ("UP", "DOWN")]
                journal = tuner.read_journal()[-20:]
                tuned = tuner.load_tuned()
                sup = {}
                try:
                    with open(os.path.join(STATE_DIR, "supervisor.json"),
                              encoding="utf-8") as f:
                        sup = json.load(f)
                except (OSError, ValueError):
                    pass
                return self._json({
                    "ok": True,
                    "observations": len(rows),
                    "labeled": len(labeled),
                    "needed_to_tune": tuner.MIN_LABELED_ROWS,
                    "active_params": tuner.active_params(),
                    "defaults": tuner.DEFAULTS,
                    "tuned": tuned,
                    "journal": list(reversed(journal)),
                    "supervisor": sup,
                })
            except Exception as e:
                return self._json({"ok": False, "error": str(e)}, 500)

        # Control API
        if path == "/api/control":
            from urllib.parse import urlparse, parse_qs
            _tf = (parse_qs(urlparse(self.path).query).get("tf") or ["5m"])[0]
            return self._json(_state(_tf))

        # Market-making research: recorder status + backtest
        if path == "/api/mm":
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            tf = (q.get("tf") or ["5m"])[0]
            out = {"ok": True, "timeframe": tf,
                   "recorder_running": _rec_running(tf),
                   "recorded_slots": _rec_count(tf)}
            try:
                edge = float((q.get("edge") or ["0.02"])[0])
                size = float((q.get("size") or ["5"])[0])
            except (TypeError, ValueError):
                edge, size = 0.02, 5.0
            try:
                import mm_backtest
                out["backtest"] = mm_backtest.compute_backtest(
                    edge=edge, size=size, state_dir=_state_dir_for(tf))
            except Exception as e:
                out["backtest"] = {"ok": False, "error": str(e)}
            return self._json(out)

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
        if route == "/api/mm/record/start":
            _tf = (opts or {}).get("timeframe") if isinstance(opts, dict) else None
            return self._json(_rec_start(_tf))
        if route == "/api/mm/record/stop":
            _tf = (opts or {}).get("timeframe") if isinstance(opts, dict) else None
            return self._json(_rec_stop(_tf))
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
