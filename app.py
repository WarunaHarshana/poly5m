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

import http.server
import json
import os
import re
import subprocess
import sys
import threading
import webbrowser

PORT = int(os.getenv("DASH_PORT", "8000"))
HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, "state")
ENV_PATH = os.path.join(HERE, ".env")
os.chdir(HERE)

ALLOWED_MODES = {"paper", "paper_live", "live"}

_lock = threading.Lock()
_bot = {"proc": None, "cmd": None, "mode": None, "log_file": None}


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
STRATEGY_FIELDS = [
    # --- Strategy / mode-ish ---
    {"key": "STRATEGY", "label": "Default strategy", "type": "str",
     "default": "mid", "group": "general",
     "hint": "mid | late_scalp | late_every | hybrid (Start menu can override)"},
    # --- Late scalp bands ---
    {"key": "LATE_SCALP_EVERY_SLOT", "label": "Trade every 5m slot", "type": "bool",
     "default": "1", "group": "late",
     "hint": "1=relaxed gates, try to bet every slot (more risk)"},
    {"key": "LATE_SCALP_MIN_ENTRY", "label": "Late min entry (favorite ask)", "type": "float",
     "default": "0.60", "group": "late",
     "hint": "Skip softer favorites (0.57 loss) — try 0.60+"},
    {"key": "LATE_SCALP_MAX_ENTRY", "label": "Late max entry (hard cap)", "type": "float",
     "default": "0.99", "group": "late", "hint": "Hard max ask allowed"},
    {"key": "LATE_SCALP_PREFER_MAX", "label": "Late prefer max (sweet spot)", "type": "float",
     "default": "0.99", "group": "late",
     "hint": "Ignored when Trade every 5m slot is ON"},
    {"key": "LATE_SCALP_MIN_REMAINING", "label": "Late min seconds left", "type": "int",
     "default": "8", "group": "late", "hint": "Stop trading below this many seconds"},
    {"key": "LATE_SCALP_MAX_REMAINING", "label": "Late max seconds left", "type": "int",
     "default": "50", "group": "late", "hint": "Start late window at this many seconds"},
    {"key": "LATE_SCALP_MIN_FAVORITE_LEAD", "label": "Min favorite lead", "type": "float",
     "default": "0.02", "group": "late", "hint": "fav_ask - dog_ask must be >= this"},
    {"key": "LATE_SCALP_MAX_UNDERDOG_ASK", "label": "Max underdog ask", "type": "float",
     "default": "0.49", "group": "late", "hint": "Other side max (relaxed for every-slot)"},
    {"key": "LATE_SCALP_REQUIRE_SPOT_ALIGN", "label": "Require spot aligns with favorite", "type": "bool",
     "default": "0", "group": "late", "hint": "0 recommended for every-slot mode"},
    {"key": "LATE_SCALP_SPOT_ALIGN_EPS_USD", "label": "Spot align epsilon $", "type": "float",
     "default": "8", "group": "late", "hint": "Allow this $ noise vs strike"},
    {"key": "LATE_SCALP_REQUOTE", "label": "Late requote before fill", "type": "bool",
     "default": "0", "group": "late"},
    {"key": "LATE_SCALP_KEEP_FIRST_ASK", "label": "Keep first ask if requote spikes", "type": "bool",
     "default": "1", "group": "late", "hint": "1=limit at first ask instead of chasing"},
    {"key": "LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", "label": "Pause late windows after loss", "type": "int",
     "default": "0", "group": "late"},
    {"key": "LATE_SCALP_MAX_STAKE_USD", "label": "Late max stake $", "type": "float",
     "default": "5.00", "group": "late"},
    {"key": "LATE_SCALP_MAX_SPREAD", "label": "Late max spread", "type": "float",
     "default": "0.15", "group": "late"},
    {"key": "LATE_SCALP_MIN_MOVE_USD", "label": "Late min move $", "type": "float",
     "default": "15.0", "group": "late", "hint": "BTC price must move this much from strike"},
    {"key": "LATE_SCALP_REQUIRE_CHAINLINK", "label": "Late require Chainlink", "type": "bool",
     "default": "1", "group": "late", "hint": "Refuse trading on exchange-proxy strikes"},
    {"key": "LATE_SCALP_MIN_EDGE", "label": "Late min edge", "type": "float",
     "default": "0.02", "group": "late", "hint": "Min model probability edge over ask"},
    # --- Mid ---
    {"key": "MIN_ENTRY_PRICE", "label": "Mid min entry", "type": "float",
     "default": "0.22", "group": "mid", "hint": "Do not buy cheaper than this"},
    {"key": "MAX_ENTRY_PRICE", "label": "Mid max entry", "type": "float",
     "default": "0.72", "group": "mid", "hint": "Do not buy richer than this"},
    {"key": "EDGE_THRESHOLD", "label": "Min edge", "type": "float",
     "default": "0.04", "group": "mid"},
    {"key": "MIN_EV", "label": "Min EV", "type": "float",
     "default": "0.02", "group": "mid"},
    {"key": "MIN_MOVE_USD", "label": "Min move $ vs strike", "type": "float",
     "default": "5", "group": "mid"},
    {"key": "MIN_CONFIDENCE", "label": "Min confidence", "type": "float",
     "default": "0.40", "group": "mid"},
    {"key": "MAX_SPREAD", "label": "Max spread (mid)", "type": "float",
     "default": "0.08", "group": "mid"},
    {"key": "FEE_BUFFER", "label": "Fee buffer", "type": "float",
     "default": "0.01", "group": "mid"},
    {"key": "MIN_SLOT_SECONDS", "label": "Min seconds left (mid window)", "type": "int",
     "default": "60", "group": "mid"},
    {"key": "MAX_TRADE_REMAINING", "label": "Max seconds left (mid window)", "type": "int",
     "default": "240", "group": "mid"},
    {"key": "MID_MIN_MODEL", "label": "Mid min model prob", "type": "float",
     "default": "0.52", "group": "mid"},
    {"key": "MID_MAX_MODEL", "label": "Mid max model prob", "type": "float",
     "default": "0.95", "group": "mid"},
    {"key": "MID_NOISE_MOVE_CAP", "label": "Noise move cap $ (0=off)", "type": "float",
     "default": "0", "group": "mid", "hint": "0 = only MIN_MOVE_USD applies"},
    {"key": "MID_Z_MIN", "label": "Fair-value Z min", "type": "float",
     "default": "0.15", "group": "mid"},
    {"key": "MID_USE_FAIR_VALUE", "label": "Use fair-value model", "type": "bool",
     "default": "1", "group": "mid"},
    {"key": "MID_REQUIRE_SIGNIFICANT", "label": "Require significant Z", "type": "bool",
     "default": "0", "group": "mid"},
    {"key": "MID_REQUIRE_CHAINLINK_STRIKE", "label": "Require Chainlink strike", "type": "bool",
     "default": "0", "group": "mid", "hint": "1=block exchange-proxy strikes"},
    {"key": "MID_MAX_CHOP_CROSSES", "label": "Max chop crosses", "type": "int",
     "default": "6", "group": "mid"},
    {"key": "MID_REQUOTE", "label": "Mid requote", "type": "bool",
     "default": "1", "group": "mid"},
    {"key": "MID_PAUSE_SLOTS_AFTER_LOSS", "label": "Pause mid slots after loss", "type": "int",
     "default": "1", "group": "mid"},
    {"key": "MID_MAX_STAKE_USD", "label": "Mid max stake $", "type": "float",
     "default": "4.00", "group": "mid"},
    {"key": "PAPER_FILL_SLIPPAGE", "label": "Paper fill slippage", "type": "float",
     "default": "0.01", "group": "mid"},
    # --- Hybrid ---
    {"key": "HYBRID_PREFER_LATE", "label": "Hybrid prefer late", "type": "bool",
     "default": "1", "group": "hybrid"},
    {"key": "HYBRID_MID_MIN_EDGE", "label": "Hybrid mid min edge", "type": "float",
     "default": "0.10", "group": "hybrid"},
    {"key": "HYBRID_MID_MIN_MOVE_USD", "label": "Hybrid mid min move $", "type": "float",
     "default": "15", "group": "hybrid"},
    {"key": "HYBRID_MID_MIN_MODEL", "label": "Hybrid mid min model", "type": "float",
     "default": "0.65", "group": "hybrid"},
    {"key": "HYBRID_MID_MIN_CONFIDENCE", "label": "Hybrid mid min conf", "type": "float",
     "default": "0.55", "group": "hybrid"},
    # --- Risk ---
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
        "general": "General",
        "late": "Late scalp bands & guards",
        "mid": "Mid strategy",
        "hybrid": "Hybrid",
        "risk": "Risk & size",
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
        elif k == "STRATEGY":
            s = s.lower()
            if s not in ("mid", "late_scalp", "late_every", "hybrid"):
                errors.append(
                    "STRATEGY must be mid, late_scalp, late_every, or hybrid")
                continue
            # Keep LATE_SCALP_EVERY_SLOT in sync with strategy menu choice
            if s == "late_every":
                updates["LATE_SCALP_EVERY_SLOT"] = "1"
            elif s == "late_scalp":
                updates["LATE_SCALP_EVERY_SLOT"] = "0"
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
def _running() -> bool:
    p = _bot["proc"]
    return p is not None and p.poll() is None


def _get_logs() -> str:
    log_path = os.path.join(STATE_DIR, "bot.log")
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
        if _running():
            return {"ok": False, "error": "A run is already in progress \u2014 stop it first."}
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
        strategy = str(opts.get("strategy") or "mid").strip().lower()
        if strategy not in ("mid", "late_scalp", "late_every", "hybrid"):
            strategy = "mid"
        # Persist menu choice + every-slot flag for next load
        try:
            if strategy == "late_every":
                _upsert_env({"STRATEGY": "late_every", "LATE_SCALP_EVERY_SLOT": "1"})
            elif strategy == "late_scalp":
                _upsert_env({"STRATEGY": "late_scalp", "LATE_SCALP_EVERY_SLOT": "0"})
            else:
                _upsert_env({"STRATEGY": strategy})
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

        # Clear/initialize log file
        log_path = os.path.join(STATE_DIR, "bot.log")
        try:
            if _bot["log_file"]:
                _bot["log_file"].close()
        except Exception:
            pass

        try:
            log_file = open(log_path, "w", encoding="utf-8")
        except Exception as e:
            return {"ok": False, "error": f"Failed to open log file: {e}"}

        try:
            # Fresh env for child so .env reloads with python-dotenv in config
            proc = subprocess.Popen(
                cmd, cwd=HERE, stdout=log_file, stderr=subprocess.STDOUT,
                env=os.environ.copy(),
            )
        except Exception as e:
            log_file.close()
            return {"ok": False, "error": f"Failed to launch bot: {e}"}

        _bot.update(proc=proc, cmd=cmd, mode=mode, log_file=log_file)
        return {"ok": True, "running": True, "pid": proc.pid, "cmd": cmd,
                "mode": mode}


def _stop() -> dict:
    with _lock:
        p = _bot["proc"]
        if p is None or p.poll() is not None:
            return {"ok": True, "running": False, "message": "Nothing was running."}
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        
        # Close the log file handle
        try:
            if _bot["log_file"]:
                _bot["log_file"].close()
        except Exception:
            pass
        _bot["log_file"] = None
        
        return {"ok": True, "running": False, "message": "Stopped the bot."}


def _state() -> dict:
    p = _bot["proc"]
    return {
        "running": _running(),
        "pid": p.pid if p else None,
        "mode": _bot["mode"],
        "cmd": _bot["cmd"],
        "exit_code": (p.poll() if p else None),
    }


# ---------------------------------------------------------------------------
# Embedded dashboard HTML
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polymarket BTC Bot — Live Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
/* ===== DESIGN TOKENS ===== */
:root {
  --bg:        #0a0e17;
  --bg2:       #0d1220;
  --panel:     rgba(14, 20, 35, 0.75);
  --panel2:    rgba(10, 15, 28, 0.9);
  --glass:     rgba(20, 28, 50, 0.45);
  --glass-border: rgba(255,255,255,0.06);
  --line:      rgba(255,255,255,0.06);
  --line2:     rgba(255,255,255,0.03);
  --dim:       #5a6478;
  --txt:       #e2e6ed;
  --txt-soft:  #9aa3b4;
  --accent:    #00d4ff;
  --accent-dim:rgba(0,212,255,0.15);
  --green:     #00ff88;
  --green-dim: rgba(0,255,136,0.12);
  --red:       #ff4757;
  --red-dim:   rgba(255,71,87,0.12);
  --amber:     #ffa502;
  --amber-dim: rgba(255,165,2,0.12);
  --radius:    12px;
  --radius-sm: 8px;
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
    radial-gradient(ellipse 80% 60% at 20% 10%, rgba(0,212,255,0.04), transparent),
    radial-gradient(ellipse 60% 50% at 80% 80%, rgba(0,255,136,0.03), transparent);
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
  0%   { background: rgba(0,212,255,0.28); }
  40%  { background: rgba(0,255,136,0.18); }
  100% { background: transparent; }
}
@keyframes page-flash {
  0%   { box-shadow: inset 0 0 0 0 rgba(0,255,136,0); }
  15%  { box-shadow: inset 0 0 0 3px rgba(0,255,136,0.85), 0 0 40px rgba(0,255,136,0.25); }
  100% { box-shadow: inset 0 0 0 0 rgba(0,255,136,0); }
}
@keyframes page-flash-down {
  0%   { box-shadow: inset 0 0 0 0 rgba(255,71,87,0); }
  15%  { box-shadow: inset 0 0 0 3px rgba(255,71,87,0.85), 0 0 40px rgba(255,71,87,0.25); }
  100% { box-shadow: inset 0 0 0 0 rgba(255,71,87,0); }
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
.badge.mode { color: var(--accent); border-color: rgba(0,212,255,0.2); background: rgba(0,212,255,0.06); }
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
  background: linear-gradient(135deg, rgba(0,212,255,0.15), transparent 40%, transparent 60%, rgba(0,255,136,0.1));
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
  box-shadow: 0 0 0 2px rgba(0,212,255,0.15);
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
  background: linear-gradient(135deg, #00ff88, #00cc6a);
  color: #042010;
  box-shadow: 0 2px 12px rgba(0,255,136,0.25);
}
.btn-start:hover:not(:disabled) { box-shadow: 0 4px 20px rgba(0,255,136,0.4); filter: brightness(1.1); }
.btn-stop {
  background: linear-gradient(135deg, #ff4757, #e03040);
  color: #fff;
  box-shadow: 0 2px 12px rgba(255,71,87,0.25);
}
.btn-stop:hover:not(:disabled) { box-shadow: 0 4px 20px rgba(255,71,87,0.4); filter: brightness(1.1); }
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
.ph-analyzing, .ph-idle   { color: var(--green); border-color: rgba(0,255,136,0.25); background: var(--green-dim); }
.ph-holding                { color: var(--amber); border-color: rgba(255,165,2,0.25); background: var(--amber-dim); animation: pulse-dot 2s ease-in-out infinite; }
.ph-resolved               { color: var(--green); border-color: rgba(0,255,136,0.25); background: var(--green-dim); }
.ph-waiting, .ph-searching, .ph-starting, .ph-stopped { color: var(--dim); }
.ph-halted, .ph-error      { color: var(--red);   border-color: rgba(255,71,87,0.25); background: var(--red-dim); }

.count-live { color: var(--amber); }
.count-soon { color: var(--red); text-shadow: 0 0 8px rgba(255,71,87,0.5); }

/* ===== STAT TILES ===== */
.tiles {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
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
  border: 1px solid rgba(0,212,255,0.05);
  border-radius: var(--radius-sm);
  padding: 12px 14px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: #00ff88;
  line-height: 1.5;
  height: 150px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-word;
  box-shadow: inset 0 2px 8px rgba(0,0,0,0.8);
}
.console-body::-webkit-scrollbar { width: 6px; }
.console-body::-webkit-scrollbar-track { background: rgba(255,255,255,0.01); }
.console-body::-webkit-scrollbar-thumb { background: rgba(0,212,255,0.15); border-radius: 3px; }
.console-body::-webkit-scrollbar-thumb:hover { background: rgba(0,212,255,0.3); }

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
tbody tr:hover { background: rgba(0,212,255,0.05); }
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
  border: 1px solid rgba(0,212,255,0.35);
  border-left: 4px solid #00d4ff;
  border-radius: 10px;
  padding: 12px 14px;
  box-shadow: 0 12px 40px rgba(0,0,0,0.45);
  animation: toast-in 4.5s ease forwards;
  font-size: 13px;
  color: #e8eef9;
}
.toast.up { border-left-color: #00ff88; }
.toast.down { border-left-color: #ff4757; }
.toast.won { border-left-color: #00ff88; }
.toast.lost { border-left-color: #ff4757; }
.toast .t-title { font-weight: 700; letter-spacing: 0.3px; margin-bottom: 4px; }
.toast .t-body { color: #9aa8c0; font-size: 12px; line-height: 1.4; }
.toast .t-body a { color: #6ea8fe; }
.sb-item.ping-trade .sb-v {
  animation: flash-row 1.8s ease-out;
}

/* ===== LIVE WALLET WIZARD ===== */
.live-wizard {
  margin-top: 12px; padding: 16px 18px; border: 1px solid rgba(255,71,87,0.25);
  background: linear-gradient(180deg, rgba(255,71,87,0.06), rgba(14,20,35,0.4));
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
.live-wizard .lw-badge.ready { color: var(--green); border-color: rgba(0,255,136,0.35); background: var(--green-dim); }
.live-wizard .lw-badge.blocked { color: var(--amber); border-color: rgba(255,165,2,0.35); background: var(--amber-dim); }
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
.lw-steps .dot.ok { color: var(--green); border-color: rgba(0,255,136,0.4); background: var(--green-dim); }
.lw-steps .dot.na { color: var(--amber); border-color: rgba(255,165,2,0.35); }
.lw-field { margin-bottom: 10px; }
.lw-field label { display: block; font-size: 11px; color: var(--dim); margin-bottom: 4px; font-weight: 600; }
.lw-field input, .lw-field select, .lw-field textarea {
  width: 100%; background: rgba(0,0,0,0.35); border: 1px solid var(--line);
  color: var(--txt); border-radius: 8px; padding: 8px 10px; font-size: 13px;
  font-family: 'JetBrains Mono', monospace;
}
.lw-field input:focus, .lw-field select:focus { outline: none; border-color: rgba(0,212,255,0.45); }
.lw-row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.lw-help {
  font-size: 12px; color: var(--txt-soft); line-height: 1.45; margin-top: 8px;
  padding: 10px; border-radius: 8px; background: rgba(0,212,255,0.05); border: 1px solid rgba(0,212,255,0.12);
}
.lw-help ol { margin: 6px 0 0 18px; }
.lw-help li { margin: 3px 0; }
.lw-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; align-items: center; }
.btn-live {
  background: linear-gradient(135deg, #ff4757, #c0392b); color: #fff; border: none;
  font-weight: 700; padding: 9px 14px; border-radius: 8px; cursor: pointer;
}
.btn-live:disabled { opacity: 0.45; cursor: not-allowed; }
.btn-save {
  background: var(--accent-dim); color: var(--accent); border: 1px solid rgba(0,212,255,0.35);
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
.live-gates { display: none; margin-top: 10px; padding-top: 10px; border-top: 1px dashed rgba(255,71,87,0.25); }
.live-gates.on { display: block; }

/* ===== STRATEGY SETTINGS ===== */
.settings-panel {
  margin-top: 12px; padding: 16px 18px;
  border: 1px solid rgba(0,212,255,0.18);
  background: linear-gradient(180deg, rgba(0,212,255,0.05), rgba(14,20,35,0.35));
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
.sp-tab.on { color: var(--accent); border-color: rgba(0,212,255,0.45); background: var(--accent-dim); }
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
.sp-field input:focus, .sp-field select:focus { outline: none; border-color: rgba(0,212,255,0.45); }
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
.won   { color: var(--green); }
.lost  { color: var(--red); }
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
      <span class="brand-text">POLYMARKET <span>BTC BOT</span></span>
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
        <span class="ctl-label">Strategy</span>
        <select id="c-strategy" class="ctl-select">
          <option value="mid">mid · edge / fair-value mid-slot</option>
          <option value="late_scalp">late_scalp · selective favorites (~0.90+)</option>
          <option value="late_every">late_every · bet almost every 5m slot</option>
          <option value="hybrid">hybrid · late preferred, strict mid if exceptional</option>
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
        <button type="button" class="sp-tab on" data-g="late">Late scalp</button>
        <button type="button" class="sp-tab" data-g="mid">Mid</button>
        <button type="button" class="sp-tab" data-g="hybrid">Hybrid</button>
        <button type="button" class="sp-tab" data-g="risk">Risk</button>
        <button type="button" class="sp-tab" data-g="general">General</button>
      </div>
      <div id="sp-groups"></div>
      <div class="sp-actions">
        <button type="button" class="btn-save" id="sp-save">Save settings to .env</button>
        <button type="button" class="btn-ghost" id="sp-reload">Reload from .env</button>
        <button type="button" class="btn-ghost" id="sp-preset-late-every">Preset: late EVERY 5m slot</button>
        <button type="button" class="btn-ghost" id="sp-preset-late90">Preset: late 0.90–0.95 profit</button>
        <button type="button" class="btn-ghost" id="sp-preset-late-strict">Preset: late strict 0.93–0.96</button>
        <button type="button" class="btn-ghost" id="sp-preset-mid-active">Preset: mid paper-active</button>
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
      <span class="sb-k">BTC Spot</span>
      <span class="sb-v big mono" id="sb-btc">—</span>
    </div>
    <div class="sb-item">
      <span class="sb-k">Signal</span>
      <span class="sb-v" id="sb-signal">—</span>
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
      <div class="phead"><span>Strategy</span><span>Momentum · Kelly</span></div>
      <div class="pbody">
        <div class="lbl">Signal</div>
        <p>Predict BTC <span class="up">UP</span>/<span class="down">DOWN</span> over the next 5 min from short-term momentum; trade only when model edge &gt; threshold.</p>
        <div class="lbl">Risk</div>
        <p>Fractional Kelly sizing · exposure cap · daily-loss circuit breaker.</p>
        <div class="lbl">Data Source</div>
        <p id="datanote">Real BTC spot + real Polymarket order-book midpoint. Fills simulated in paper-live mode.</p>
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
try { lastTradeEventId = localStorage.getItem('last_trade_event_id'); } catch (e) {}

/* ===== HELPERS ===== */
function fmtUsd(n) {
  const sign = n < 0 ? '-' : '';
  return sign + '$' + Math.abs(n).toLocaleString(undefined, {
    minimumFractionDigits: 2, maximumFractionDigits: 2
  });
}

/* ===== CLIENT-SIDE BTC SPOT TICKER ===== */
async function fetchClientBtcPrice() {
  try {
    const r = await fetch('https://hermes.pyth.network/v2/updates/price/latest?ids[]=0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43');
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
  if (String(st.trade_event_id) === String(lastTradeEventId)) return;
  lastTradeEventId = String(st.trade_event_id);
  try { localStorage.setItem('last_trade_event_id', lastTradeEventId); } catch (e) {}

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
    const cls = st.side === 'UP' ? 'up' : 'down';
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
    svg.innerHTML = `<text x="${W/2}" y="${H/2}" fill="#5a6478" font-family="Inter" font-size="13" text-anchor="middle">Equity data will appear here…</text>`;
    return;
  }
  const min = Math.min(...eq), max = Math.max(...eq), span = (max - min) || 1;
  const x = i => pad + i * (W - 2 * pad) / (eq.length - 1);
  const y = v => pad + (1 - (v - min) / span) * (H - 2 * pad);
  const pts = eq.map((v, i) => x(i) + ',' + y(v)).join(' ');
  const last = eq[eq.length - 1], up = last >= start;
  const col = up ? '#00ff88' : '#ff4757';
  const colDim = up ? 'rgba(0,255,136,0.10)' : 'rgba(255,71,87,0.10)';
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
    `stroke="#0a0e17" stroke-width="2">` +
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
    { k: 'Win Rate',        v: (stats.win_rate || 0) + '%',     s: (stats.wins || 0) + 'W / ' + (stats.losses || 0) + 'L' },
    { k: 'Trades',          v: stats.trades || 0,               s: '5-min rounds' },
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
    const sideCls = t.side === 'UP' ? 'up' : 'down';
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
    st = await fetch('state/status.json', { cache: 'no-store' }).then(r => r.ok ? r.json() : null);
  } catch (e) { /* optional file */ }
  if (st) {
    lastPollTime = Date.now() / 1000; // update connection freshness
    if (st.updated_at && parseFloat(st.updated_at) !== parseFloat(lastStatusTs)) {
      serverTimeOffset = parseFloat(st.updated_at) - (Date.now() / 1000);
    }
    try { localStorage.setItem('cache_status', JSON.stringify(st)); } catch(e){}
    renderStatus(st);
  }

  try {
    const [s, t] = await Promise.all([
      fetch('state/stats.json',  { cache: 'no-store' }).then(r => r.json()),
      fetch('state/trades.json', { cache: 'no-store' }).then(r => r.json()),
    ]);
    lastPollTime = Date.now() / 1000; // update connection freshness
    try {
      localStorage.setItem('cache_stats', JSON.stringify(s));
      localStorage.setItem('cache_trades', JSON.stringify(t));
    } catch(e){}
    document.getElementById('err').textContent = '';
    render(s, t);
  } catch (e) {
    document.getElementById('err').textContent =
      'Waiting for state files (start the bot)… ' + e;
  }

  // Poll real-time logs from server
  try {
    const logs = await fetch('/api/logs', { cache: 'no-store' }).then(r => r.ok ? r.text() : '');
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
  const cachedStatus = localStorage.getItem('cache_status');
  const cachedStats  = localStorage.getItem('cache_stats');
  const cachedTrades = localStorage.getItem('cache_trades');
  
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
      const s = JSON.parse(localStorage.getItem('cache_stats') || 'null');
      const t = JSON.parse(localStorage.getItem('cache_trades') || 'null');
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
  stEl.textContent = running
    ? ('running · pid ' + ((s && s.pid) || '?'))
    : 'idle';
  stEl.className = 'ctl-status ' + (running ? 'on' : '');
}

async function ctlState() {
  try {
    const s = await fetch('/api/control', { cache: 'no-store' }).then(r => r.json());
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
    startBtn.style.background = 'linear-gradient(135deg,#ff4757,#c0392b)';
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
    const r = await fetch('/api/stop', { method: 'POST' }).then(r => r.json());
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
let settingsActiveGroup = 'late';

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
        const v = f.value || 'mid';
        return `<div class="sp-field"><label>${f.label}</label>
          <select id="${id}">
            <option value="mid" ${v==='mid'?'selected':''}>mid</option>
            <option value="late_scalp" ${v==='late_scalp'?'selected':''}>late_scalp (selective)</option>
            <option value="late_every" ${v==='late_every'?'selected':''}>late_every (every 5m)</option>
            <option value="hybrid" ${v==='hybrid'?'selected':''}>hybrid</option>
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
  // Sync Start panel strategy menu
  const stratField = (data.fields || []).find(f => f.key === 'STRATEGY');
  const everyField = (data.fields || []).find(f => f.key === 'LATE_SCALP_EVERY_SLOT');
  let strat = stratField ? String(stratField.value || 'mid') : 'mid';
  const everyOn = everyField && ['1','true','yes','on'].includes(String(everyField.value).toLowerCase());
  if (strat === 'late_scalp' && everyOn) strat = 'late_every';
  const cStrat = document.getElementById('c-strategy');
  if (cStrat && [...cStrat.options].some(o => o.value === strat)) {
    cStrat.value = strat;
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
document.getElementById('sp-preset-late-every').onclick = () => saveSettings({
  STRATEGY: 'late_every',
  LATE_SCALP_EVERY_SLOT: '1',
  LATE_SCALP_MIN_ENTRY: '0.88',
  LATE_SCALP_MAX_ENTRY: '0.95',
  LATE_SCALP_PREFER_MAX: '0.95',
  LATE_SCALP_MIN_REMAINING: '8',
  LATE_SCALP_MAX_REMAINING: '20',
  LATE_SCALP_MIN_FAVORITE_LEAD: '0.10',
  LATE_SCALP_MAX_UNDERDOG_ASK: '0.42',
  LATE_SCALP_REQUIRE_SPOT_ALIGN: '1',
  LATE_SCALP_SPOT_ALIGN_EPS_USD: '10',
  LATE_SCALP_REQUOTE: '1',
  LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS: '2',
  LATE_SCALP_MAX_STAKE_USD: '5.0',
  LATE_SCALP_MAX_SPREAD: '0.10',
  LATE_SCALP_MIN_MOVE_USD: '15.0',
  LATE_SCALP_REQUIRE_CHAINLINK: '1',
  LATE_SCALP_MIN_EDGE: '0.02',
});
document.getElementById('sp-preset-late90').onclick = () => saveSettings({
  STRATEGY: 'late_scalp',
  LATE_SCALP_EVERY_SLOT: '0',
  LATE_SCALP_MIN_ENTRY: '0.88',
  LATE_SCALP_MAX_ENTRY: '0.99',
  LATE_SCALP_PREFER_MAX: '0.95',
  LATE_SCALP_MIN_REMAINING: '10',
  LATE_SCALP_MAX_REMAINING: '45',
  LATE_SCALP_MIN_FAVORITE_LEAD: '0.50',
  LATE_SCALP_MAX_UNDERDOG_ASK: '0.14',
  LATE_SCALP_REQUIRE_SPOT_ALIGN: '1',
  LATE_SCALP_SPOT_ALIGN_EPS_USD: '8',
  LATE_SCALP_KEEP_FIRST_ASK: '1',
  LATE_SCALP_REQUOTE: '1',
  LATE_SCALP_MAX_STAKE_USD: '5.0',
});
document.getElementById('sp-preset-late-strict').onclick = () => saveSettings({
  STRATEGY: 'late_scalp',
  LATE_SCALP_EVERY_SLOT: '0',
  LATE_SCALP_MIN_ENTRY: '0.93',
  LATE_SCALP_MAX_ENTRY: '0.96',
  LATE_SCALP_PREFER_MAX: '0.96',
  LATE_SCALP_MIN_FAVORITE_LEAD: '0.70',
  LATE_SCALP_MAX_UNDERDOG_ASK: '0.08',
  LATE_SCALP_MAX_REMAINING: '22',
  LATE_SCALP_REQUIRE_SPOT_ALIGN: '1',
  LATE_SCALP_REQUOTE: '1',
});
document.getElementById('sp-preset-mid-active').onclick = () => saveSettings({
  STRATEGY: 'mid',
  MIN_ENTRY_PRICE: '0.22',
  MAX_ENTRY_PRICE: '0.72',
  EDGE_THRESHOLD: '0.04',
  MIN_EV: '0.02',
  MIN_MOVE_USD: '5',
  MID_NOISE_MOVE_CAP: '0',
  MID_REQUIRE_CHAINLINK_STRIKE: '0',
  MID_MIN_MODEL: '0.52',
  MID_MAX_MODEL: '0.95',
});

// Save settings before start so bands always match UI
const _origStart = document.getElementById('c-start').onclick;
document.getElementById('c-start').addEventListener('click', async (ev) => {
  // Auto-sync strategy menu → .env (late_every also flips EVERY_SLOT)
  try {
    const strat = document.getElementById('c-strategy').value;
    const settings = { STRATEGY: strat };
    if (strat === 'late_every') settings.LATE_SCALP_EVERY_SLOT = '1';
    if (strat === 'late_scalp') settings.LATE_SCALP_EVERY_SLOT = '0';
    await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ settings }),
    });
  } catch (e) {}
}, true);

// When user changes strategy dropdown, reflect late_every in settings form if open
document.getElementById('c-strategy').addEventListener('change', () => {
  const strat = document.getElementById('c-strategy').value;
  const el = document.getElementById('sf-STRATEGY');
  if (el) el.value = strat;
  const every = document.getElementById('sf-LATE_SCALP_EVERY_SLOT');
  if (every) every.checked = (strat === 'late_every');
});

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

        # Dashboard
        if path in ("/", "/dashboard"):
            return self._html(DASHBOARD_HTML)

        # Control API
        if path == "/api/control":
            return self._json(_state())

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
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            body = _get_logs().encode("utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # State JSON files
        if path.startswith("/state/") and path.endswith(".json"):
            # Sanitize: only allow simple filenames
            fname = os.path.basename(path)
            if fname in ("stats.json", "trades.json", "status.json"):
                fpath = os.path.join(STATE_DIR, fname)
                return self._serve_file(fpath, "application/json")

        # Fallback 404
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Not Found")

    def do_POST(self):
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
            return self._json(_stop())
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

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler)

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

    # Auto-open browser
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        _stop()
        print("\n  Goodbye! Bot stopped.\n")
