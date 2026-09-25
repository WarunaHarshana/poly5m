"""Tiny HTTP status API so Railway can expose /trades and /stats as JSON."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import config

logger = logging.getLogger("http-api")

SnapshotFn = Callable[[], dict[str, Any]]


def is_official_trade(row: dict) -> bool:
    src = str(row.get("settlement_source") or "")
    if src.startswith("polymarket"):
        return True
    return row.get("official") is True


def load_trades(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def public_trade(row: dict) -> dict:
    slug = row.get("slot") or row.get("slug") or ""
    asset = str(row.get("asset") or "").lower()
    if not asset and isinstance(slug, str) and "-" in slug:
        asset = slug.split("-", 1)[0]

    cost = float(row.get("cost_usd") or row.get("cost") or 0.0)
    fee = float(row.get("fee") or row.get("fee_usd") or 0.0)
    exit_fee = float(row.get("exit_fee") or 0.0)
    total_fee = round(fee + exit_fee, 4)
    total_cost = float(row.get("total_cost") or round(cost + total_fee, 4))

    entry_time_utc = row.get("entry_time_utc")
    if not entry_time_utc and not row.get("early_exit"):
        entry_time_utc = row.get("timestamp")

    exit_time_utc = row.get("exit_time_utc") or (
        row.get("timestamp") if (row.get("won") is not None or row.get("early_exit")) else None
    )

    exit_price = row.get("exit_price")
    if exit_price is None and row.get("won") is not None:
        exit_price = 1.0 if row.get("won") else 0.0

    exit_reason = row.get("exit_reason")
    if not exit_reason and row.get("won") is not None:
        exit_reason = "settlement_won" if row.get("won") else "settlement_lost"

    entry_elapsed_s = row.get("entry_elapsed_s")
    exit_elapsed_s = row.get("exit_elapsed_s")
    entry_window_time = row.get("entry_window_time")
    exit_window_time = row.get("exit_window_time")

    if (entry_elapsed_s is None or entry_window_time is None) and slug and "-" in slug:
        try:
            slot_start = float(slug.rsplit("-", 1)[-1])
            if entry_time_utc:
                dt = datetime.fromisoformat(str(entry_time_utc).replace("Z", "+00:00"))
                el = max(0.0, min(300.0, round(dt.timestamp() - slot_start, 1)))
                entry_elapsed_s = el
                entry_window_time = f"t={int(el)}s ({int(el)//60:02d}:{int(el)%60:02d})"
        except Exception:
            pass

    if (exit_elapsed_s is None or exit_window_time is None) and (row.get("won") is not None or row.get("early_exit")):
        if row.get("early_exit") and exit_time_utc and slug and "-" in slug:
            try:
                slot_start = float(slug.rsplit("-", 1)[-1])
                dt = datetime.fromisoformat(str(exit_time_utc).replace("Z", "+00:00"))
                el = max(0.0, min(300.0, round(dt.timestamp() - slot_start, 1)))
                exit_elapsed_s = el
                exit_window_time = f"t={int(el)}s ({int(el)//60:02d}:{int(el)%60:02d})"
            except Exception:
                pass
        elif not row.get("early_exit") and row.get("won") is not None:
            exit_elapsed_s = 300.0
            exit_window_time = "t=300s (05:00 - expiry)"

    return {
        "slug": slug,
        "asset": asset,
        "side": row.get("side"),
        "price": row.get("price"),
        "shares": row.get("shares"),
        "cost": round(cost, 4),
        "fee": total_fee,
        "total_cost": round(total_cost, 4),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "entry_time_utc": entry_time_utc,
        "exit_time_utc": exit_time_utc,
        "entry_elapsed_s": entry_elapsed_s,
        "entry_window_time": entry_window_time,
        "exit_elapsed_s": exit_elapsed_s,
        "exit_window_time": exit_window_time,
        "pnl": row.get("pnl"),
        "won": None if row.get("won") is None else bool(row.get("won")),
        "outcome": row.get("outcome"),
        "strategy": row.get("strategy"),
        "z_score": row.get("z_score"),
        "strike": row.get("strike"),
        "final_twap": row.get("final_twap"),
        "bankroll": row.get("bankroll"),
        "early_exit": bool(row.get("early_exit")),
        "settlement_source": row.get("settlement_source"),
        "official": is_official_trade(row),
        "unofficial": not is_official_trade(row),
        "polymarket_url": f"https://polymarket.com/event/{slug}" if slug else None,
        "jev": row.get("jev"),
        "jev_shadow_verdict": _row_shadow_verdict(row),
        "jev_shadow_conviction": row.get("jev_shadow_conviction"),
    }


def _row_shadow_verdict(row: dict) -> str | None:
    v = row.get("jev_shadow_verdict")
    if v is None:
        jev = row.get("jev") or {}
        if isinstance(jev, dict) and jev.get("shadow"):
            v = jev.get("shadow_verdict")
    return v


# --- /stats breakdown helpers (reporting only; no trading behaviour) -------

EXIT_REASON_BUCKETS = (
    "settled_win", "settled_loss", "take_profit", "trailing_profit",
    "adverse_stop", "price_stop", "hard_flatten_expiry", "failsafe", "exit_before", "other",
)
ENTRY_ELAPSED_BUCKETS = ("<45", "45-90", "90-120", "120-180", "180+", "unknown")
STRATEGY_LEG_BUCKETS = ("momentum", "maker", "other")
JEV_SHADOW_BUCKETS = ("approve", "veto", "downsize", "error", "none")


def exit_reason_bucket(reason: Any) -> str:
    """Normalize a free-text exit_reason (e.g. 'trailing_profit (peak: 0.80, ...)') to a category."""
    r = str(reason or "").strip().lower()
    if not r:
        return "other"
    if r.startswith(("settlement_won", "settled_win")):
        return "settled_win"
    if r.startswith(("settlement_lost", "settled_loss", "settlement_loss")):
        return "settled_loss"
    if r.startswith("take_profit"):
        return "take_profit"
    if r.startswith("trailing_profit"):
        return "trailing_profit"
    if r.startswith(("stop_loss", "urgent_stop_loss", "adverse_stop", "salvage")):
        return "adverse_stop"
    if r.startswith("price_stop"):
        return "price_stop"
    if r.startswith("hard_flatten"):
        return "hard_flatten_expiry"
    if r.startswith("failsafe"):
        return "failsafe"
    if r.startswith("exit_before"):
        return "exit_before"
    return "other"


def entry_elapsed_bucket(elapsed: Any) -> str:
    """Bucket seconds elapsed in the 5-min round at entry."""
    if elapsed is None or isinstance(elapsed, bool):
        return "unknown"
    try:
        e = float(elapsed)
    except (TypeError, ValueError):
        return "unknown"
    if e != e or e < 0:  # NaN / negative
        return "unknown"
    if e < 45:
        return "<45"
    if e < 90:
        return "45-90"
    if e < 120:
        return "90-120"
    if e < 180:
        return "120-180"
    return "180+"


def strategy_leg_bucket(strategy: Any) -> str:
    s = str(strategy or "").upper()
    if "MOMENTUM" in s:
        return "momentum"
    if "MAKER" in s:
        return "maker"
    return "other"


def jev_shadow_bucket(verdict: Any) -> str:
    """Bucket the real (non-enforced) Jev verdict recorded under JEV_SHADOW."""
    v = str(verdict or "").strip().lower()
    if v in ("shadow_error", "error", "timeout"):
        return "error"
    if v in ("approve", "veto", "downsize"):
        return v
    return "none"


def _group_resolved(resolved: list[dict], key_fn: Callable[[dict], str], order: tuple[str, ...] | None = None) -> dict[str, dict]:
    groups: dict[str, dict] = {}
    for r in resolved:
        k = key_fn(r)
        bucket = groups.setdefault(k, {"trades": 0, "wins": 0, "pnl": 0.0, "cost": 0.0, "fee": 0.0, "total_cost": 0.0})
        bucket["trades"] += 1
        bucket["wins"] += 1 if r.get("won") else 0
        bucket["pnl"] = round(bucket["pnl"] + float(r.get("pnl") or 0.0), 2)
        bucket["cost"] = round(bucket["cost"] + float(r.get("cost") or 0.0), 4)
        bucket["fee"] = round(bucket["fee"] + float(r.get("fee") or 0.0), 4)
        bucket["total_cost"] = round(bucket["total_cost"] + float(r.get("total_cost") or r.get("cost") or 0.0), 4)
    for bucket in groups.values():
        n = bucket["trades"]
        bucket["win_rate"] = round(bucket["wins"] / n, 4) if n else 0.0
    if order:
        rank = {k: i for i, k in enumerate(order)}
        groups = dict(sorted(groups.items(), key=lambda kv: rank.get(kv[0], len(rank))))
    return groups


def build_stats(trades: list[dict], snapshot: dict[str, Any] | None = None) -> dict:
    rows = [public_trade(t) for t in trades]
    resolved = [r for r in rows if r.get("won") is not None and r.get("pnl") is not None]
    wins = sum(1 for r in resolved if r.get("won"))
    pnl = sum(float(r.get("pnl") or 0.0) for r in resolved)
    cost = sum(float(r.get("cost") or 0.0) for r in resolved)
    total_fee = sum(float(r.get("fee") or 0.0) for r in resolved)
    total_outlay = sum(float(r.get("total_cost") or (float(r.get("cost") or 0.0) + float(r.get("fee") or 0.0))) for r in resolved)
    by_asset = _group_resolved(resolved, lambda r: r.get("asset") or "unknown")
    by_exit_reason = _group_resolved(resolved, lambda r: exit_reason_bucket(r.get("exit_reason")), EXIT_REASON_BUCKETS)
    by_entry_elapsed = _group_resolved(resolved, lambda r: entry_elapsed_bucket(r.get("entry_elapsed_s")), ENTRY_ELAPSED_BUCKETS)
    by_strategy_leg = _group_resolved(resolved, lambda r: strategy_leg_bucket(r.get("strategy")), STRATEGY_LEG_BUCKETS)
    by_jev_shadow = _group_resolved(resolved, lambda r: jev_shadow_bucket(r.get("jev_shadow_verdict")), JEV_SHADOW_BUCKETS)
    now = time.time()
    day_pnl = 0.0
    for raw in trades:
        if raw.get("pnl") is None:
            continue
        ts = str(raw.get("timestamp") or "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if (now - dt.timestamp()) <= 86400:
                day_pnl += float(raw.get("pnl") or 0.0)
        except Exception:
            continue
    snap = snapshot or {}
    n = len(resolved)
    return {
        "service": "poly5m-bot",
        "mode": snap.get("mode") or config.MODE,
        "strategy": snap.get("strategy") or config.STRATEGY,
        "assets": snap.get("assets") or list(config.ASSETS),
        "trades": n,
        "resolved": n,
        "wins": wins,
        "win_rate": round(wins / n, 4) if n else 0.0,
        "pnl": round(pnl, 2),
        "cost": round(cost, 4),
        "fee": round(total_fee, 4),
        "total_cost": round(total_outlay, 4),
        "daily_pnl_24h": round(day_pnl, 2),
        "bankroll": snap.get("bankroll"),
        "daily_pnl": snap.get("daily_pnl"),
        "open_positions": snap.get("open_positions") or [],
        "uptime_seconds": snap.get("uptime_seconds"),
        "by_asset": by_asset,
        "by_exit_reason": by_exit_reason,
        "by_entry_elapsed": by_entry_elapsed,
        "by_strategy_leg": by_strategy_leg,
        "by_jev_shadow": by_jev_shadow,
        "jev": snap.get("jev"),
    }


def _make_handler(bot_snapshot: SnapshotFn, trades_path: Path):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:
            logger.debug("%s - " + fmt, self.address_string(), *args)

        def _send(self, code: int, payload: Any) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = parse_qs(parsed.query)
            try:
                if path in ("/", "/health"):
                    snap = bot_snapshot()
                    self._send(200, {
                        "status": "healthy",
                        "service": "poly5m-bot",
                        **{k: snap.get(k) for k in (
                            "mode", "strategy", "assets", "bankroll", "daily_pnl",
                            "open_positions", "pending_settlements", "uptime_seconds",
                            "jev",
                        )},
                    })
                    return
                trades = load_trades(trades_path)
                asset_filter = (qs.get("asset") or [""])[0].strip().lower()
                include_unofficial = (qs.get("include_unofficial") or ["0"])[0].strip().lower() in (
                    "1", "true", "yes"
                )
                if not include_unofficial:
                    trades = [t for t in trades if is_official_trade(t)]
                if path == "/trades":
                    rows = [public_trade(t) for t in reversed(trades)]
                    if asset_filter:
                        rows = [r for r in rows if r.get("asset") == asset_filter]
                    self._send(200, {"count": len(rows), "trades": rows})
                    return
                if path == "/stats":
                    snap = bot_snapshot()
                    filtered = trades
                    if asset_filter:
                        filtered = [
                            t for t in trades
                            if str(t.get("asset") or "").lower() == asset_filter
                            or str(t.get("slot") or "").startswith(asset_filter + "-")
                        ]
                    self._send(200, build_stats(filtered, snap))
                    return
                self._send(404, {"error": "not found", "paths": ["/", "/health", "/stats", "/trades"]})
            except Exception as e:
                logger.error("http handler failed: %s", e)
                self._send(500, {"error": str(e)})

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

    return Handler


class StatusServer:
    def __init__(self, snapshot: SnapshotFn, trades_path: Path, host: str = "0.0.0.0", port: int = 8080):
        self.snapshot = snapshot
        self.trades_path = Path(trades_path)
        self.host = host
        self.port = int(port)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        handler = _make_handler(self.snapshot, self.trades_path)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="status-http")
        self._thread.start()
        logger.info("Status HTTP listening on %s:%s  (/trades /stats /health)", self.host, self.port)

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
