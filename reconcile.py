"""Re-check locally-resolved trades against Polymarket, and correct them.

WHY THIS EXISTS
---------------
When a slot closes the bot polls Gamma for the official outcome. If Gamma has
not published yet, it falls back to resolving from its own Chainlink tick
buffer — and then books that result permanently. Nothing ever went back to
check.

That fallback is not reliable. Audited over the first 56 live trades, 7 were
booked with the wrong outcome, every one of them from a non-official source:

    R55  UP  @0.61  booked WON +1.95   Polymarket: DOWN  -> actually LOST
    R56 DOWN @0.56  booked LOST -2.80  Polymarket: DOWN  -> actually WON
    R7/36/41/46/47 (ARB) outcome label wrong; PnL unaffected (a pair pays
                   either way, which is exactly why they went unnoticed)

The errors run in both directions, so this is unreliability rather than bias —
but a fabricated win is still a fabricated win, and a bankroll built on them is
fiction. Local resolution disagrees most often on near-ties, where the slot is
decided by a few dollars and the exact tick chosen matters.

Polymarket's own resolution is the only source that defines the outcome, so
this module treats it as the sole authority: any trade resolved by anything
else is provisional until confirmed here.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import paths

STATE_DIR = paths.state_dir()
ARCHIVE_DIR = paths.archive_dir()
HISTORY_JSONL = os.path.join(ARCHIVE_DIR, "trades_history.jsonl")
TRADES_PATH = os.path.join(STATE_DIR, "trades.json")
STATS_PATH = os.path.join(STATE_DIR, "stats.json")
CORRECTIONS_LOG = os.path.join(ARCHIVE_DIR, "corrections.jsonl")

# Anything other than these was a guess, however confident the log line looked.
TRUSTED_SOURCES = ("polymarket-official", "polymarket-official-corrected")
# Give Gamma time to publish before treating a slot as checkable.
MIN_AGE_S = float(os.getenv("RECONCILE_MIN_AGE_S", "180"))


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.isfile(path):
        return rows
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        continue
    except OSError:
        pass
    return rows


def _atomic_write(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _recompute(trade: dict, official: str) -> dict:
    """Rebook a trade against the official outcome (mirrors executor math)."""
    out = dict(trade)
    fee = float(out.get("fee_usd") or 0.0)
    shares = float(out.get("shares") or 0.0)
    stake = float(out.get("stake_usd") or 0.0)
    out["outcome"] = official
    if out.get("side") == "ARB":
        # One leg always pays $1/share whichever way it lands, so an ARB's PnL
        # does not depend on the outcome — only the label was wrong.
        payout = shares
        out["pnl"] = round(payout - stake - fee, 4)
        out["won"] = out["pnl"] > 0
    else:
        out["won"] = (out.get("side") == official)
        payout = shares * (1.0 if out["won"] else 0.0)
        out["pnl"] = round(payout - stake - fee, 4)
    out["resolve_source"] = "polymarket-official-corrected"
    out["corrected_at"] = time.time()
    return out


def _journal(entry: dict) -> None:
    entry.setdefault("ts", time.time())
    entry.setdefault("time_utc", datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"))
    try:
        with open(CORRECTIONS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=True, default=str) + "\n")
    except OSError:
        pass


def reconcile(pub=None, max_lookups: int = 300, verbose: bool = True) -> dict:
    """Confirm or correct every provisionally-resolved trade.

    Returns a summary. Corrections are applied to the permanent archive, the
    live blotter, and the bankroll in stats.json, and each one is journalled.
    """
    def say(msg):
        if verbose:
            print(f"[reconcile] {msg}")

    history = _read_jsonl(HISTORY_JSONL)
    if not history:
        return {"checked": 0, "corrected": 0, "pnl_delta": 0.0}

    if pub is None:
        from polymarket_client import PolymarketPublic
        pub = PolymarketPublic()

    now = time.time()
    # One lookup per slug — several trades can share a slot.
    pending: dict[str, list[dict]] = {}
    for t in history:
        if t.get("resolve_source") in TRUSTED_SOURCES:
            continue
        if t.get("won") is None:
            continue
        slug = t.get("slug")
        end_ts = t.get("end_ts") or t.get("resolved_ts")
        if not slug or not end_ts or (now - float(end_ts)) < MIN_AGE_S:
            continue
        pending.setdefault(slug, []).append(t)

    if not pending:
        return {"checked": 0, "corrected": 0, "pnl_delta": 0.0,
                "provisional": 0}

    official_by_slug: dict[str, str] = {}
    for slug in list(pending)[:max_lookups]:
        try:
            outcome = pub.get_official_btc_updown_outcome(slug)
        except Exception:
            continue
        if outcome in ("UP", "DOWN"):
            official_by_slug[slug] = outcome

    corrected = 0
    confirmed = 0
    pnl_delta = 0.0
    changes = []
    for t in history:
        slug = t.get("slug")
        if slug not in official_by_slug or t.get("resolve_source") in TRUSTED_SOURCES:
            continue
        official = official_by_slug[slug]
        before_pnl = float(t.get("pnl") or 0.0)
        before_outcome = t.get("outcome")
        before_won = t.get("won")
        fixed = _recompute(t, official)
        t.update(fixed)
        if before_outcome != official or before_won != fixed["won"] \
                or abs(before_pnl - float(fixed["pnl"])) > 1e-9:
            corrected += 1
            delta = float(fixed["pnl"]) - before_pnl
            pnl_delta += delta
            change = {
                "round_id": t.get("round_id"), "slug": slug,
                "side": t.get("side"),
                "was": {"outcome": before_outcome, "won": before_won,
                        "pnl": round(before_pnl, 4)},
                "now": {"outcome": official, "won": fixed["won"],
                        "pnl": fixed["pnl"]},
                "pnl_delta": round(delta, 4),
            }
            changes.append(change)
            _journal({"action": "correct", **change})
            say(f"R{t.get('round_id')} {t.get('side')}: "
                f"{before_outcome}/{before_won} -> {official}/{fixed['won']} "
                f"pnl {before_pnl:+.2f} -> {float(fixed['pnl']):+.2f} "
                f"({delta:+.2f})")
        else:
            confirmed += 1

    if corrected or confirmed:
        _atomic_write(HISTORY_JSONL,
                      "".join(json.dumps(r, ensure_ascii=True, default=str) + "\n"
                              for r in history))

    # Mirror the corrections into the live blotter so the dashboard agrees.
    if corrected:
        try:
            with open(TRADES_PATH, "r", encoding="utf-8") as f:
                blotter = json.load(f)
            by_key = {(r.get("round_id"), r.get("slug")): r for r in history}
            for row in blotter:
                fixed = by_key.get((row.get("round_id"), row.get("slug")))
                if fixed:
                    row.update({k: fixed[k] for k in
                                ("outcome", "won", "pnl", "resolve_source")
                                if k in fixed})
            _atomic_write(TRADES_PATH, json.dumps(blotter, indent=2, default=str))
        except (OSError, ValueError) as e:
            say(f"could not update the live blotter: {e}")

        # The bankroll was built on the wrong numbers; move it by the delta so
        # the next --resume picks up the corrected account.
        try:
            with open(STATS_PATH, "r", encoding="utf-8") as f:
                stats = json.load(f)
            old = float(stats.get("bankroll") or 0.0)
            stats["bankroll"] = round(old + pnl_delta, 2)
            stats["reconciled_at"] = now
            stats["reconcile_pnl_delta"] = round(pnl_delta, 4)
            _atomic_write(STATS_PATH, json.dumps(stats, indent=2, default=str))
            say(f"bankroll ${old:.2f} -> ${stats['bankroll']:.2f} "
                f"({pnl_delta:+.2f} from {corrected} correction(s))")
        except (OSError, ValueError) as e:
            say(f"could not adjust the bankroll: {e}")

    still_provisional = sum(
        1 for t in history if t.get("resolve_source") not in TRUSTED_SOURCES
        and t.get("won") is not None)
    return {"checked": confirmed + corrected, "confirmed": confirmed,
            "corrected": corrected, "pnl_delta": round(pnl_delta, 4),
            "provisional": still_provisional, "changes": changes}


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv:
        # Report what would change without touching anything.
        from polymarket_client import PolymarketPublic
        pub = PolymarketPublic()
        hist = _read_jsonl(HISTORY_JSONL)
        bad = 0
        for t in hist:
            if t.get("resolve_source") in TRUSTED_SOURCES or t.get("won") is None:
                continue
            try:
                official = pub.get_official_btc_updown_outcome(t.get("slug"))
            except Exception:
                continue
            if official in ("UP", "DOWN") and official != t.get("outcome"):
                fixed = _recompute(t, official)
                bad += 1
                print(f"  R{t.get('round_id')} {t.get('side')} "
                      f"{t.get('outcome')}->{official} "
                      f"pnl {t.get('pnl'):+.2f} -> {fixed['pnl']:+.2f}")
        print(f"{bad} trade(s) would be corrected")
    else:
        print(json.dumps(reconcile(), indent=2, default=str))
