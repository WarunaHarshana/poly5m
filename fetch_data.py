"""Pull the running bot's research data down to this machine.

The bot's data lives on the Railway volume, which `railway run` cannot reach —
that command runs locally with the service's variables injected, not inside the
container. So the dashboard exposes an authenticated export endpoint and this
script downloads it.

    python fetch_data.py https://your-app.up.railway.app --token XXXX

By default it writes into ``data/<host>/`` next to this file, then prints what
arrived. Point the analysis tools at it with STATE_ROOT:

    STATE_ROOT=data/your-app.up.railway.app python analyze.py
    STATE_ROOT=data/your-app.up.railway.app python tuner.py --status

``--merge`` instead appends the downloaded rows into your local ``state/``
archive, de-duplicated, so local and deployed history become one dataset.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))

# Files that are append-only JSON Lines and can therefore be merged row by row.
# Everything else is a snapshot and is simply replaced.
MERGEABLE = {
    "archive/observations.jsonl": ("slug", "ts"),
    "archive/trades_history.jsonl": ("ts", "round_id", "slug"),
    "archive/decisions.jsonl": ("ts", "slug", "action"),
    "archive/tuning_log.jsonl": ("ts", "action"),
    "archive/book_recordings.jsonl": ("slug", "ts"),
}


def download(base_url: str, token: str, full: bool, timeframe: str) -> bytes:
    q = {"token": token, "tf": timeframe}
    if full:
        q["full"] = "1"
    url = base_url.rstrip("/") + "/api/export?" + urllib.parse.urlencode(q)
    shown = url.replace(token, "***") if token else url
    print(f"GET {shown}")
    req = urllib.request.Request(url, headers={"User-Agent": "polybot-fetch"})
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]
        if e.code == 401:
            raise SystemExit(
                "401 Unauthorized - the token is wrong or missing.\n"
                "It must match DASH_TOKEN in the Railway service variables.")
        if e.code == 404:
            raise SystemExit(f"404 - {body}")
        raise SystemExit(f"HTTP {e.code}: {body}")
    except urllib.error.URLError as e:
        raise SystemExit(f"Could not reach {base_url}: {e.reason}")


def _row_key(row: dict, fields: tuple) -> tuple:
    return tuple(str(row.get(f, "")) for f in fields)


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    continue
    return rows


def merge_jsonl(dest: str, incoming: list[dict], key_fields: tuple) -> tuple[int, int]:
    """Append rows not already present. Returns (added, skipped)."""
    existing = _read_jsonl(dest)
    seen = {_row_key(r, key_fields) for r in existing}
    added = 0
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with open(dest, "a", encoding="utf-8") as f:
        for row in incoming:
            k = _row_key(row, key_fields)
            if k in seen:
                continue
            seen.add(k)
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
            added += 1
    return added, len(incoming) - added


def summarize(out_dir: str) -> None:
    obs = _read_jsonl(os.path.join(out_dir, "archive", "observations.jsonl"))
    trades = _read_jsonl(os.path.join(out_dir, "archive", "trades_history.jsonl"))
    labeled = [r for r in obs if r.get("outcome") in ("UP", "DOWN")]
    print("\n--- what you now have locally ---")
    print(f"  observations : {len(obs):>6}  ({len(labeled)} labeled)")
    print(f"  trades       : {len(trades):>6}")
    if trades:
        settled = [t for t in trades if t.get("won") is not None]
        if settled:
            pnl = sum(float(t.get("pnl") or 0) for t in settled)
            wins = sum(1 for t in settled if t.get("won"))
            print(f"  realized pnl : {pnl:>+9.2f} over {len(settled)} settled "
                  f"({100.0 * wins / len(settled):.1f}% win rate, "
                  f"{pnl / len(settled):+.4f}/trade)")
    tuned_path = os.path.join(out_dir, "tuned_params.json")
    if os.path.isfile(tuned_path):
        try:
            with open(tuned_path, encoding="utf-8") as f:
                tuned = json.load(f)
            print(f"  tuned params : {json.dumps(tuned.get('params', {}))}")
            print(f"                 updated {tuned.get('updated_utc', '?')}")
        except (OSError, ValueError):
            pass


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Download the deployed bot's research data.")
    ap.add_argument("url", help="dashboard base URL, e.g. "
                                "https://your-app.up.railway.app")
    ap.add_argument("--token", default=os.getenv("DASH_TOKEN", ""),
                    help="DASH_TOKEN (defaults to the env var of that name)")
    ap.add_argument("--out", default=None,
                    help="output directory (default: data/<host>)")
    ap.add_argument("--full", action="store_true",
                    help="also fetch book recordings and the bot log (large)")
    ap.add_argument("--timeframe", default="5m", choices=["5m", "15m"])
    ap.add_argument("--merge", action="store_true",
                    help="merge into the local state/ archive instead of "
                         "writing a separate directory (de-duplicated)")
    args = ap.parse_args()

    if not args.token:
        raise SystemExit(
            "No token. Pass --token, or set DASH_TOKEN in your environment.\n"
            "It is the same value as DASH_TOKEN in the Railway variables.")

    blob = download(args.url, args.token, args.full, args.timeframe)
    print(f"downloaded {len(blob) / 1024:.1f} KiB")

    zf = zipfile.ZipFile(io.BytesIO(blob))
    try:
        manifest = json.loads(zf.read("MANIFEST.json"))
        for entry in manifest.get("files", []):
            print(f"  {entry['file']:<34} {entry['bytes'] / 1024:>9.1f} KiB")
    except (KeyError, ValueError):
        pass

    if args.merge:
        import paths
        dest_root = paths.state_dir()
        print(f"\nmerging into {dest_root}")
        for name in zf.namelist():
            if name == "MANIFEST.json":
                continue
            dest = os.path.join(dest_root, *name.split("/"))
            if name in MERGEABLE:
                incoming = [json.loads(l) for l in
                            zf.read(name).decode("utf-8").splitlines() if l.strip()]
                added, skipped = merge_jsonl(dest, incoming, MERGEABLE[name])
                print(f"  {name:<34} +{added} new, {skipped} already present")
            else:
                # Snapshots would clobber local state that is still in use;
                # keep them alongside instead of overwriting.
                side = dest + ".remote"
                os.makedirs(os.path.dirname(side), exist_ok=True)
                with open(side, "wb") as f:
                    f.write(zf.read(name))
                print(f"  {name:<34} -> {os.path.basename(side)}")
        summarize(dest_root)
        return 0

    host = urllib.parse.urlparse(args.url).netloc or "remote"
    out_dir = args.out or os.path.join(HERE, "data", host)
    os.makedirs(out_dir, exist_ok=True)
    zf.extractall(out_dir)
    print(f"\nextracted to {out_dir}")
    summarize(out_dir)
    print(f"\nAnalyse it with:\n"
          f"  STATE_ROOT={out_dir} python analyze.py\n"
          f"  STATE_ROOT={out_dir} python tuner.py --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
