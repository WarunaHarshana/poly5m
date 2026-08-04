# Running 24/7 on Railway (paper money)

One Railway service runs everything: the dashboard, a supervised `bot.py` that
trades paper money against **real** Polymarket books, and a nightly learning
cycle that tunes the strategy on data the bot collected itself.

No wallet, no private key, no funds. `MODE=paper_live` uses genuine BTC prices
and genuine order books but simulates every fill.

---

## 1. Push the repo to GitHub

Railway deploys from a Git repo. `.env` is gitignored and must stay that way —
secrets go in Railway's service variables instead.

```bash
git add -A && git commit -m "Add Railway 24/7 deployment + self-tuning loop"
```

Then push to your GitHub remote.

## 2. Create the service

New Project → **Deploy from GitHub repo** → pick this repo. Railway reads
`railway.json` and builds the `Dockerfile`; no other build setup is needed.

## 3. Attach a volume — do not skip this

The container filesystem is wiped on every deploy and restart. Without a
volume, each redeploy throws away the trade archive **and everything the tuner
has learned**, and the bot starts from zero forever.

In the service: **Variables → + Volume**, mount path:

```
/data
```

`STATE_ROOT=/data` is already set in the Dockerfile, so every module writes
there once the volume exists.

## 4. Set the variables

Required:

| Variable | Value | Why |
|---|---|---|
| `DASH_TOKEN` | a long random string | The dashboard can start/stop the bot, so a public bind without a token is refused at startup. Generate with `python -c "import secrets;print(secrets.token_urlsafe(32))"` |

Useful, all optional:

| Variable | Default | Meaning |
|---|---|---|
| `PAPER_BANKROLL` | whatever `.env`/config says | Starting paper balance |
| `PAPER_BET` | config default | Fixed stake per trade |
| `BOT_LIFETIME_HOURS` | `6` | Recycle the bot this often (feed hygiene) |
| `TUNE_INTERVAL_S` | `86400` | Seconds between tuning cycles |
| `TUNER_ENABLED` | `1` | `0` freezes the strategy at its current parameters |
| `TUNER_FROZEN_KEYS` | — | Comma-separated parameters the tuner may never move |
| `ASSET` | `btc` | `btc` / `eth` / `sol` |

Do **not** set `PRIVATE_KEY` or `MODE=live`. The supervisor refuses `live`
unless `SUPERVISOR_ALLOW_LIVE=1` is also set, precisely so a stray variable
cannot start real-money trading on a redeploy.

## 5. Open the dashboard

Settings → Networking → **Generate Domain**, then visit:

```
https://<your-domain>/?token=<DASH_TOKEN>
```

The token is stored as a cookie after the first load. `/healthz` is
unauthenticated so Railway's healthcheck can reach it.

---

## What runs, and when

| Job | Cadence | What it does |
|---|---|---|
| Trading | continuous | Evaluates every 5-minute slot; trades the ones that pass the gates |
| Observation logging | every slot | Records the full feature vector for **every** evaluated slot, traded or not |
| Outcome labeling | every 15 min | Fills in Polymarket's official resolution for settled slots |
| Tuning | every 24 h | Walk-forward search; applies a change only if it survives every guardrail |
| Bot recycle | every 6 h, or after a tuning change | Fresh feeds; picks up new parameters |

## How the self-improvement actually works

The bot records one row per *evaluated* slot — roughly 288/day — not just the
handful it trades. Each row carries the move, the ask, the spread, the model
edge, the time of day and the seconds remaining, and is labeled afterwards with
how the slot really resolved. That gives the tuner the counterfactual it needs:
"what would a wider entry band have earned on the slots it would have taken?"

Each night the tuner splits that history chronologically, fits on the older
part, and requires the winning parameter set to **also** beat the incumbent on
a recent slice it never saw. A change is written only if all of these hold:

- at least 300 labeled slots exist,
- the candidate trades enough slots in both slices to mean anything,
- it improves expectancy on train **and** holdout,
- holdout expectancy is positive with a t-statistic of at least 2,
- and no parameter moves more than one notch on its ladder.

Anything that fails is journalled to `state/archive/tuning_log.jsonl` with the
reason and nothing changes. If a change that *did* pass is followed by real
losses, `check_revert` rolls it back automatically.

`_selftest_tuner.py` measures the property that matters: fed 80 datasets of
pure noise, the tuner changed nothing 80 times. Fed a real effect, it found it.

### The honest part

This tunes thresholds on an existing signal. It cannot create edge that is not
there, and on this strategy the historical record is mixed: the paper archive
shows roughly +4% ROI while the 53 real-money trades came in at −9.7%, which is
what simulated fills flatter. Long stretches of "no change — the evidence does
not support one" are the tuner working correctly, not a bug. Judge it on the
paper bankroll curve over weeks, and treat a positive paper result as a
necessary condition for going live, not a sufficient one.

## Getting the data onto your PC

The data lives on the Railway volume. `railway run` will not reach it — that
command runs on *your* machine with the service's variables injected, not
inside the container. Two ways to pull it down:

**From the dashboard.** The *Self-Tuning & Data Export* panel has a
**Download research data (.zip)** button. That is the whole thing — the panel
also shows how many labeled slots have been collected, which parameters are
currently in force (and which the tuner moved), and the last few tuning
decisions with their reasons.

**From the command line**, when you want it scripted or merged:

```bash
python fetch_data.py https://your-app.up.railway.app --token YOUR_DASH_TOKEN
```

It lands in `data/<host>/`. Analyse it with the tools already in the repo:

```bash
STATE_ROOT=data/your-app.up.railway.app python analyze.py
```

`--merge` appends the deployed rows into your local `state/` archive instead,
de-duplicated, so local and remote history become one dataset. `--full` adds
order-book recordings and the bot log (much larger).

The export is an **allowlist**, not "everything except". The same directory
holds `poly_api_creds.json`, so a blocklist would be one typo away from
shipping credentials — they are never included, and the endpoint needs
`DASH_TOKEN` like every other control route.

## Checking on it

```bash
railway logs
```

Dashboard tiles show bankroll, expectancy and the blotter. On the volume:

| File | Contents |
|---|---|
| `/data/supervisor.json` | Restart count, last labeling and tuning cycle |
| `/data/tuned_params.json` | Parameters currently in force, and why |
| `/data/archive/tuning_log.jsonl` | Every tuning decision, applied or rejected |
| `/data/archive/observations.jsonl` | The labeled learning dataset |
| `/data/archive/trades_history.jsonl` | Permanent trade archive |

Inspect the learning state without opening the dashboard:

```bash
railway run python tuner.py --status
```

## Cost

One always-on service with a small volume. The bot polls Polymarket's public
API and one websocket; there is no database and no second service.
