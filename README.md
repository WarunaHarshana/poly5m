# Polymarket BTC 5-Minute Up/Down Trading Bot

A working recreation of the *All About AI* build: an AI/quant trading bot for
Polymarket's fastest market — “Will BTC be up or down in the next 5 minutes?”
(ETH and SOL 5-minute markets work too via `ASSET` / `--asset`.)

> ⚠️ Trading real money on Polymarket carries real risk and is geo-restricted in
> some regions. Use the paper modes to validate everything first. Go live only
> after you understand the code and the risks.

## One strategy: `late_momentum` (+ a sum-to-one arb scanner)

As of 2026-07-17 the bot runs a **single trading strategy**. The old `mid`,
`late_scalp`, `late_every`, and `hybrid` strategies were retired after the
trade archive showed either negative live expectancy (rich-favorite buying
can't clear its ~92% breakeven) or paper results inflated by simulation bugs.
Any legacy `STRATEGY` value is coerced to `late_momentum` at startup.

**Late momentum** — in the last ~40–8 seconds of each 5-minute slot, if BTC has
moved at least `LATE_MOMENTUM_MIN_MOVE` (default $10) from the slot's verified
Chainlink strike, buy the side the move points to, subject to an entry band,
an edge floor *and ceiling*, and book-quality gates (details below).

**Sum-to-one arb** — alongside momentum, every analyzed slot is scanned: when
`ask_UP + ask_DOWN < 1 − ARB_MIN_EDGE`, buying **both** sides pays $1/share
whichever way the slot resolves — structural edge, no prediction needed.
Arb execution is **paper-only**: a live arb needs both legs to fill (leg
risk), which is not handled, so in `live` mode arb signals are logged only.

## Three modes

| Mode | BTC price data | Market quotes | Fills | Wallet needed? |
|------|----------------|---------------|-------|----------------|
| `paper` | Simulated random walk | Synthetic | Simulated | No |
| `paper_live` | **Real** (Chainlink stream/Pyth/Coinbase/Binance/Kraken) | **Real** Polymarket book | Simulated | **No** |
| `live` | Real | Real | **REAL ORDERS** | Yes (funded) |

`paper_live` is the sweet spot for testing: the bot reacts to **genuine live BTC
prices and real Polymarket order books**, but only *simulates* the fills — so
nothing is at stake and **no wallet or private key is required**.

## Files

| File | Role |
|------|------|
| `config.py` | All settings + secrets from `.env` |
| `price_feed.py` | Live BTC spot (Chainlink Data Stream first, multi-source fallback) **or** offline simulator |
| `signal_engine.py` | Drift/diffusion CDF fair value → model probability for the momentum side |
| `risk.py` | Edge-scaled sizing, exposure cap, daily-loss circuit breaker |
| `polymarket_client.py` | Public read-only client (no auth) + live CLOB V2 trading client |
| `executor.py` | Paper fills and live order routing |
| `bot.py` | Main loop: `run_sim` (offline) + `run_realtime` (real data, background settlement) |
| `discover.py` | Prints the current auto-discovered 5-min market (debugging) |
| `app.py` | **All-in-one dashboard server + control API** (start/stop the bot from the browser) |
| `_selftest_realtime.py` / `_selftest_discovery.py` / `_selftest_pricing.py` | Offline self-tests (network stubbed) |

## Quick start

### 1. Offline paper sim (no deps, no network)

```bash
python3 bot.py --mode paper --rounds 200
python3 app.py                   # then open http://localhost:8000 to view the run
```

### 2. Real data, paper money (recommended test)

```bash
pip install -r requirements.txt
cp .env.example .env             # defaults to MODE=paper_live; no key needed
```

**Nothing to paste — token IDs are discovered automatically.** At the start of
every round the bot computes the current 5-minute slot and fetches that market
from Polymarket's Gamma API by its deterministic slug
(`btc-updown-5m-<unix>`, or `eth-`/`sol-` when `ASSET` is changed), so it
always trades the live slot with fresh CLOB token ids and no manual updating.
(If your region renames the series, set `BTC_SLUG_PREFIX` /
`BTC_MARKET_MATCH` in `.env`; run `python3 discover.py` to inspect what the
API returns.)

Then just launch the all-in-one dashboard:

```bash
python3 app.py                   # opens http://localhost:8000 automatically
```

The dashboard has a control panel — pick the mode, asset, starting balance,
bet size, and rounds, then hit **▶ Start**. It launches `bot.py` for you,
streams the live log, and updates the tiles/blotter every few seconds.
Strategy settings live in four tabs — **Late Momentum / Arbitrage / Risk /
General** — and are written back to `.env` (never secrets). You can still run
`python3 bot.py --mode paper_live --rounds 24` directly in a terminal;
`app.py` will display whatever run is active.

Each slot the bot waits for the late window (~last 40s), verifies the
**Chainlink Data Streams strike** for the slot open, measures how far BTC has
moved from it, and — if the move, entry band, and edge gates all pass — buys
the momentum side at the **real best ask** (plus simulated slippage in paper).
The trade then **settles in a background task** against Polymarket's official
outcome while the main loop moves on to the next slot; a slot that already has
an open trade is never re-entered. Meanwhile the arb scanner re-checks the
book (roughly every 15s while waiting) for sum-to-one opportunities.

### 3. Go live (real money, at your own risk)

**There is no profit guarantee.** The bot only limits *how much* you can lose
per trade/day. Expect to skip most slots; that is intentional — most slots
never produce a $10+ verified move inside the late window.

Before live:
1. Run `paper_live` for many slots and check win rate **and expectancy**.
2. Fund a wallet you can afford to lose completely.
3. In `.env`:

```bash
MODE=live
PRIVATE_KEY=0x...
FUNDER_ADDRESS=0x...
LIVE_CONFIRM=yes          # required kill-switch
FIXED_STAKE_USD=1         # $1 only
DAILY_LOSS_LIMIT_USD=5
MAX_TRADES_PER_DAY=10
```

```bash
# fund with pUSD (+ POL for gas if EOA/type-0)
python3 bot.py --mode live --rounds 5 --bet 1
```

Live mode auto-applies a **safety profile**: $1 fixed stakes, tightened loss
limits, Chainlink-stream-first prices (Polymarket’s resolution oracle), cancel
unfilled orders at slot end, consecutive-loss halt. Live also checks the
**actual matched fill size** from the Polymarket API before booking a trade,
so unfilled orders don't pollute your stats. Arb stays log-only in live.

For safety the dashboard Start button only launches `paper` / `paper_live`;
real-money `live` stays command-line only.

## Start fresh / set your own paper balance and bet

These flags work in `paper` and `paper_live`, either on the CLI or via the
dashboard control panel:

```bash
python3 bot.py --mode paper_live --rounds 24 --reset --bankroll 500 --bet 10
```

| Flag | Effect |
|------|--------|
| `--reset` | Wipes previous trades/stats/status so the dashboard starts empty (permanent archive in `state/archive/` is kept) |
| `--bankroll N` | Sets your starting paper balance to `N` (no wallet, no money) |
| `--bet N` | Stakes a **fixed** `N` per trade and raises the per-strategy stake caps to match in paper modes |
| `--asset btc\|eth\|sol` | Trade a different 5-minute series (default from `ASSET` in `.env`) |

> ⚖️ **Scale your risk to your bankroll.** The circuit breaker halts trading
> once the day's loss hits `DAILY_LOSS_LIMIT_USD` (default **$5**) — on a large
> balance that trips almost immediately. Raise it and the trade sizes
> (`BASE_TRADE_USD`, `MAX_TRADE_USD`, `MAX_EXPOSURE_USD`) in `.env` to match.

## How a momentum trade is gated

Every gate must pass, in order, inside the late window
(`LATE_MOMENTUM_TIME_LIMIT` → `LATE_MOMENTUM_MIN_REMAINING` seconds left,
default 40 → 8):

1. **Verified strike** — the slot-open price must come from the Chainlink
   Data Stream (Polymarket's resolution source). Exchange-proxy strikes are
   refused when `LATE_SCALP_REQUIRE_CHAINLINK=1` (default): a wrong strike
   produces fake signals *and* wrong local resolution.
2. **Move** — `|spot − strike| ≥ LATE_MOMENTUM_MIN_MOVE` (default $10).
   Direction picks the side; DOWN entries require
   `LATE_MOMENTUM_BIDIRECTIONAL=1` (default).
3. **Entry band** — the best ask must be inside
   `[LATE_MOMENTUM_MIN_ENTRY, LATE_MOMENTUM_MAX_ENTRY]` (default 0.55–0.92).
   Archive data: entries under ~0.92 carried the profit; 0.95+ favorites were
   net losers even at a 93% win rate.
4. **Book quality** — spread ≤ `LATE_SCALP_MAX_SPREAD`, then a **requote**
   (`LATE_SCALP_REQUOTE=1`) re-checks the book and skips if the ask jumped
   more than `LATE_SCALP_MAX_ASK_JUMP`.
5. **Edge floor + ceiling** — edge = model probability (drift/diffusion CDF
   fair value) minus ask. It must be ≥ `LATE_MOMENTUM_MIN_EDGE` (default 0 =
   off) **and** ≤ `LATE_MOMENTUM_MAX_EDGE` (default 0.10). The ceiling is the
   unusual part: archive buckets showed edge ≥ 0.15 was the *worst* performer —
   a huge "model edge" in the last minute usually means a stale strike or vol
   misestimate, and the book is right.
6. **Sizing** — edge-scaled fractional Kelly (`LATE_SCALP_USE_EDGE_SIZING=1`)
   capped by `LATE_SCALP_MAX_STAKE_USD`; shares are capped at half the visible
   ask depth and must clear the venue minimum (`MIN_ORDER_SHARES`).

Paper fills are **pessimistic**: entry pays ask + `PAPER_FILL_SLIPPAGE`
(default 0.01), because thin end-of-slot books slip the most. After a loss the
bot skips the next `LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS` late windows. A time
guard also skips whole slots during historically chaotic hours (US/London
opens).

## Arb settings

| Key | Default | Meaning |
|-----|---------|---------|
| `ARB_TRADE_ENABLED` | `1` | Scan + paper-execute sum-to-one arbs (live: log-only) |
| `ARB_MIN_EDGE` | `0.02` | Required `1 − (ask_UP + ask_DOWN)` before slippage |
| `ARB_MAX_STAKE_USD` | `10.0` | Total stake for both legs of one pair |
| `ARB_MIN_REMAINING` | `20` | Don't take arbs with less than this many seconds left |

One attempt per slot; the pair must still be under $1 after slippage on both
legs, and shares are depth-capped at half the thinner ask level. The locked-in
PnL is `shares × (1 − pair_cost)` regardless of the outcome.

## Data-quality guard (important)

When a 5-minute slot has just opened, its order book can be empty or one-sided,
and the exchange may report a missing/zero midpoint. The bot **never trades a
degenerate quote**: `midpoint()` falls back to the real book mid
`(best_bid + best_ask) / 2` and raises if no sane `(0, 1)` price exists, and the
trade loop skips any slot whose price falls outside the tradeable band
`[MIN_QUOTE, MAX_QUOTE]` (defaults 0.02–0.98). This prevents the pathological
“buy at ~$0.00 → impossible payout” fills that would otherwise inflate the
bankroll.

## Self-tests (offline, no network)

```bash
python3 _selftest_realtime.py    # full realtime loop, feeds/Gamma stubbed:
                                 #   Phase A: late_momentum must trade and win
                                 #   Phase B: arb must place pairs with locked-in PnL
python3 _selftest_discovery.py   # real Gamma/CLOB parsing against a fake HTTP
                                 #   layer; asserts the exact market shape the
                                 #   bot expects (tokens, end_ts, conditionId)
python3 _selftest_pricing.py     # quote validation / degenerate-book rejection
                                 #   (the "$2000 bankroll bug" regression test)
```

The realtime self-test isolates all state writes to a temp directory, so it
never pollutes the dashboard files or the permanent trade archive.

## Live risk model (minimal capital)

| Guard | Default live behavior |
|-------|------------------------|
| Stake | Fixed **$1** (no Kelly scale-up) |
| Exposure | **1 open trade** at a time |
| Daily loss | **$5** and/or **5%** of bankroll (tighter wins) |
| Streak | Stop after **3** losses in a row |
| Window | Only the late window (**~40–8s** left) |
| Move | Verified Chainlink strike + **$10+** move required |
| Entry band | Only asks in **0.55–0.92** |
| Edge | Floor `LATE_MOMENTUM_MIN_EDGE`, **ceiling** `LATE_MOMENTUM_MAX_EDGE` (0.10) |
| Book | Spread guard + requote + depth cap + venue min shares |
| Price | **Chainlink Data Stream first** (matches resolution) |
| Orders | GTC at ask; **cancel** leftovers at slot end; fills verified via API |
| Arb | **Log-only** (no live execution — leg risk) |
| Confirm | `LIVE_CONFIRM=yes` required |

## Honest note

The signal is still simple. A >50% win rate can still lose money (buying near
$0.90 pays poorly on wins), and the retired strategies are proof: they looked
fine on paper until the archive and simulation-bug fixes said otherwise.
**This is not a money-printer.** The arb scanner's locked-in edges are real but
rare and paper-only. Use tiny stakes, treat live as a paid experiment, and stop
if expectancy stays negative in `paper_live`.
