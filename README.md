# Polymarket BTC 5-Minute Up/Down Trading Bot

A working recreation of the *All About AI* build: an AI/quant trading bot for
Polymarket's fastest market — “Will BTC be up or down in the next 5 minutes?”

> ⚠️ Trading real money on Polymarket carries real risk and is geo-restricted in
> some regions. Use the paper modes to validate everything first. Go live only
> after you understand the code and the risks.

## Three modes

| Mode | BTC price data | Market quotes | Fills | Wallet needed? |
|------|----------------|---------------|-------|----------------|
| `paper` | Simulated random walk | Synthetic | Simulated | No |
| `paper_live` | **Real** (Pyth/Coinbase/Binance/Kraken/Chainlink) | **Real** Polymarket book | Simulated | **No** |
| `live` | Real | Real | **REAL ORDERS** | Yes (funded) |

`paper_live` is the sweet spot for testing: the bot reacts to **genuine live BTC
prices and real Polymarket order-book midpoints**, but only *simulates* the
fills — so nothing is at stake and **no wallet or private key is required**.

## Files

| File | Role |
|------|------|
| `config.py` | All settings + secrets from `.env` |
| `price_feed.py` | Live BTC spot (multi-source, incl. 5-min candle open) **or** offline simulator |
| `signal_engine.py` | Momentum + drift/diffusion CDF → UP/DOWN + model probability |
| `risk.py` | Fractional-Kelly sizing, exposure cap, daily-loss circuit breaker |
| `polymarket_client.py` | Public read-only client (no auth) + live CLOB V2 trading client |
| `executor.py` | Paper fills and live order routing |
| `bot.py` | Main loop: `run_sim` (offline) + `run_realtime` (real data) |
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
(`btc-updown-5m-<unix>`), so it always trades the live slot with fresh CLOB
token ids and no manual updating. (If your region renames the series, set
`BTC_SLUG_PREFIX` / `BTC_MARKET_MATCH` in `.env`; run `python3 discover.py` to
inspect what the API returns.)

Then just launch the all-in-one dashboard:

```bash
python3 app.py                   # opens http://localhost:8000 automatically
```

The dashboard has a control panel — pick the mode, starting balance, bet size,
and rounds, then hit **▶ Start**. It launches `bot.py` for you, streams the live
log, and updates the tiles/blotter every few seconds. (You can still run
`python3 bot.py --mode paper_live --rounds 24` directly in a terminal if you
prefer; `app.py` will display whatever run is active.)

Each round the bot samples real BTC prices for ~1 minute, decides UP/DOWN,
fetches the **verified 5-minute candle open** as the strike, pulls the **real
order-book midpoint** for that side, sizes with fractional Kelly, records a
simulated fill, waits the real 5 minutes, then resolves the round against the
actual BTC move.

### 3. Go live (real money, at your own risk)

**There is no profit guarantee.** The bot only limits *how much* you can lose
per trade/day. Expect to skip most slots; that is intentional.

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

Live mode auto-applies a **safety profile**: $1 fixed stakes, high edge/EV
thresholds, Chainlink-first prices (Polymarket’s resolution oracle), mid-slot
trade window only, cancel unfilled orders at slot end, consecutive-loss halt.

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
| `--reset` | Wipes previous trades/stats/status so the dashboard starts empty |
| `--bankroll N` | Sets your starting paper balance to `N` (no wallet, no money) |
| `--bet N` | Stakes a **fixed** `N` per trade. Omit it to let fractional Kelly size each bet between `BASE_TRADE_USD` and `MAX_TRADE_USD` |

> ⚖️ **Scale your risk to your bankroll.** The circuit breaker halts trading
> once the day's loss hits `DAILY_LOSS_LIMIT_USD` (default **$15**) — on a large
> balance that trips almost immediately. Raise it and the trade sizes
> (`BASE_TRADE_USD`, `MAX_TRADE_USD`, `MAX_EXPOSURE_USD`) in `.env` to match.

## The signal

With a verified slot strike price and the seconds left in the slot, the engine
uses a drift/diffusion (random-walk CDF) model: it estimates how likely BTC is
to finish above the strike given the current distance, a volatility-scaled
standard deviation, and a short-term drift term. Volatility is a blend of the
configured daily vol (`VOLATILITY_DAILY`) and realized vol from the warmup
window (`VOL_BLEND`). Low sample quality pulls probabilities toward 0.5
(confidence haircut). When strike/time aren't available it falls back to the
original short-term momentum sigmoid.

## Sizing and fills

- **Executable price:** sizing and paper fills use the **best ask** via
  `buy_quote()` (not the midpoint), so results aren't optimistically filled at
  mid.
- **Edge / EV filters:** trade only if confidence-adjusted edge ≥
  `EDGE_THRESHOLD` and expected value ≥ `MIN_EV`.
- **Kelly:** correct binary form `f* = (q − p) / (1 − p)`, scaled by
  `KELLY_FRACTION` and signal confidence, then clamped to USD caps.
- **Spread guard:** skip books wider than `MAX_SPREAD`.

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
python3 _selftest_realtime.py    # full real-data loop with feeds stubbed
python3 _selftest_discovery.py   # slug-based market discovery + token mapping
python3 _selftest_pricing.py     # quote validation / bad-book rejection
```

## Live risk model (minimal capital)

| Guard | Default live behavior |
|-------|------------------------|
| Stake | Fixed **$1** (no Kelly scale-up) |
| Exposure | **1 open trade** at a time |
| Daily loss | **$5** and/or **5%** of bankroll (tighter wins) |
| Streak | Stop after **3** losses in a row |
| Edge | Model − ask − fee buffer ≥ **~6%** (live profile) |
| Entry band | Only buys asks in roughly **0.22–0.65** (stricter than paper) |
| Book | Spread + minimum ask depth |
| Window | Only mid-slot (~90–220s left) |
| Price | **Chainlink first** (matches resolution) |
| Orders | GTC at ask; **cancel** leftovers at slot end |
| Confirm | `LIVE_CONFIRM=yes` required |

## Honest note

The signal is still simple. A >50% win rate can still lose money (buying near
$0.60 pays poorly on wins). **This is not a money-printer.** Use tiny stakes,
treat live as a paid experiment, and stop if expectancy stays negative in
`paper_live`.
