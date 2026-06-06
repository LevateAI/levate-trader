# Levate Trader

Async-first cryptocurrency paper-trading bot for Hyperliquid. V1 trades only
`BTC-PERP` and `ETH-PERP`, defaults to local paper simulation on Hyperliquid
mainnet public market data, persists activity to Supabase, and sends
Discord/Twilio alerts.

This is paper trading infrastructure, not financial advice. Do not point this at
real-money endpoints.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env`, then run:

```bash
python -m src.main
```

## Environment Variables

`EXECUTION_MODE`: one of `paper_sim`, `testnet_real`, or `mainnet_real`.
Defaults to `paper_sim`.

`ACCOUNT_ID`: tournament account namespace. Defaults to `balanced`.

`PERSONALITY`: human-readable bot personality/config profile. Defaults to
`balanced`.

`HYPERLIQUID_PRIVATE_KEY`: API wallet private key. Optional in `paper_sim`;
required in `testnet_real`.

`HYPERLIQUID_ACCOUNT_ADDRESS`: main account address. Hyperliquid info requests
must use the main wallet address even when trading with an API wallet. Optional
in `paper_sim`; required in `testnet_real`.

`HYPERLIQUID_TESTNET`: legacy setting kept for compatibility. `EXECUTION_MODE`
controls runtime behavior.

`SUPABASE_URL` and `SUPABASE_SERVICE_KEY`: Supabase project URL and service role
key.

`DISCORD_WEBHOOK_URL`: optional Discord alert webhook.

`SMS_ALERTS_ENABLED`: set `false` to mute SMS while testing.

`TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`,
`TWILIO_TO_NUMBER`: Twilio SMS settings.

`MAX_DAILY_LOSS_PCT`, `MAX_WEEKLY_LOSS_PCT`, `MAX_DRAWDOWN_PCT`: hard breaker
thresholds as whole percentages. Defaults are `5`, `10`, and `20`.

`MAX_POSITION_SIZE_PCT`: hard cap for signal sizing as a whole percentage of
equity. Defaults to `10`.

`LEVERAGE_CAP`: maximum notional exposure multiple used by the position sizer.
Defaults to `15`.

`STRATEGIES_ENABLED`: comma-separated strategy names. For the scalp suite use
`micro_rsi_scalp,book_imbalance,volume_fade,rsi_mean_reversion,cme_gap_fill`.

`STARTING_BALANCE_USD`: used for PnL snapshots before full history is available.

`PAPER_SLIPPAGE_BPS`: simulated slippage in basis points. Defaults to `5`.

`PAPER_MAX_PENDING_ORDERS`: max pending paper limit orders. Defaults to `10`.

`SCALP_MODE_ENABLED`: set `false` to keep scalp strategies registered but muted.
Defaults to `true`.

`SCALP_MAX_HOLD_MINUTES`: max paper hold time for scalp positions. Defaults to
`15`.

`SCALP_COOLDOWN_SECONDS`: per-symbol scalp cooldown after a signal. Defaults to
`600`.

`STALE_THRESHOLD_SEC`: max allowed age for websocket-derived prices before the
bot reconnects, halts trading, and skips stale market writes. Defaults to `20`.

`CHAOS_MODE`: when `true`, wraps enabled strategies with randomized signal
skipping and sizing for the `chaos` tournament account.

`MARKET_DATA_WRITER`: when `true`, this bot writes shared
`market_data_snapshots`. In tournament mode, only one instance should set this
to `true`.

`CLOSE_POSITIONS_ON_SHUTDOWN`: reserved safety toggle for future shutdown
behavior.

### Polymarket Paper Bot

`POLYMARKET_ACCOUNT_ID`: fallback prediction-market account namespace. In the
horizon services, the runtime derives one book per asset as
`btc_5m`, `eth_5m`, `sol_5m`, `xrp_5m`, `btc_15m`, `eth_15m`, `sol_15m`, and
`xrp_15m`.

`POLYMARKET_ASSETS`: comma-separated assets for a horizon process. Defaults to
`BTC,ETH,SOL,XRP`.

`POLYMARKET_STARTING_BALANCE_USD`: starting paper balance for each
asset-by-horizon book. Defaults to `500`.

`POLYMARKET_POLL_INTERVAL_SEC`: two-feed polling cadence for Polymarket CLOB and
Coinbase spot. Defaults to `2`.

`POLYMARKET_STALE_THRESHOLD_SEC`: max feed silence before the Polymarket or
Coinbase watchdog reconnects. Defaults to `20`.

`POLYMARKET_MARKET_KEYWORDS`: comma-separated market-discovery filters for
short-duration crypto markets. Defaults to
`bitcoin,btc,ethereum,eth,solana,sol,xrp,ripple`.

`POLYMARKET_FEE_RATE_CRYPTO`: Polymarket crypto taker fee coefficient used by
the paper executor. Defaults to `0.07`.

`POLYMARKET_STRATEGIES_ENABLED`: comma-separated Polymarket strategies. Defaults
to `multi_outcome_sum_arb,latency_arb`.

`POLYMARKET_SUM_ARB_THRESHOLD`: minimum guaranteed edge per YES+NO pair after
fees for sum arbitrage. Defaults to `0.02`.

`POLYMARKET_SUM_ARB_MAX_ACCOUNT_PCT`: max account equity allocated to one
sum-arb pair. Defaults to `0.10`.

`POLYMARKET_SUM_ARB_MAX_STAKE_USD`: hard dollar cap allocated to one sum-arb
pair, regardless of inflated paper equity. Defaults to `50`.

`POLYMARKET_LATENCY_EDGE_THRESHOLD`: minimum model edge for probabilistic
Coinbase-vs-Polymarket latency arb. Defaults to `0.05`.

`POLYMARKET_LATENCY_MAX_ACCOUNT_PCT`: max account equity allocated to one
latency-arb leg. Defaults to `0.05`.

`POLYMARKET_LATENCY_MAX_STAKE_USD`: hard dollar cap allocated to one latency-arb
leg, regardless of inflated paper equity. Defaults to `25`.

`POLYMARKET_VOL_WINDOW_SEC`: rolling Coinbase spot window used for realized
volatility. Defaults to `900`.

## Execution Modes

`paper_sim` is the default. It connects to Hyperliquid mainnet public market
data in read-only mode, does not require Hyperliquid credentials, and simulates
all fills, fees, stops, take profits, balances, and PnL locally.

`testnet_real` preserves the original behavior: it connects to Hyperliquid
testnet, requires `HYPERLIQUID_PRIVATE_KEY` and
`HYPERLIQUID_ACCOUNT_ADDRESS`, and places real orders on testnet only.

`mainnet_real` is intentionally disabled in v1. Startup fails with:
`MAINNET REAL MONEY EXECUTION IS DISABLED. Set EXECUTION_MODE=paper_sim or testnet_real.`

## Database

Run the SQL migrations in order against your Supabase project before starting
the bot:

1. `supabase/migrations/001_initial_schema.sql`
2. `supabase/migrations/002_add_execution_mode.sql`
3. `supabase/migrations/003_tournament_mode.sql`
4. `supabase/migrations/20260601145523_polymarket_part1.sql`

## Tournament Mode

Tournament mode runs six isolated paper accounts against the same Hyperliquid
public market feed. Each account writes to the shared Supabase database with its
own `account_id`, so trades, positions, equity snapshots, strategy signals,
circuit breakers, and bot state do not leak across competitors.

The bundled personalities are:

- `conservative`: CME gap plus RSI mean reversion with tight risk limits.
- `balanced`: the default mixed strategy set and the only market-data writer.
- `aggressive`: mixed strategies with wider risk and leverage settings.
- `scalp_only`: micro RSI, book imbalance, and volume fade only.
- `swing_only`: CME gap plus RSI with moderate swing risk.
- `chaos`: mixed strategies with randomized signal skipping and sizing.

Template env files live under `envs/`. Fill `SUPABASE_URL` and
`SUPABASE_SERVICE_KEY` in each account file before starting services. All
tournament templates default to `EXECUTION_MODE=paper_sim`,
`STARTING_BALANCE_USD=1000`, and `SMS_ALERTS_ENABLED=false`.

Install the systemd template, then start one bot:

```bash
sudo systemctl start levate-trader@conservative
sudo journalctl -u levate-trader@conservative -f
```

Start all six:

```bash
for bot in conservative balanced aggressive scalp_only swing_only chaos; do
  sudo systemctl start "levate-trader@${bot}"
done
```

Each unit reads `/home/levateai/levate-trader/envs/%i.env` and sets
`ACCOUNT_ID=%i`.

## Polymarket Module

The Polymarket paper bot is bot #7, but it uses a separate leaderboard and
separate `polymarket_*` Supabase tables because prediction shares resolve to
`$0` or `$1`, not perp PnL.

The module reads public Polymarket CLOB books and Coinbase BTC/ETH/SOL/XRP spot
prices, joins them into synchronized snapshots, writes
`polymarket_market_snapshots`, and tracks standalone paper accounts. It has no
wallets, signing, or real-money order paths.

Discovery is restricted to short-duration crypto `Up or Down` markets with
5-minute and 15-minute ET windows. Long-dated milestone markets are rejected
before they ever reach the strategy layer. Each snapshot also stores the
per-window `price_to_beat` strike from Polymarket's public equity endpoint when
available, plus `horizon`, `window_seconds`, and seconds remaining to
resolution.

The repo includes two horizon env templates:

- `envs/polymarket_5m.env`: `POLYMARKET_HORIZON=5m`,
  `POLYMARKET_ASSETS=BTC,ETH,SOL,XRP`
- `envs/polymarket_15m.env`: `POLYMARKET_HORIZON=15m`,
  `POLYMARKET_ASSETS=BTC,ETH,SOL,XRP`

Together they drive eight isolated paper books, one account per coin and
horizon: `btc_5m`, `eth_5m`, `sol_5m`, `xrp_5m`, `btc_15m`, `eth_15m`,
`sol_15m`, and `xrp_15m`. Each book starts with `$500`; P&L never commingles
between coins or horizons.

The first two paper strategies are:

- `multi_outcome_sum_arb`: buys equal YES and NO shares only when the filled
  pair cost, including fees and walked book depth, leaves a provable settlement
  edge.
- `latency_arb`: estimates fair YES probability from Coinbase spot and recent
  realized volatility, then buys an underpriced side only when the model edge
  clears the configured threshold. This is probabilistic, not guaranteed.

Runtime liveness is work-gated. Heartbeat component names include horizon and
coin, such as `5m_btc_snapshot` and `15m_xrp_strategy`, so a single stalled
book is visible. `STALE_LIMIT_SECONDS` controls how long a component can go
without a proven successful iteration before the process exits for systemd
restart. `WATCHDOG_INTERVAL_SECONDS` controls how often those component
heartbeats are upserted to `bot_heartbeat`.

Run locally:

```bash
python -m src.polymarket.main
```

Install the separate systemd units, then start each horizon:

```bash
sudo systemctl start levate-polymarket-5m
sudo systemctl start levate-polymarket-15m
sudo journalctl -u levate-polymarket-5m -f
```

## Funding Hyperliquid Testnet

1. Create or authorize a Hyperliquid API wallet.
2. Open the Hyperliquid testnet app.
3. Deposit or faucet testnet USDC into the account.
4. Put the API private key and main account address in `.env`.

`testnet_real` refuses mainnet, but still guard your private keys like real keys.

## Strategies

`cme_gap_fill`: runs once around Sunday 22:00 UTC, compares a placeholder Friday
CME settlement with current BTC perp price, and trades toward gap closure when
the gap is over `$200`.

`rsi_mean_reversion`: checks RSI(5) on five-minute BTC/ETH bars. It goes long
below `20`, short above `80`, and uses a `0.8%` stop.

`micro_rsi_scalp`: aggregates incoming trades into one-minute bars, computes
RSI(3), and scalps extreme readings below `15` or above `85`.

`book_imbalance`: reads the top five L2 levels and scalps when bid or ask depth
is more than `3x` the other side for three consecutive ticks.

`volume_fade`: builds one-minute volume bars and fades 0.3% one-minute moves
when volume is more than `3x` the rolling 20-minute average.

## Risk Controls

Position sizing uses quarter-Kelly, volatility targeting, a 2% equity-at-risk
cap, and a 10x max leverage cap.

Circuit breakers:

- Daily loss `>= 5%`: pause new entries for 4 hours.
- Weekly loss `>= 10%`: flat-all action is logged and new entries pause for 24 hours.
- All-time drawdown `>= 20%`: flat-all action is logged and entries pause until manual reset.

Breaker state persists in Supabase `bot_state`.

## Tests

```bash
pytest
```

## Droplet Deployment

On a fresh Ubuntu droplet:

```bash
export REPO_URL=git@github.com:YOUR_ORG/YOUR_REPO.git
bash scripts/setup_droplet.sh
```

Then fill the account env files under `/home/levateai/levate-trader/envs/` and
start one or more tournament services:

```bash
sudo systemctl start levate-trader@balanced
sudo journalctl -u levate-trader@balanced -f
```

For normal deploys from your laptop:

```bash
export DROPLET_HOST=your.droplet.ip
export DROPLET_USER=levateai
bash scripts/deploy.sh
```

GitHub Actions deploys on pushes to `main` with these secrets:

- `DROPLET_HOST`
- `DROPLET_SSH_KEY`
- optional `DROPLET_USER`
