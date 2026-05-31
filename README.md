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

`CHAOS_MODE`: when `true`, wraps enabled strategies with randomized signal
skipping and sizing for the `chaos` tournament account.

`MARKET_DATA_WRITER`: when `true`, this bot writes shared
`market_data_snapshots`. In tournament mode, only one instance should set this
to `true`.

`CLOSE_POSITIONS_ON_SHUTDOWN`: reserved safety toggle for future shutdown
behavior.

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
