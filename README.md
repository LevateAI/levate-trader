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

`STRATEGIES_ENABLED`: comma-separated strategy names. Defaults to
`cme_gap_fill,rsi_mean_reversion`.

`STARTING_BALANCE_USD`: used for PnL snapshots before full history is available.

`PAPER_SLIPPAGE_BPS`: simulated slippage in basis points. Defaults to `5`.

`PAPER_MAX_PENDING_ORDERS`: max pending paper limit orders. Defaults to `10`.

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

Then fill `/opt/levate-trader/.env` and start:

```bash
sudo systemctl start levate-trader
sudo journalctl -u levate-trader -f
```

For normal deploys from your laptop:

```bash
export DROPLET_HOST=your.droplet.ip
export DROPLET_USER=levatetrader
bash scripts/deploy.sh
```

GitHub Actions deploys on pushes to `main` with these secrets:

- `DROPLET_HOST`
- `DROPLET_SSH_KEY`
- optional `DROPLET_USER`
