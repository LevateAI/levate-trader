create extension if not exists pgcrypto;

create table if not exists trades (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  strategy_name text not null,
  symbol text not null,
  side text not null check (side in ('buy', 'sell', 'long', 'short')),
  size numeric not null check (size >= 0),
  entry_price numeric(18, 2) not null,
  exit_price numeric(18, 2),
  pnl_usd numeric(18, 2),
  pnl_pct double precision,
  fees_usd numeric(18, 2) not null default 0,
  hold_duration_sec integer,
  reason_entry text,
  reason_exit text,
  regime text,
  status text not null check (status in ('open', 'closed')),
  created_at timestamptz not null default now()
);
create index if not exists idx_trades_timestamp on trades (timestamp desc);

create table if not exists positions (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  symbol text not null,
  side text not null check (side in ('buy', 'sell', 'long', 'short')),
  size numeric not null check (size >= 0),
  entry_price numeric(18, 2) not null,
  liquidation_price numeric(18, 2),
  unrealized_pnl numeric(18, 2) not null default 0,
  leverage numeric not null check (leverage > 0),
  strategy_name text not null,
  stop_loss numeric(18, 2),
  take_profit numeric(18, 2),
  created_at timestamptz not null default now()
);
create index if not exists idx_positions_timestamp on positions (timestamp desc);

create table if not exists equity_snapshots (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  balance_usd numeric(18, 2) not null,
  equity_usd numeric(18, 2) not null,
  margin_used_usd numeric(18, 2) not null,
  open_position_count integer not null,
  daily_pnl numeric(18, 2) not null,
  weekly_pnl numeric(18, 2) not null,
  mdd_pct double precision not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_equity_snapshots_timestamp on equity_snapshots (timestamp desc);

create table if not exists strategy_signals (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  strategy_name text not null,
  symbol text not null,
  signal_type text not null,
  signal_strength double precision not null,
  features jsonb not null default '{}'::jsonb,
  action_taken text,
  created_at timestamptz not null default now()
);
create index if not exists idx_strategy_signals_timestamp on strategy_signals (timestamp desc);

create table if not exists circuit_breaker_events (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  breaker_type text not null,
  threshold_value double precision not null,
  observed_value double precision not null,
  action text,
  created_at timestamptz not null default now()
);
create index if not exists idx_circuit_breaker_events_timestamp on circuit_breaker_events (timestamp desc);

create table if not exists market_data_snapshots (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  symbol text not null,
  bid numeric(18, 2) not null,
  ask numeric(18, 2) not null,
  mid numeric(18, 2) not null,
  last_trade_price numeric(18, 2) not null,
  volume_24h numeric(18, 2),
  funding_rate double precision,
  open_interest numeric(18, 2),
  created_at timestamptz not null default now()
);
create index if not exists idx_market_data_snapshots_timestamp on market_data_snapshots (timestamp desc);

create table if not exists bot_state (
  key text primary key,
  value jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
