alter table trades
  add column if not exists account_id text not null default 'balanced';

alter table positions
  add column if not exists account_id text not null default 'balanced';

alter table equity_snapshots
  add column if not exists account_id text not null default 'balanced';

alter table strategy_signals
  add column if not exists account_id text not null default 'balanced';

alter table circuit_breaker_events
  add column if not exists account_id text not null default 'balanced';

alter table market_data_snapshots
  add column if not exists account_id text not null default 'shared';

alter table bot_state
  add column if not exists account_id text not null default 'balanced';

alter table bot_state
  drop constraint if exists bot_state_pkey;

alter table bot_state
  add constraint bot_state_pkey primary key (account_id, key);

create index if not exists idx_trades_account_timestamp
  on trades (account_id, timestamp desc);

create index if not exists idx_positions_account
  on positions (account_id);

create index if not exists idx_equity_account_timestamp
  on equity_snapshots (account_id, timestamp desc);

create table if not exists tournament_accounts (
  account_id text primary key,
  display_name text not null,
  personality text not null,
  starting_balance_usd numeric not null,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  archived_at timestamptz,
  config_snapshot jsonb not null
);

create table if not exists tournament_weekly_snapshots (
  id uuid primary key default gen_random_uuid(),
  account_id text not null references tournament_accounts(account_id),
  week_start timestamptz not null,
  week_end timestamptz not null,
  equity_start numeric not null,
  equity_end numeric not null,
  trades_count integer not null,
  win_rate double precision,
  sharpe_ratio double precision,
  max_drawdown_pct double precision,
  rank integer
);

insert into tournament_accounts (
  account_id,
  display_name,
  personality,
  starting_balance_usd,
  config_snapshot
)
values
  (
    'conservative',
    'Conservative',
    'conservative',
    1000,
    '{"strategies_enabled":"cme_gap_fill,rsi_mean_reversion","max_daily_loss_pct":5,"max_weekly_loss_pct":10,"max_drawdown_pct":15,"leverage_cap":10,"max_position_size_pct":3,"execution_mode":"paper_sim"}'::jsonb
  ),
  (
    'balanced',
    'Balanced',
    'balanced',
    1000,
    '{"strategies_enabled":"cme_gap_fill,rsi_mean_reversion,micro_rsi_scalp,book_imbalance,volume_fade","max_daily_loss_pct":15,"max_weekly_loss_pct":25,"max_drawdown_pct":40,"leverage_cap":15,"max_position_size_pct":8,"execution_mode":"paper_sim","market_data_writer":true}'::jsonb
  ),
  (
    'aggressive',
    'Aggressive',
    'aggressive',
    1000,
    '{"strategies_enabled":"cme_gap_fill,rsi_mean_reversion,micro_rsi_scalp,book_imbalance,volume_fade","max_daily_loss_pct":25,"max_weekly_loss_pct":40,"max_drawdown_pct":60,"leverage_cap":25,"max_position_size_pct":12,"execution_mode":"paper_sim"}'::jsonb
  ),
  (
    'scalp_only',
    'Scalp Only',
    'scalp_only',
    1000,
    '{"strategies_enabled":"micro_rsi_scalp,book_imbalance,volume_fade","max_daily_loss_pct":15,"max_weekly_loss_pct":25,"max_drawdown_pct":40,"leverage_cap":15,"max_position_size_pct":10,"execution_mode":"paper_sim"}'::jsonb
  ),
  (
    'swing_only',
    'Swing Only',
    'swing_only',
    1000,
    '{"strategies_enabled":"cme_gap_fill,rsi_mean_reversion","max_daily_loss_pct":10,"max_weekly_loss_pct":20,"max_drawdown_pct":30,"leverage_cap":10,"max_position_size_pct":5,"execution_mode":"paper_sim"}'::jsonb
  ),
  (
    'chaos',
    'Chaos',
    'chaos',
    1000,
    '{"strategies_enabled":"cme_gap_fill,rsi_mean_reversion,micro_rsi_scalp,book_imbalance,volume_fade","max_daily_loss_pct":30,"max_weekly_loss_pct":50,"max_drawdown_pct":70,"leverage_cap":25,"max_position_size_pct":15,"execution_mode":"paper_sim","chaos_mode":true}'::jsonb
  )
on conflict (account_id) do update set
  display_name = excluded.display_name,
  personality = excluded.personality,
  starting_balance_usd = excluded.starting_balance_usd,
  active = true,
  archived_at = null,
  config_snapshot = excluded.config_snapshot;
