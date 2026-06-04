alter table polymarket_market_snapshots
  add column if not exists price_to_beat numeric(18, 2);

alter table polymarket_market_snapshots
  add column if not exists horizon text not null default '5m',
  add column if not exists window_seconds integer not null default 300,
  add column if not exists seconds_to_resolution integer not null default 0;

alter table polymarket_trades
  add column if not exists horizon text not null default '5m',
  add column if not exists window_seconds integer not null default 300;

alter table polymarket_positions
  add column if not exists horizon text not null default '5m',
  add column if not exists window_seconds integer not null default 300;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_market_snapshots_horizon_check'
  ) then
    alter table polymarket_market_snapshots
      add constraint polymarket_market_snapshots_horizon_check
      check (horizon in ('5m', '15m'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_trades_horizon_check'
  ) then
    alter table polymarket_trades
      add constraint polymarket_trades_horizon_check
      check (horizon in ('5m', '15m'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_positions_horizon_check'
  ) then
    alter table polymarket_positions
      add constraint polymarket_positions_horizon_check
      check (horizon in ('5m', '15m'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_market_snapshots_window_seconds_check'
  ) then
    alter table polymarket_market_snapshots
      add constraint polymarket_market_snapshots_window_seconds_check
      check (window_seconds in (300, 900));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_trades_window_seconds_check'
  ) then
    alter table polymarket_trades
      add constraint polymarket_trades_window_seconds_check
      check (window_seconds in (300, 900));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_positions_window_seconds_check'
  ) then
    alter table polymarket_positions
      add constraint polymarket_positions_window_seconds_check
      check (window_seconds in (300, 900));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'polymarket_market_snapshots_seconds_remaining_check'
  ) then
    alter table polymarket_market_snapshots
      add constraint polymarket_market_snapshots_seconds_remaining_check
      check (seconds_to_resolution >= 0);
  end if;
end
$$;

create index if not exists idx_polymarket_market_snapshots_market_strike_timestamp
  on polymarket_market_snapshots (market_id, price_to_beat, timestamp desc);

create index if not exists idx_polymarket_market_snapshots_horizon_timestamp
  on polymarket_market_snapshots (horizon, timestamp desc);

create index if not exists idx_polymarket_trades_account_horizon_timestamp
  on polymarket_trades (account_id, horizon, timestamp desc);

create index if not exists idx_polymarket_positions_account_horizon_status
  on polymarket_positions (account_id, horizon, status);

insert into polymarket_accounts (
  account_id,
  display_name,
  starting_balance_usd,
  active
)
values
  ('btc_5m', 'Polymarket BTC 5m', 500, true),
  ('eth_5m', 'Polymarket ETH 5m', 500, true),
  ('sol_5m', 'Polymarket SOL 5m', 500, true),
  ('xrp_5m', 'Polymarket XRP 5m', 500, true),
  ('btc_15m', 'Polymarket BTC 15m', 500, true),
  ('eth_15m', 'Polymarket ETH 15m', 500, true),
  ('sol_15m', 'Polymarket SOL 15m', 500, true),
  ('xrp_15m', 'Polymarket XRP 15m', 500, true)
on conflict (account_id) do update set
  display_name = excluded.display_name,
  starting_balance_usd = excluded.starting_balance_usd,
  active = excluded.active;
