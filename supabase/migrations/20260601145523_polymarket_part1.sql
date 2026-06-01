create table if not exists polymarket_accounts (
  account_id text primary key,
  display_name text not null,
  starting_balance_usd numeric(18, 2) not null default 500,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists polymarket_market_snapshots (
  id uuid primary key default gen_random_uuid(),
  timestamp timestamptz not null,
  created_at timestamptz not null default now(),
  market_id text not null,
  market_question text not null,
  yes_price numeric(10, 4) not null check (yes_price >= 0 and yes_price <= 1),
  no_price numeric(10, 4) not null check (no_price >= 0 and no_price <= 1),
  yes_book_depth numeric(18, 6) not null default 0,
  no_book_depth numeric(18, 6) not null default 0,
  coinbase_ref_price numeric(18, 2) not null,
  implied_gap numeric(10, 4) not null,
  resolution_time timestamptz
);

create index if not exists idx_polymarket_market_snapshots_timestamp
  on polymarket_market_snapshots (timestamp desc);
create index if not exists idx_polymarket_market_snapshots_market_timestamp
  on polymarket_market_snapshots (market_id, timestamp desc);

create table if not exists polymarket_positions (
  id uuid primary key default gen_random_uuid(),
  account_id text not null references polymarket_accounts(account_id),
  timestamp timestamptz not null,
  market_id text not null,
  side text not null check (side in ('YES', 'NO')),
  shares numeric(18, 6) not null check (shares >= 0),
  avg_entry_price numeric(10, 4) not null check (
    avg_entry_price >= 0 and avg_entry_price <= 1
  ),
  current_price numeric(10, 4) not null check (
    current_price >= 0 and current_price <= 1
  ),
  unrealized_pnl numeric(18, 2) not null default 0,
  status text not null check (status in ('open', 'closed', 'resolved')),
  resolution_outcome text check (resolution_outcome in ('YES', 'NO')),
  created_at timestamptz not null default now()
);

create index if not exists idx_polymarket_positions_account_status
  on polymarket_positions (account_id, status);
create index if not exists idx_polymarket_positions_market
  on polymarket_positions (market_id);

create table if not exists polymarket_trades (
  id uuid primary key default gen_random_uuid(),
  account_id text not null references polymarket_accounts(account_id),
  timestamp timestamptz not null,
  market_id text not null,
  strategy_name text not null,
  side text not null check (side in ('YES', 'NO')),
  shares numeric(18, 6) not null check (shares >= 0),
  entry_price numeric(10, 4) not null check (entry_price >= 0 and entry_price <= 1),
  exit_price numeric(10, 4) check (exit_price >= 0 and exit_price <= 1),
  pnl_usd numeric(18, 2),
  status text not null check (status in ('open', 'closed', 'resolved')),
  reason_entry text,
  reason_exit text,
  created_at timestamptz not null default now()
);

create index if not exists idx_polymarket_trades_account_timestamp
  on polymarket_trades (account_id, timestamp desc);
create index if not exists idx_polymarket_trades_market
  on polymarket_trades (market_id);

create table if not exists polymarket_equity_snapshots (
  account_id text not null references polymarket_accounts(account_id),
  timestamp timestamptz not null default now(),
  balance_usd numeric(18, 2) not null,
  equity_usd numeric(18, 2) not null,
  open_position_count integer not null,
  created_at timestamptz not null default now(),
  primary key (account_id, timestamp)
);

create index if not exists idx_polymarket_equity_account_timestamp
  on polymarket_equity_snapshots (account_id, timestamp desc);

insert into polymarket_accounts (
  account_id,
  display_name,
  starting_balance_usd,
  active
)
values (
  'polymarket_crypto',
  'Polymarket Crypto Arb',
  500,
  true
)
on conflict (account_id) do update set
  display_name = excluded.display_name,
  starting_balance_usd = excluded.starting_balance_usd,
  active = excluded.active;

alter table polymarket_accounts enable row level security;
alter table polymarket_market_snapshots enable row level security;
alter table polymarket_positions enable row level security;
alter table polymarket_trades enable row level security;
alter table polymarket_equity_snapshots enable row level security;

grant select on table
  polymarket_accounts,
  polymarket_market_snapshots,
  polymarket_positions,
  polymarket_trades,
  polymarket_equity_snapshots
to anon;

create policy "anon can read polymarket accounts"
  on polymarket_accounts
  for select
  to anon
  using (true);

create policy "anon can read polymarket market snapshots"
  on polymarket_market_snapshots
  for select
  to anon
  using (true);

create policy "anon can read polymarket positions"
  on polymarket_positions
  for select
  to anon
  using (true);

create policy "anon can read polymarket trades"
  on polymarket_trades
  for select
  to anon
  using (true);

create policy "anon can read polymarket equity snapshots"
  on polymarket_equity_snapshots
  for select
  to anon
  using (true);
