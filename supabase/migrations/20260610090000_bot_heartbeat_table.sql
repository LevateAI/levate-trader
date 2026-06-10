-- Shared component-liveness heartbeat table for the perp tournament bots.
-- The Polymarket services already write to this table (it was created
-- manually); this migration captures the schema idempotently and is required
-- before the perp bots' fail-loud runtime watchdog starts writing rows like
-- (balanced, market), (balanced, strategy), (balanced, equity), ...

create table if not exists bot_heartbeat (
  account_id text not null,
  component text not null,
  last_ok_at timestamptz not null,
  detail jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (account_id, component)
);

create index if not exists idx_bot_heartbeat_last_ok_at
  on bot_heartbeat (last_ok_at desc);

alter table bot_heartbeat enable row level security;

grant select on table bot_heartbeat to anon;

drop policy if exists "anon can read bot heartbeat" on bot_heartbeat;
create policy "anon can read bot heartbeat"
  on bot_heartbeat
  for select
  to anon
  using (true);
