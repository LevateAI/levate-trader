alter table trades
  add column if not exists execution_mode text not null default 'paper_sim';

alter table positions
  add column if not exists execution_mode text not null default 'paper_sim';

alter table equity_snapshots
  add column if not exists execution_mode text not null default 'paper_sim';

create index if not exists idx_trades_execution_mode_timestamp
  on trades (execution_mode, timestamp desc);

create index if not exists idx_positions_execution_mode_timestamp
  on positions (execution_mode, timestamp desc);

create index if not exists idx_equity_snapshots_execution_mode_timestamp
  on equity_snapshots (execution_mode, timestamp desc);
