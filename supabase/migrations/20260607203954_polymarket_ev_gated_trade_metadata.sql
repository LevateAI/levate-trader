alter table public.polymarket_trades
    add column if not exists p_model numeric,
    add column if not exists edge_at_entry numeric,
    add column if not exists fee_paid numeric,
    add column if not exists entry_reason_code text;

create index if not exists idx_polymarket_trades_ev_gated_metadata
    on public.polymarket_trades (account_id, timestamp desc)
    where strategy_name = 'ev_gated';
