"""Read-only per-strategy expectancy report from the trades table.

Surfaces, per strategy_name: trade count, win rate, avg win, avg loss,
avg fees, net expectancy per trade, and total PnL. Reporting only — it
never writes and never changes how trades are decided.

Usage:
    python scripts/strategy_expectancy.py
    python scripts/strategy_expectancy.py --account-id balanced --days 30
    python scripts/strategy_expectancy.py --by-account --execution-mode paper_sim
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from dotenv import load_dotenv
from supabase import Client, create_client

PAGE_SIZE = 1000


@dataclass
class StrategyStats:
    """Aggregated closed-trade outcomes for one strategy."""

    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    total_pnl_usd: float = 0.0
    total_win_usd: float = 0.0
    total_loss_usd: float = 0.0
    total_fees_usd: float = 0.0
    accounts: set[str] = field(default_factory=set)

    def add(self, pnl_usd: float, fees_usd: float, account_id: str) -> None:
        self.trade_count += 1
        self.total_pnl_usd += pnl_usd
        self.total_fees_usd += fees_usd
        self.accounts.add(account_id)
        if pnl_usd > 0:
            self.win_count += 1
            self.total_win_usd += pnl_usd
        elif pnl_usd < 0:
            self.loss_count += 1
            self.total_loss_usd += pnl_usd

    @property
    def win_rate(self) -> float:
        return self.win_count / self.trade_count if self.trade_count else 0.0

    @property
    def avg_win_usd(self) -> float:
        return self.total_win_usd / self.win_count if self.win_count else 0.0

    @property
    def avg_loss_usd(self) -> float:
        return self.total_loss_usd / self.loss_count if self.loss_count else 0.0

    @property
    def avg_fees_usd(self) -> float:
        return self.total_fees_usd / self.trade_count if self.trade_count else 0.0

    @property
    def expectancy_usd(self) -> float:
        """Mean realized PnL per closed trade (fees already included in pnl)."""
        return self.total_pnl_usd / self.trade_count if self.trade_count else 0.0


def fetch_closed_trades(
    client: Client,
    execution_mode: str,
    account_id: str | None,
    since_iso: str | None,
) -> list[dict[str, Any]]:
    """Fetch closed trade rows page by page."""
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        query = (
            client.table("trades")
            .select("strategy_name,account_id,symbol,pnl_usd,fees_usd,timestamp,status")
            .eq("status", "closed")
            .eq("execution_mode", execution_mode)
        )
        if account_id:
            query = query.eq("account_id", account_id)
        if since_iso:
            query = query.gte("timestamp", since_iso)
        response = (
            query.order("timestamp", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        page = [row for row in (response.data or []) if isinstance(row, dict)]
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def aggregate(
    rows: list[dict[str, Any]],
    by_account: bool,
) -> dict[str, StrategyStats]:
    """Group closed trades per strategy (optionally split per account)."""
    stats: dict[str, StrategyStats] = {}
    skipped = 0
    for row in rows:
        raw_pnl = row.get("pnl_usd")
        if raw_pnl is None:
            skipped += 1
            continue
        strategy_name = str(row.get("strategy_name") or "unknown")
        account = str(row.get("account_id") or "unknown")
        key = f"{strategy_name} [{account}]" if by_account else strategy_name
        stats.setdefault(key, StrategyStats()).add(
            pnl_usd=float(raw_pnl),
            fees_usd=float(row.get("fees_usd") or 0.0),
            account_id=account,
        )
    if skipped:
        print(f"note: skipped {skipped} closed trades with null pnl_usd", file=sys.stderr)
    return stats


def render(stats: dict[str, StrategyStats]) -> str:
    """Render an aligned plain-text expectancy table."""
    header = (
        f"{'strategy':<38} {'trades':>7} {'win%':>7} {'avg win':>10} "
        f"{'avg loss':>10} {'avg fees':>9} {'expectancy':>11} {'total pnl':>11} {'accts':>6}"
    )
    lines = [header, "-" * len(header)]
    ordered = sorted(stats.items(), key=lambda item: item[1].expectancy_usd)
    for name, strategy_stats in ordered:
        lines.append(
            f"{name:<38} "
            f"{strategy_stats.trade_count:>7d} "
            f"{strategy_stats.win_rate:>6.1%} "
            f"{strategy_stats.avg_win_usd:>10.2f} "
            f"{strategy_stats.avg_loss_usd:>10.2f} "
            f"{strategy_stats.avg_fees_usd:>9.3f} "
            f"{strategy_stats.expectancy_usd:>11.3f} "
            f"{strategy_stats.total_pnl_usd:>11.2f} "
            f"{len(strategy_stats.accounts):>6d}"
        )
    total = StrategyStats()
    for strategy_stats in stats.values():
        total.trade_count += strategy_stats.trade_count
        total.win_count += strategy_stats.win_count
        total.loss_count += strategy_stats.loss_count
        total.total_pnl_usd += strategy_stats.total_pnl_usd
        total.total_win_usd += strategy_stats.total_win_usd
        total.total_loss_usd += strategy_stats.total_loss_usd
        total.total_fees_usd += strategy_stats.total_fees_usd
        total.accounts |= strategy_stats.accounts
    lines.append("-" * len(header))
    lines.append(
        f"{'ALL':<38} "
        f"{total.trade_count:>7d} "
        f"{total.win_rate:>6.1%} "
        f"{total.avg_win_usd:>10.2f} "
        f"{total.avg_loss_usd:>10.2f} "
        f"{total.avg_fees_usd:>9.3f} "
        f"{total.expectancy_usd:>11.3f} "
        f"{total.total_pnl_usd:>11.2f} "
        f"{len(total.accounts):>6d}"
    )
    return "\n".join(lines)


def main() -> int:
    """Print the per-strategy expectancy table for closed trades."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", default=None, help="filter to one tournament account")
    parser.add_argument("--execution-mode", default="paper_sim")
    parser.add_argument("--days", type=float, default=None, help="lookback window in days")
    parser.add_argument(
        "--by-account",
        action="store_true",
        help="split rows per strategy+account instead of per strategy",
    )
    args = parser.parse_args()

    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set", file=sys.stderr)
        return 2

    since_iso = (
        (datetime.now(tz=UTC) - timedelta(days=args.days)).isoformat()
        if args.days is not None
        else None
    )
    client = create_client(supabase_url, supabase_key)
    rows = fetch_closed_trades(client, args.execution_mode, args.account_id, since_iso)
    if not rows:
        print("no closed trades found for the given filters")
        return 0
    stats = aggregate(rows, by_account=args.by_account)
    window = f"last {args.days:g} days" if args.days is not None else "all time"
    scope = args.account_id or "all accounts"
    print(
        f"strategy expectancy | mode={args.execution_mode} | {scope} | {window} | "
        f"{len(rows)} closed trades\n"
    )
    print(render(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
