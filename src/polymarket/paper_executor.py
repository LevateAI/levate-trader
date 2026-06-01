"""Polymarket prediction-share paper execution engine."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import structlog

from src.polymarket.models import (
    PolymarketBookLevel,
    PolymarketOrderBook,
    PolymarketPosition,
    PolymarketPositionStatus,
    PolymarketSide,
    PolymarketTrade,
    PolymarketTradeStatus,
    fee_for_trade,
)

logger = structlog.get_logger(__name__)


class PolymarketPaperExecutor:
    """Simulate USDC-settled prediction-share fills locally."""

    def __init__(
        self,
        account_id: str,
        starting_balance_usd: float = 500.0,
        taker_fee_rate: float = 0.07,
        fees_enabled: bool = True,
    ) -> None:
        self.account_id = account_id
        self.balance_usd = _money(starting_balance_usd)
        self.taker_fee_rate = taker_fee_rate
        self.fees_enabled = fees_enabled
        self.positions: dict[tuple[str, PolymarketSide], PolymarketPosition] = {}
        self.trades: dict[str, PolymarketTrade] = {}

    @property
    def equity_usd(self) -> float:
        """Return current paper equity including marked open positions."""
        open_value = sum(
            position.current_price * position.shares
            for position in self.positions.values()
            if position.status == PolymarketPositionStatus.OPEN
        )
        return _money(self.balance_usd + open_value)

    async def open_position(
        self,
        market_id: str,
        side: PolymarketSide,
        requested_shares: float,
        order_book: PolymarketOrderBook,
        strategy_name: str,
        reason_entry: str,
    ) -> PolymarketTrade:
        """Buy shares from the ask book, filling only available depth."""
        if requested_shares <= 0:
            raise ValueError("requested shares must be positive")
        if order_book.side != side:
            raise ValueError("order book side does not match requested side")

        filled_shares, gross_cost = _walk_book(order_book.asks, requested_shares)
        if filled_shares <= 0:
            raise ValueError("no ask liquidity available for paper fill")
        avg_entry_price = gross_cost / filled_shares
        fee = self._fee(filled_shares, avg_entry_price)
        total_cost = gross_cost + fee
        if total_cost > self.balance_usd:
            affordable_shares = _affordable_shares(
                order_book.asks,
                self.balance_usd,
                self.taker_fee_rate,
            )
            filled_shares, gross_cost = _walk_book(order_book.asks, affordable_shares)
            if filled_shares <= 0:
                raise ValueError("paper balance cannot afford available shares")
            avg_entry_price = gross_cost / filled_shares
            fee = self._fee(filled_shares, avg_entry_price)
            total_cost = gross_cost + fee

        self.balance_usd = _money(self.balance_usd - total_cost)
        key = (market_id, side)
        existing = self.positions.get(key)
        if existing is None:
            position = PolymarketPosition.open(
                account_id=self.account_id,
                market_id=market_id,
                side=side,
                shares=filled_shares,
                avg_entry_price=avg_entry_price,
                fees_paid=fee,
            )
            self.positions[key] = position
        else:
            new_shares = existing.shares + filled_shares
            new_cost = existing.avg_entry_price * existing.shares + gross_cost
            existing.shares = new_shares
            existing.avg_entry_price = new_cost / new_shares
            existing.current_price = existing.avg_entry_price
            existing.fees_paid += fee
            position = existing

        trade = PolymarketTrade(
            id=uuid4(),
            account_id=self.account_id,
            timestamp=datetime.now(tz=UTC),
            market_id=market_id,
            strategy_name=strategy_name,
            side=side,
            shares=filled_shares,
            entry_price=avg_entry_price,
            exit_price=None,
            pnl_usd=None,
            status=PolymarketTradeStatus.OPEN,
            reason_entry=reason_entry,
        )
        self.trades[str(trade.id)] = trade
        logger.info(
            "polymarket_paper_position_opened",
            account_id=self.account_id,
            market_id=market_id,
            side=side.value,
            shares=filled_shares,
            avg_entry_price=avg_entry_price,
            fee_usd=fee,
        )
        position.unrealized_pnl = _money(
            position.shares * position.current_price - _cost_basis(position)
        )
        return trade

    async def mark_to_market(
        self,
        market_id: str,
        side: PolymarketSide,
        book: PolymarketOrderBook,
    ) -> None:
        """Mark one open position against the current bid book."""
        position = self.positions.get((market_id, side))
        if position is None or position.status != PolymarketPositionStatus.OPEN:
            return
        current_price = book.best_bid if book.best_bid is not None else 0.0
        position.current_price = current_price
        position.unrealized_pnl = _money(position.shares * current_price - _cost_basis(position))
        logger.info(
            "polymarket_paper_position_marked",
            account_id=self.account_id,
            market_id=market_id,
            side=side.value,
            current_price=current_price,
            unrealized_pnl=position.unrealized_pnl,
        )

    async def close_position(
        self,
        market_id: str,
        side: PolymarketSide,
        order_book: PolymarketOrderBook,
        reason_exit: str,
    ) -> PolymarketTrade:
        """Sell an open position into bid liquidity."""
        position = self.positions.get((market_id, side))
        if position is None or position.status != PolymarketPositionStatus.OPEN:
            raise KeyError(f"open Polymarket position not found: {market_id} {side}")
        filled_shares, gross_proceeds = _walk_book(order_book.bids, position.shares)
        if filled_shares <= 0:
            raise ValueError("no bid liquidity available for paper close")
        avg_exit_price = gross_proceeds / filled_shares
        fee = self._fee(filled_shares, avg_exit_price)
        proceeds_after_fee = gross_proceeds - fee
        entry_cost = position.avg_entry_price * filled_shares
        entry_fee_alloc = position.fees_paid * (filled_shares / position.shares)
        pnl = proceeds_after_fee - entry_cost - entry_fee_alloc

        self.balance_usd = _money(self.balance_usd + proceeds_after_fee)
        position.shares -= filled_shares
        position.fees_paid -= entry_fee_alloc
        if position.shares <= 1e-9:
            position.shares = 0.0
            position.status = PolymarketPositionStatus.CLOSED
            self.positions.pop((market_id, side), None)

        trade = PolymarketTrade(
            id=uuid4(),
            account_id=self.account_id,
            timestamp=datetime.now(tz=UTC),
            market_id=market_id,
            strategy_name="paper_manual",
            side=side,
            shares=filled_shares,
            entry_price=position.avg_entry_price,
            exit_price=avg_exit_price,
            pnl_usd=pnl,
            status=PolymarketTradeStatus.CLOSED,
            reason_entry="paper close of existing position",
            reason_exit=reason_exit,
        )
        self.trades[str(trade.id)] = trade
        logger.info(
            "polymarket_paper_position_closed",
            account_id=self.account_id,
            market_id=market_id,
            side=side.value,
            pnl_usd=pnl,
        )
        return trade

    async def settle_position(
        self,
        market_id: str,
        side: PolymarketSide,
        resolution_outcome: PolymarketSide,
    ) -> PolymarketTrade:
        """Settle an open position at $1/share if correct, otherwise $0."""
        position = self.positions.get((market_id, side))
        if position is None:
            raise KeyError(f"open Polymarket position not found: {market_id} {side}")
        payout_price = 1.0 if side == resolution_outcome else 0.0
        payout = position.shares * payout_price
        pnl = payout - _cost_basis(position)
        self.balance_usd = _money(self.balance_usd + payout)
        position.current_price = payout_price
        position.unrealized_pnl = _money(pnl)
        position.status = PolymarketPositionStatus.RESOLVED
        position.resolution_outcome = resolution_outcome
        self.positions.pop((market_id, side), None)

        trade = PolymarketTrade(
            id=uuid4(),
            account_id=self.account_id,
            timestamp=datetime.now(tz=UTC),
            market_id=market_id,
            strategy_name="paper_resolution",
            side=side,
            shares=position.shares,
            entry_price=position.avg_entry_price,
            exit_price=payout_price,
            pnl_usd=pnl,
            status=PolymarketTradeStatus.RESOLVED,
            reason_entry="paper settlement of existing position",
            reason_exit=f"market resolved {resolution_outcome.value}",
        )
        self.trades[str(trade.id)] = trade
        logger.info(
            "polymarket_paper_position_resolved",
            account_id=self.account_id,
            market_id=market_id,
            side=side.value,
            outcome=resolution_outcome.value,
            pnl_usd=pnl,
        )
        return trade

    def _fee(self, shares: float, avg_price: float) -> float:
        if not self.fees_enabled:
            return 0.0
        return fee_for_trade(shares, avg_price, self.taker_fee_rate)


def _walk_book(levels: list[PolymarketBookLevel], requested_shares: float) -> tuple[float, float]:
    remaining = requested_shares
    filled = 0.0
    notional = 0.0
    for level in levels:
        if remaining <= 0:
            break
        level_fill = min(level.size, remaining)
        filled += level_fill
        notional += level_fill * level.price
        remaining -= level_fill
    return filled, notional


def _affordable_shares(
    asks: list[PolymarketBookLevel],
    available_usdc: float,
    fee_rate: float,
) -> float:
    shares = 0.0
    remaining = available_usdc
    for level in asks:
        cost_per_share = level.price + fee_for_trade(1.0, level.price, fee_rate)
        if cost_per_share <= 0:
            continue
        level_shares = min(level.size, remaining / cost_per_share)
        shares += level_shares
        remaining -= level_shares * cost_per_share
        if level_shares < level.size:
            break
    return shares


def _cost_basis(position: PolymarketPosition) -> float:
    return position.shares * position.avg_entry_price + position.fees_paid


def _money(value: float) -> float:
    return round(value + 1e-12, 2)
