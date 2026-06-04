"""Supabase persistence for the standalone Polymarket module."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import structlog
from supabase import Client, create_client

from src.polymarket.config import PolymarketSettings
from src.polymarket.models import (
    PolymarketMarketSnapshot,
    PolymarketPosition,
    PolymarketTrade,
)

logger = structlog.get_logger(__name__)


class PolymarketRepository:
    """Persistence gateway for Polymarket paper-trading tables."""

    def __init__(self, settings: PolymarketSettings, account_id: str | None = None) -> None:
        self._account_id = account_id or settings.polymarket_account_id
        self._client: Client = create_client(
            str(settings.supabase_url),
            settings.supabase_service_key,
        )

    async def ensure_account(self, display_name: str, starting_balance_usd: float) -> None:
        """Upsert the standalone Polymarket account row."""
        payload: dict[str, Any] = {
            "account_id": self._account_id,
            "display_name": display_name,
            "starting_balance_usd": starting_balance_usd,
            "active": True,
        }
        logger.info("polymarket_account_upsert", account_id=self._account_id)
        await asyncio.to_thread(
            lambda: self._client.table("polymarket_accounts")
            .upsert(payload, on_conflict="account_id")
            .execute()
        )

    async def insert_snapshot(self, snapshot: PolymarketMarketSnapshot) -> None:
        """Insert a market snapshot row."""
        await self._insert("polymarket_market_snapshots", snapshot.to_payload())

    async def insert_position(self, position: PolymarketPosition) -> None:
        """Insert or update a paper position row."""
        payload: dict[str, Any] = {
            "id": str(position.id),
            "account_id": self._account_id,
            "timestamp": position.timestamp.isoformat(),
            "market_id": position.market_id,
            "horizon": position.horizon,
            "window_seconds": position.window_seconds,
            "side": position.side.value,
            "shares": round(position.shares, 6),
            "avg_entry_price": round(position.avg_entry_price, 4),
            "current_price": round(position.current_price, 4),
            "unrealized_pnl": round(position.unrealized_pnl, 2),
            "status": position.status.value,
            "resolution_outcome": (
                position.resolution_outcome.value if position.resolution_outcome else None
            ),
        }
        logger.info(
            "polymarket_position_upsert",
            account_id=self._account_id,
            market_id=position.market_id,
            side=position.side.value,
        )
        await asyncio.to_thread(
            lambda: self._client.table("polymarket_positions")
            .upsert(payload, on_conflict="id")
            .execute()
        )

    async def insert_trade(self, trade: PolymarketTrade) -> None:
        """Insert a paper trade row."""
        await self._insert("polymarket_trades", trade.to_payload())

    async def insert_equity_snapshot(
        self,
        balance_usd: float,
        equity_usd: float,
        open_position_count: int,
    ) -> None:
        """Insert a Polymarket account equity snapshot."""
        payload: dict[str, Any] = {
            "account_id": self._account_id,
            "balance_usd": round(balance_usd, 2),
            "equity_usd": round(equity_usd, 2),
            "open_position_count": open_position_count,
        }
        await self._insert("polymarket_equity_snapshots", payload)

    async def upsert_heartbeat(
        self,
        component: str,
        last_ok_at: datetime,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """Upsert one component liveness row into the shared heartbeat table."""
        payload: dict[str, Any] = {
            "account_id": self._account_id,
            "component": component,
            "last_ok_at": last_ok_at.isoformat(),
            "detail": detail or {},
        }
        logger.info(
            "polymarket_heartbeat_upsert",
            account_id=self._account_id,
            component=component,
        )
        await asyncio.to_thread(
            lambda: self._client.table("bot_heartbeat")
            .upsert(payload, on_conflict="account_id,component")
            .execute()
        )

    async def _insert(self, table: str, payload: dict[str, Any]) -> None:
        logger.info(
            "polymarket_supabase_insert",
            table=table,
            account_id=payload.get("account_id"),
        )
        await asyncio.to_thread(lambda: self._client.table(table).insert(payload).execute())
