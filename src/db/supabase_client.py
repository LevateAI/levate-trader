"""Small async-friendly wrapper around supabase-py's synchronous client."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from supabase import Client, create_client

from src.config import Settings

logger = structlog.get_logger(__name__)

ACCOUNT_SCOPED_TABLES = {
    "trades",
    "positions",
    "equity_snapshots",
    "strategy_signals",
    "circuit_breaker_events",
    "bot_state",
}
ACCOUNT_WRITTEN_TABLES = ACCOUNT_SCOPED_TABLES | {"market_data_snapshots"}


class SupabaseRepository:
    """Persistence gateway for trading data."""

    def __init__(self, settings: Settings) -> None:
        self._account_id = settings.account_id
        self._client: Client = create_client(
            str(settings.supabase_url),
            settings.supabase_service_key,
        )

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Insert a row and return the inserted payload when available."""
        payload = self._with_account(table, payload)
        logger.info("supabase_insert", table=table, account_id=payload.get("account_id"))
        return await asyncio.to_thread(self._insert_sync, table, payload)

    async def update(
        self,
        table: str,
        row_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Update a row by id."""
        payload = self._with_account(table, payload)
        logger.info(
            "supabase_update",
            table=table,
            row_id=row_id,
            account_id=payload.get("account_id"),
        )
        return await asyncio.to_thread(self._update_sync, table, row_id, payload)

    async def delete(self, table: str, row_id: str) -> None:
        """Delete a row by id."""
        logger.info("supabase_delete", table=table, row_id=row_id, account_id=self._account_id)

        def _delete_sync() -> Any:
            query = self._client.table(table).delete().eq("id", row_id)
            if table in ACCOUNT_SCOPED_TABLES:
                query = query.eq("account_id", self._account_id)
            return query.execute()

        await asyncio.to_thread(_delete_sync)

    async def select_latest(
        self,
        table: str,
        limit: int = 1,
        order_column: str = "timestamp",
    ) -> list[dict[str, Any]]:
        """Return the latest rows from a table."""
        logger.info(
            "supabase_select_latest",
            table=table,
            limit=limit,
            account_id=self._account_id if table in ACCOUNT_SCOPED_TABLES else None,
        )

        def _select_latest_sync() -> Any:
            query = self._client.table(table).select("*")
            if table in ACCOUNT_SCOPED_TABLES:
                query = query.eq("account_id", self._account_id)
            return query.order(order_column, desc=True).limit(limit).execute()

        response = await asyncio.to_thread(_select_latest_sync)
        return list(response.data or [])

    async def select_where(
        self,
        table: str,
        filters: dict[str, Any],
        limit: int = 1000,
        order_column: str = "timestamp",
        desc: bool = True,
    ) -> list[dict[str, Any]]:
        """Return rows matching exact filters."""
        filters = self._with_account_filter(table, filters)
        logger.info("supabase_select_where", table=table, filters=list(filters), limit=limit)

        def _select_sync() -> Any:
            query = self._client.table(table).select("*")
            for column, value in filters.items():
                query = query.eq(column, value)
            return query.order(order_column, desc=desc).limit(limit).execute()

        response = await asyncio.to_thread(_select_sync)
        return list(response.data or [])

    async def select_since(
        self,
        table: str,
        since_iso: str,
        order_column: str = "timestamp",
        limit: int = 1,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return the earliest rows at or after a timestamp."""
        logger.info("supabase_select_since", table=table, since_iso=since_iso, limit=limit)
        filters = self._with_account_filter(table, filters or {})

        def _select_sync() -> Any:
            query = (
                self._client.table(table)
                .select("*")
                .gte(order_column, since_iso)
            )
            for column, value in filters.items():
                query = query.eq(column, value)
            return query.order(order_column, desc=False).limit(limit).execute()

        response = await asyncio.to_thread(
            _select_sync
        )
        return list(response.data or [])

    async def upsert_state(self, key: str, value: dict[str, Any]) -> None:
        """Persist key/value runtime state."""
        logger.info("supabase_upsert_state", key=key, account_id=self._account_id)
        payload = {"account_id": self._account_id, "key": key, "value": value}
        await asyncio.to_thread(
            lambda: self._client.table("bot_state")
            .upsert(payload, on_conflict="account_id,key")
            .execute()
        )

    async def get_state(self, key: str) -> dict[str, Any] | None:
        """Load key/value runtime state."""
        logger.info("supabase_get_state", key=key, account_id=self._account_id)
        response = await asyncio.to_thread(
            lambda: self._client.table("bot_state")
            .select("value")
            .eq("account_id", self._account_id)
            .eq("key", key)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        return dict(response.data[0]["value"])

    def _insert_sync(self, table: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        self._validate_account_payload(table, payload)
        response = self._client.table(table).insert(payload).execute()
        if not response.data:
            return None
        return dict(response.data[0])

    def _update_sync(
        self,
        table: str,
        row_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        self._validate_account_payload(table, payload)
        query = self._client.table(table).update(payload).eq("id", row_id)
        if table in ACCOUNT_SCOPED_TABLES:
            query = query.eq("account_id", self._account_id)
        response = query.execute()
        if not response.data:
            return None
        return dict(response.data[0])

    def _with_account(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of payload with this bot's account id injected when needed."""
        if table not in ACCOUNT_WRITTEN_TABLES:
            return dict(payload)
        return dict(payload) | {"account_id": self._account_id}

    def _with_account_filter(
        self,
        table: str,
        filters: dict[str, Any],
    ) -> dict[str, Any]:
        """Return filters scoped to this tournament account when the table requires it."""
        if table not in ACCOUNT_SCOPED_TABLES:
            return dict(filters)
        return dict(filters) | {"account_id": self._account_id}

    def _validate_account_payload(self, table: str, payload: dict[str, Any]) -> None:
        """Reject scoped writes that would leak into the shared account namespace."""
        if table not in ACCOUNT_WRITTEN_TABLES:
            return
        if not payload.get("account_id"):
            raise ValueError(f"{table} writes must include account_id")
