"""Small async-friendly wrapper around supabase-py's synchronous client."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from supabase import Client, create_client

from src.config import Settings

logger = structlog.get_logger(__name__)


class SupabaseRepository:
    """Persistence gateway for trading data."""

    def __init__(self, settings: Settings) -> None:
        self._client: Client = create_client(
            str(settings.supabase_url),
            settings.supabase_service_key,
        )

    async def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Insert a row and return the inserted payload when available."""
        logger.info("supabase_insert", table=table)
        return await asyncio.to_thread(self._insert_sync, table, payload)

    async def update(
        self,
        table: str,
        row_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Update a row by id."""
        logger.info("supabase_update", table=table, row_id=row_id)
        return await asyncio.to_thread(self._update_sync, table, row_id, payload)

    async def delete(self, table: str, row_id: str) -> None:
        """Delete a row by id."""
        logger.info("supabase_delete", table=table, row_id=row_id)
        await asyncio.to_thread(
            lambda: self._client.table(table).delete().eq("id", row_id).execute()
        )

    async def select_latest(
        self,
        table: str,
        limit: int = 1,
        order_column: str = "timestamp",
    ) -> list[dict[str, Any]]:
        """Return the latest rows from a table."""
        logger.info("supabase_select_latest", table=table, limit=limit)
        response = await asyncio.to_thread(
            lambda: self._client.table(table)
            .select("*")
            .order(order_column, desc=True)
            .limit(limit)
            .execute()
        )
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
        filters = filters or {}

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
        logger.info("supabase_upsert_state", key=key)
        payload = {"key": key, "value": value}
        await asyncio.to_thread(
            lambda: self._client.table("bot_state").upsert(payload, on_conflict="key").execute()
        )

    async def get_state(self, key: str) -> dict[str, Any] | None:
        """Load key/value runtime state."""
        logger.info("supabase_get_state", key=key)
        response = await asyncio.to_thread(
            lambda: self._client.table("bot_state")
            .select("value")
            .eq("key", key)
            .limit(1)
            .execute()
        )
        if not response.data:
            return None
        return dict(response.data[0]["value"])

    def _insert_sync(self, table: str, payload: dict[str, Any]) -> dict[str, Any] | None:
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
        response = self._client.table(table).update(payload).eq("id", row_id).execute()
        if not response.data:
            return None
        return dict(response.data[0])
