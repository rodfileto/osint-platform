"""Async PostgreSQL connection manager for FastAPI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from fastapi import Request
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import Settings


@dataclass(slots=True)
class DatabaseClients:
    postgres: AsyncEngine
    postgres_sessions: async_sessionmaker[AsyncSession]


class DatabaseManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._postgres_engine = create_async_engine(
            settings.postgres_dsn,
            pool_pre_ping=True,
            pool_size=settings.postgres_pool_size,
            max_overflow=settings.postgres_max_overflow,
            pool_timeout=settings.postgres_pool_timeout,
            pool_recycle=settings.postgres_pool_recycle,
        )
        self._postgres_sessions = async_sessionmaker(
            self._postgres_engine,
            expire_on_commit=False,
        )

    @property
    def clients(self) -> DatabaseClients:
        return DatabaseClients(
            postgres=self._postgres_engine,
            postgres_sessions=self._postgres_sessions,
        )

    async def connect(self) -> None:
        await self._verify_postgres()

    async def disconnect(self) -> None:
        await self._postgres_engine.dispose()

    async def healthcheck(self) -> dict[str, dict[str, Any]]:
        postgres_result: dict[str, Any] = {"ok": True}

        try:
            await self._verify_postgres()
        except SQLAlchemyError as exc:
            postgres_result = {"ok": False, "error": str(exc.__class__.__name__)}

        return {"postgres": postgres_result}

    @asynccontextmanager
    async def postgres_connection(self) -> AsyncIterator[AsyncConnection]:
        async with self._postgres_engine.connect() as connection:
            yield connection

    @asynccontextmanager
    async def postgres_session(self) -> AsyncIterator[AsyncSession]:
        async with self._postgres_sessions() as session:
            yield session

    async def fetch_all(self, query: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.postgres_connection() as connection:
            result = await connection.execute(text(query), parameters or {})
            return [dict(row) for row in result.mappings().all()]

    async def fetch_one(self, query: str, parameters: dict[str, Any] | None = None) -> dict[str, Any] | None:
        async with self.postgres_connection() as connection:
            result = await connection.execute(text(query), parameters or {})
            row = result.mappings().first()
            return dict(row) if row is not None else None

    async def _verify_postgres(self) -> None:
        async with self._postgres_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

def get_database_manager(request: Request) -> DatabaseManager:
    return request.app.state.database_manager
