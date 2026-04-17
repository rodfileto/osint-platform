"""Async PostgreSQL and Neo4j connection managers for FastAPI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from fastapi import Request
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import Neo4jError
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
    neo4j: AsyncDriver
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
        self._neo4j_driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
            max_connection_lifetime=settings.neo4j_max_connection_lifetime,
            max_connection_pool_size=settings.neo4j_max_connection_pool_size,
            connection_timeout=settings.neo4j_connection_timeout,
        )

    @property
    def clients(self) -> DatabaseClients:
        return DatabaseClients(
            postgres=self._postgres_engine,
            neo4j=self._neo4j_driver,
            postgres_sessions=self._postgres_sessions,
        )

    async def connect(self) -> None:
        await self._verify_postgres()
        await self._neo4j_driver.verify_connectivity()

    async def disconnect(self) -> None:
        await self._neo4j_driver.close()
        await self._postgres_engine.dispose()

    async def healthcheck(self) -> dict[str, dict[str, Any]]:
        postgres_result: dict[str, Any] = {"ok": True}
        neo4j_result: dict[str, Any] = {"ok": True}

        try:
            await self._verify_postgres()
        except SQLAlchemyError as exc:
            postgres_result = {"ok": False, "error": str(exc.__class__.__name__)}

        try:
            await self._verify_neo4j()
        except Neo4jError as exc:
            neo4j_result = {"ok": False, "error": str(exc.code or exc.__class__.__name__)}
        except Exception as exc:  # pragma: no cover - driver-level failures vary
            neo4j_result = {"ok": False, "error": str(exc.__class__.__name__)}

        return {
            "postgres": postgres_result,
            "neo4j": neo4j_result,
        }

    @asynccontextmanager
    async def postgres_connection(self) -> AsyncIterator[AsyncConnection]:
        async with self._postgres_engine.connect() as connection:
            yield connection

    @asynccontextmanager
    async def postgres_session(self) -> AsyncIterator[AsyncSession]:
        async with self._postgres_sessions() as session:
            yield session

    @asynccontextmanager
    async def                       neo4j_session(self) -> AsyncIterator[Any]:
        async with self._neo4j_driver.session(database=self.settings.neo4j_database) as session:
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

    async def run_neo4j_query(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        async with self.neo4j_session() as session:
            result = await session.run(query, parameters or {})
            return [record.data() async for record in result]

    async def _verify_postgres(self) -> None:
        async with self._postgres_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def _verify_neo4j(self) -> None:
        async with self.neo4j_session() as session:
            await session.run("RETURN 1 AS ok")


def get_database_manager(request: Request) -> DatabaseManager:
    return request.app.state.database_manager
