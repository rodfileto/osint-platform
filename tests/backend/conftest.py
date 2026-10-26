"""Backend tests run against a PostgreSQL with the Flyway migrations and dev/data/postgres/seed_sample.sql.

Point POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD at it; without
POSTGRES_HOST the tests are skipped.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))


@pytest.fixture
def run():
    """Run `fn(database)` on a fresh DatabaseManager and return its result."""
    if not os.getenv("POSTGRES_HOST"):
        pytest.skip("POSTGRES_HOST not set: no test database")

    from core.config import Settings
    from core.database import DatabaseManager

    async def call(fn):
        database = DatabaseManager(Settings())
        try:
            return await fn(database)
        finally:
            await database.disconnect()

    return lambda fn: asyncio.run(call(fn))
