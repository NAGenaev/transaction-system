# common/db.py

import asyncpg
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self._pool = None

    @property
    def pool(self):
        return self._pool

    async def initialize(self):
        if self._pool is not None:
            return
        try:
            self._pool = await asyncpg.create_pool(
                host='postgres',
                port=5432,
                user='user',
                password='password',
                database='transactions',
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def execute(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("Database pool closed")
    


# Экземпляр для использования во всех модулях
db = Database()
