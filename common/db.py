import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class Database:
    _instance = None
    
    def __init__(self):
        self._pool = None
    
    @classmethod
    async def get_instance(cls):
        if cls._instance is None:
            cls._instance = Database()
            await cls._instance._initialize_pool()
        return cls._instance
    
    async def _initialize_pool(self):
        """Инициализация пула соединений"""
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
        """Выполнение SQL-запроса"""
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        """Выборка нескольких записей"""
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        """Выборка одной записи"""
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def close(self):
        """Закрытие пула соединений"""
        if self._pool:
            await self._pool.close()
            logger.info("Database pool closed")

# Глобальный экземпляр для импорта
db = Database()