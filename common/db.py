# common/db.py
import asyncpg
import asyncio
from datetime import datetime


DATABASE_URL = "postgresql://user:password@postgres/transactions"

# Пул соединений для работы с PostgreSQL
class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def init(self):
        """Инициализация пула соединений."""
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=5, max_size=10)

    async def fetch(self, query, *args):
        """Выполнение SELECT запроса."""
        async with self.pool.acquire() as connection:
            result = await connection.fetch(query, *args)
        return result

    async def fetchrow(self, query, *args):
        """Выполнение SELECT запроса, возвращающего одну строку."""
        async with self.pool.acquire() as connection:
            result = await connection.fetchrow(query, *args)
        return result

    async def execute(self, query, *args):
        """Выполнение INSERT/UPDATE/DELETE запроса."""
        async with self.pool.acquire() as connection:
            await connection.execute(query, *args)

    async def close(self):
        """Закрытие пула соединений."""
        await self.pool.close()


# Инициализация глобальной переменной для работы с БД
db = DB(DATABASE_URL)


# Модель для транзакции (не изменилось)
async def create_transaction(sender_account: str, receiver_account: str, amount: float):
    query = """
    INSERT INTO transactions (sender_account, receiver_account, amount, timestamp) 
    VALUES ($1, $2, $3, $4)
    RETURNING id;
    """
    timestamp = datetime.utcnow()
    result = await db.fetchrow(query, sender_account, receiver_account, amount, timestamp)
    return result


# Пример функции для получения транзакции по ID
async def get_transaction_by_id(transaction_id: int):
    query = """
    SELECT * FROM transactions WHERE id = $1;
    """
    result = await db.fetchrow(query, transaction_id)
    return result


# Пример функции для получения всех транзакций
async def get_all_transactions():
    query = """
    SELECT * FROM transactions;
    """
    result = await db.fetch(query)
    return result
