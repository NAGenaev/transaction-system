import asyncpg
import asyncio
from datetime import datetime
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://user:password@postgres/transactions"

class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def init(self):
        """Инициализация пула соединений."""
        try:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=2)
            logger.info("Пул соединений с базой данных успешно инициализирован.")
        except Exception as e:
            logger.error(f"Ошибка при инициализации пула соединений: {e}")
            raise

    async def fetch(self, query, *args):
        """Выполнение SELECT запроса."""
        try:
            async with self.pool.acquire() as connection:
                result = await connection.fetch(query, *args)
            return result
        except Exception as e:
            logger.error(f"Ошибка при выполнении SELECT запроса: {e}")
            raise

    async def fetchrow(self, query, *args):
        """Выполнение SELECT запроса, возвращающего одну строку."""
        try:
            async with self.pool.acquire() as connection:
                result = await connection.fetchrow(query, *args)
            return result
        except Exception as e:
            logger.error(f"Ошибка при выполнении SELECT запроса (fetchrow): {e}")
            raise

    async def execute(self, query, *args):
        """Выполнение INSERT/UPDATE/DELETE запроса."""
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(query, *args)
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса (execute): {e}")
            raise

    async def close(self):
        """Закрытие пула соединений."""
        if self.pool:
            await self.pool.close()
            logger.info("Пул соединений закрыт.")
        else:
            logger.warning("Попытка закрыть пул соединений, который не был инициализирован.")

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
    try:
        result = await db.fetchrow(query, sender_account, receiver_account, amount, timestamp)
        return result
    except Exception as e:
        logger.error(f"Ошибка при создании транзакции: {e}")
        raise

# Пример функции для получения транзакции по ID
async def get_transaction_by_id(transaction_id: int):
    query = """
    SELECT * FROM transactions WHERE id = $1;
    """
    try:
        result = await db.fetchrow(query, transaction_id)
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении транзакции по ID {transaction_id}: {e}")
        raise

# Пример функции для получения всех транзакций
async def get_all_transactions():
    query = """
    SELECT * FROM transactions;
    """
    try:
        result = await db.fetch(query)
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении всех транзакций: {e}")
        raise