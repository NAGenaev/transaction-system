from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import asyncpg
import aio_pika
import json
import asyncio
from asyncpg import Pool

app = FastAPI()

# Подключение к PostgreSQL
async def get_db_pool() -> Pool:
    return await asyncpg.create_pool(
    "postgresql://user:password@postgres/transactions",
    min_size=50,    # минимальное количество соединений
    max_size=200,   # максимальное количество соединений
    max_queries=50000,  # максимальное количество запросов на одно соединение
    max_inactive_connection_lifetime=60 * 60,  # время жизни бездействующего соединения (в секундах)
    )

# Подключение к RabbitMQ (асинхронно)
async def send_to_queue(queue, message):
    connection = await aio_pika.connect_robust("amqp://admin:strongpassword@rabbitmq/")
    async with connection:
        channel = await connection.channel()  # создаем канал
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue,
        )

# Модель запроса
class TransactionRequest(BaseModel):
    sender: str
    recipient: str
    amount: float

@app.post("/transactions")
async def create_transaction(tx: TransactionRequest, db=Depends(get_db_pool)):
    async with db.acquire() as conn:
        async with conn.transaction():
            # Проверяем баланс отправителя
            sender_balance = await conn.fetchval("SELECT balance FROM users WHERE id = $1", tx.sender)
            if sender_balance < tx.amount:
                raise HTTPException(status_code=400, detail="Недостаточно средств")

            # Обновляем баланс отправителя и получателя
            await conn.execute("UPDATE users SET balance = balance - $1 WHERE id = $2", tx.amount, tx.sender)
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE id = $2", tx.amount, tx.recipient)

            # Вставляем транзакцию в базу данных
            tx_id = await conn.fetchval(
                "INSERT INTO transactions (sender, recipient, amount) VALUES ($1, $2, $3) RETURNING id",
                tx.sender, tx.recipient, tx.amount
            )

            # Отправляем сообщение в очередь
            asyncio.create_task(send_to_queue("transactions", {"tx_id": tx_id, "amount": tx.amount}))

            return {"transaction_id": tx_id}
