from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import asyncpg
import aio_pika
import json
from asyncpg import Pool
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
# Подключаем сбор метрик
Instrumentator().instrument(app).expose(app)

# Глобальные переменные для пула соединений
db_pool: Pool = None
rabbitmq_connection: aio_pika.RobustConnection = None

# Инициализация соединения с БД
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        "postgresql://user:password@postgres/transactions",
        min_size=50,
        max_size=200,
        max_inactive_connection_lifetime=3600,
    )

# Инициализация соединения с RabbitMQ
async def init_rabbitmq():
    global rabbitmq_connection
    rabbitmq_connection = await aio_pika.connect_robust("amqp://admin:strongpassword@rabbitmq/")

# Зависимость для получения соединения с БД
async def get_db():
    async with db_pool.acquire() as connection:
        yield connection

# Функция отправки сообщений в очередь RabbitMQ
async def send_to_queue(queue: str, message: dict):
    async with rabbitmq_connection.channel() as channel:
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
async def create_transaction(tx: TransactionRequest, db=Depends(get_db)):
    async with db.transaction():
        # Проверяем баланс отправителя
        sender_balance = await db.fetchval("SELECT balance FROM users WHERE id = $1", tx.sender)
        if sender_balance is None:
            raise HTTPException(status_code=404, detail="Отправитель не найден")
        if sender_balance < tx.amount:
            raise HTTPException(status_code=400, detail="Недостаточно средств")

        # Обновляем баланс отправителя и получателя
        await db.execute("UPDATE users SET balance = balance - $1 WHERE id = $2", tx.amount, tx.sender)
        await db.execute("UPDATE users SET balance = balance + $1 WHERE id = $2", tx.amount, tx.recipient)

        # Вставляем транзакцию в базу данных
        tx_id = await db.fetchval(
            "INSERT INTO transactions (sender, recipient, amount) VALUES ($1, $2, $3) RETURNING id",
            tx.sender, tx.recipient, tx.amount
        )

        # Отправляем сообщение в очередь
        await send_to_queue("transactions", {"tx_id": tx_id, "amount": tx.amount})

        return {"transaction_id": tx_id}

# Инициализация соединений при запуске приложения
@app.on_event("startup")
async def startup():
    await init_db()
    await init_rabbitmq()

# Закрытие соединений при остановке приложения
@app.on_event("shutdown")
async def shutdown():
    await db_pool.close()
    await rabbitmq_connection.close()
