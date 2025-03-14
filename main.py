from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import asyncpg
import pika
import json

app = FastAPI()

# Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect("postgresql://user:password@postgres/transactions")

# Подключение к RabbitMQ
def send_to_queue(queue, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="rabbitmq", 
            credentials=pika.PlainCredentials("admin", "strongpassword")
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))
    connection.close()

# Модель запроса
class TransactionRequest(BaseModel):
    sender: str
    recipient: str
    amount: float

@app.post("/transactions")
async def create_transaction(tx: TransactionRequest, db=Depends(get_db)):
    async with db.transaction():
        sender_balance = await db.fetchval("SELECT balance FROM users WHERE id = $1", tx.sender)
        if sender_balance < tx.amount:
            raise HTTPException(status_code=400, detail="Недостаточно средств")
        await db.execute("UPDATE users SET balance = balance - $1 WHERE id = $2", tx.amount, tx.sender)
        await db.execute("UPDATE users SET balance = balance + $1 WHERE id = $2", tx.amount, tx.recipient)

        tx_id = await db.fetchval("INSERT INTO transactions (sender, recipient, amount) VALUES ($1, $2, $3) RETURNING id",
                                  tx.sender, tx.recipient, tx.amount)

        send_to_queue("transactions", {"tx_id": tx_id, "amount": tx.amount})
        return {"transaction_id": tx_id}
