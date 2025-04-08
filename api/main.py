from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from common.kafka import get_producer
import asyncpg

app = FastAPI()
db_pool = None
producer = None

class TransactionRequest(BaseModel):
    sender: str
    recipient: str
    amount: float

@app.on_event("startup")
async def startup():
    global db_pool, producer
    db_pool = await asyncpg.create_pool("postgresql://user:password@postgres/transactions")
    producer = await get_producer()

@app.on_event("shutdown")
async def shutdown():
    await db_pool.close()
    await producer.stop()

@app.post("/transactions")
async def create_transaction(tx: TransactionRequest):
    async with db_pool.acquire() as db:
        async with db.transaction():
            sender_balance = await db.fetchval("SELECT balance FROM users WHERE id = $1", tx.sender)
            if sender_balance < tx.amount:
                raise HTTPException(status_code=400, detail="Недостаточно средств")
            await db.execute("UPDATE users SET balance = balance - $1 WHERE id = $2", tx.amount, tx.sender)
            await db.execute("UPDATE users SET balance = balance + $1 WHERE id = $2", tx.amount, tx.recipient)
            tx_id = await db.fetchval("INSERT INTO transactions (sender, recipient, amount) VALUES ($1, $2, $3) RETURNING id", tx.sender, tx.recipient, tx.amount)

    await producer.send_and_wait("transactions", {"tx_id": tx_id, "amount": tx.amount})
    return {"transaction_id": tx_id}
