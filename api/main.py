# api/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from common.kafka import get_kafka_producer
from common.constants import TOPIC_API_TO_ORCH
import pyfiglet

ascii_banner = pyfiglet.figlet_format("TTTS API 0.0.3", font="slant")
print(ascii_banner)

app = FastAPI()

class Transaction(BaseModel):
    sender_account: str
    receiver_account: str
    amount: float

@app.on_event("startup")
async def startup_event():
    app.state.producer = await get_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.producer.stop()

@app.post("/transactions/")
async def create_transaction(tx: Transaction):
    try:
        message = tx.json().encode("utf-8")
        await app.state.producer.send_and_wait(TOPIC_API_TO_ORCH, message)  # Отправляем в топик Orchestrator
        print(f"✅ Запрос принят и отправлен в Orchestrator (topic:transaction-events): {tx}")
        print(f"📦 API → Orchestrator: {tx}")
        return {"status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
