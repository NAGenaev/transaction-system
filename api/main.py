# api/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from common.kafka import get_kafka_producer
from common.constants import TOPIC_API_TO_ORCH
import pyfiglet
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter

ascii_banner = pyfiglet.figlet_format("TTTS API 0.0.3", font="slant")
print(ascii_banner)

api_requests_total = Counter("api_requests_total", "Total API transaction requests")
api_requests_failed = Counter("api_requests_failed", "API transaction failures")

app = FastAPI()
Instrumentator().instrument(app).expose(app)
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

@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

@app.post("/transactions/")
async def create_transaction(tx: Transaction):
    api_requests_total.inc()
    try:
        message = tx.json().encode("utf-8")
        await app.state.producer.send_and_wait(TOPIC_API_TO_ORCH, message)  # Отправляем в топик Orchestrator
        print(f"✅ Запрос принят и отправлен в Orchestrator (topic:transaction-events): {tx}")
        print(f"📦 API → Orchestrator: {tx}")
        return {"status": "queued"}
    except Exception as e:
        api_requests_failed.inc()
        raise HTTPException(status_code=500, detail=str(e))
