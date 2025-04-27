import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from common.kafka import get_kafka_producer
from common.constants import TOPIC_API_TO_ORCH
import pyfiglet
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter
import logging

logging.basicConfig(
    level=logging.INFO,  # или DEBUG
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("api")

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
    transaction_id: str = None  # Добавляем поле для transaction_id

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
    
    # Если transaction_id не было передано, генерируем его
    if not tx.transaction_id:
        tx.transaction_id = str(uuid.uuid4())  # Генерация уникального ID

    try:
        # Преобразуем транзакцию в JSON и кодируем в байты
        message = tx.json().encode("utf-8")
        
        # Отправляем транзакцию в Kafka
        await asyncio.wait_for(
            app.state.producer.send_and_wait(TOPIC_API_TO_ORCH, message),
            timeout=2.0  # ⏱ ограничиваем время ожидания
        )
        
        # Логируем успешную отправку
        logger.info(f"📦 API → Orchestrator: {tx}")
        return {"status": "queued", "transaction_id": tx.transaction_id}
    
    except asyncio.TimeoutError:
        api_requests_failed.inc()
        logger.error(f"⏱ Timeout при отправке Kafka: {tx}")
        raise HTTPException(status_code=504, detail="Kafka timeout")
    except Exception as e:
        api_requests_failed.inc()
        logger.exception(f"💥 Ошибка при отправке в Kafka: {e}")
        raise HTTPException(status_code=500, detail="Kafka send failed")
