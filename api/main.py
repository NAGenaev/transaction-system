from pydantic import BaseModel, field_validator, FieldValidationInfo
import uuid
import asyncio
import logging
import pyfiglet
from fastapi import FastAPI, HTTPException
#from starlette_timeout.middleware import TimeoutMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from common.kafka import get_kafka_producer
from common.constants import TOPIC_API_TO_ORCH

# Настройка логирования
#logging.basicConfig(
#    level=logging.INFO,
#    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#)

#logger = logging.getLogger("api")

# Печать баннера
ascii_banner = pyfiglet.figlet_format("TTTS API 0.0.3", font="slant")
print(ascii_banner)

# Инициализация приложения FastAPI
app = FastAPI()

# Метрики Prometheus
Instrumentator().instrument(app).expose(app, include_in_schema=False)

# Ограничение времени обработки запроса
#app.add_middleware(TimeoutMiddleware, timeout=10.0)  # таймаут 10 секунд
# Модель транзакции
class Transaction(BaseModel):
    sender_account: str
    receiver_account: str
    amount: float
    transaction_id: str = None

    @field_validator("receiver_account", "sender_account")
    @classmethod
    def validate_account_length(cls, v: str) -> str:
        if len(v) != 17:
            raise ValueError("Номер счёта должен содержать ровно 17 символов")
        return v

    @field_validator("receiver_account")
    @classmethod
    def validate_receiver_account(cls, v: str, info: FieldValidationInfo) -> str:
        sender_account = info.data.get('sender_account')
        if sender_account and v == sender_account:
            raise ValueError("Счёт получателя не может быть равным счёту отправителя")
        return v

    @field_validator("sender_account")
    @classmethod
    def validate_sender_account(cls, v: str, info: FieldValidationInfo) -> str:
        receiver_account = info.data.get('receiver_account')
        if receiver_account and v == receiver_account:
            raise ValueError("Счёт отправителя не может быть равным счёту получателя")
        return v

@app.on_event("startup")
async def startup_event():
    #logger.info("Создаём Kafka producer...")
    producer = await get_kafka_producer()
    app.state.producer = producer
    #logger.info("Kafka producer создан успешно")

@app.on_event("shutdown")
async def shutdown_event():
    #logger.info("Останавливаем Kafka producer...")
    await app.state.producer.stop()
    #logger.info("Kafka producer остановлен")

@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

@app.post("/transactions/")
async def create_transaction(tx: Transaction):
    if not tx.transaction_id:
        tx.transaction_id = str(uuid.uuid4())

    try:
        message = tx.json().encode("utf-8")
        await asyncio.wait_for(
            app.state.producer.send_and_wait(TOPIC_API_TO_ORCH, message),
            timeout=2.0
        )
        #logger.info(f"📦 API → Orchestrator: {tx}")
        return {"status": "queued", "transaction_id": tx.transaction_id}

    except asyncio.TimeoutError:
        #logger.error(f"⏱ Timeout при отправке Kafka: {tx}")
        raise HTTPException(status_code=504, detail="Kafka timeout")
    except Exception as e:
        #logger.exception(f"💥 Ошибка при отправке в Kafka: {e}")
        raise HTTPException(status_code=500, detail="Kafka send failed")
