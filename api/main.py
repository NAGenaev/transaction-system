from pydantic import BaseModel, field_validator, FieldValidationInfo
import uuid
import asyncio
import logging
import pyfiglet
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter
from common.kafka import get_kafka_producer
from common.constants import TOPIC_API_TO_ORCH

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("api")

# Печать баннера
ascii_banner = pyfiglet.figlet_format("TTTS API 0.0.3", font="slant")
print(ascii_banner)

# Метрики Prometheus
api_requests_total = Counter("api_requests_total", "Total API transaction requests")
api_requests_failed = Counter("api_requests_failed", "API transaction failures")

# Инициализация приложения FastAPI
app = FastAPI()
Instrumentator().instrument(app).expose(app)

# Модель транзакции
class Transaction(BaseModel):
    sender_account: str
    receiver_account: str
    amount: float
    transaction_id: str = None

# ФЛК на АПИ
    @field_validator("receiver_account", "sender_account")
    @classmethod
    def validate_account_length(cls, v: str) -> str:
        """Проверка длины счета (должно быть 17 символов)"""
        if len(v) != 17:
            logger.error(f"ФЛК: Некорректная длина счёта: {v}")
            raise ValueError("Номер счёта должен содержать ровно 17 символов")
        return v

    @field_validator("receiver_account")
    @classmethod
    def validate_receiver_account(cls, v: str, info: FieldValidationInfo) -> str:
        """Проверка, чтобы отправитель и получатель не совпадали"""
        sender_account = info.data.get('sender_account')
        if sender_account and v == sender_account:
            logger.error(f"ФЛК: Счета совпадают: {v} == {sender_account}")
            raise ValueError("Счёт получателя не может быть равным счёту отправителя")
        return v

    @field_validator("sender_account")
    @classmethod
    def validate_sender_account(cls, v: str, info: FieldValidationInfo) -> str:
        """Проверка, чтобы отправитель и получатель не совпадали"""
        receiver_account = info.data.get('receiver_account')
        if receiver_account and v == receiver_account:
            logger.error(f"ФЛК: Счета совпадают: {v} == {receiver_account}")
            raise ValueError("Счёт отправителя не может быть равным счёту получателя")
        return v
        
# События старта и остановки приложения
@app.on_event("startup")
async def startup_event():
    app.state.producer = await get_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.producer.stop()

# Эндпоинт для проверки здоровья
@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

# Эндпоинт для создания транзакции
@app.post("/transactions/")
async def create_transaction(tx: Transaction):
    api_requests_total.inc()

    # Если transaction_id не было передано, генерируем его
    if not tx.transaction_id:
        tx.transaction_id = str(uuid.uuid4())  # Генерация уникального ID

    try:
        # Преобразуем транзакцию в JSON и кодируем в байты
        message = tx.json().encode("utf-8")
        
        # Отправляем транзакцию в Kafka с ограничением времени
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