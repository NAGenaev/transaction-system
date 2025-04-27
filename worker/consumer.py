import asyncio
import json
import os
import logging
from common.kafka import get_kafka_consumer, get_kafka_producer
from common.db import db
from datetime import datetime
import pyfiglet
from prometheus_client import start_http_server, Counter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError

# ────────────────────────────
# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,  # или DEBUG
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

# ────────────────────────────
# ASCII-баннер
ascii_banner = pyfiglet.figlet_format("TTTS SHARD WORKER 0.0.3", font="slant")
print(ascii_banner)

# ────────────────────────────
# METRICS
tx_processed = Counter("transactions_processed_total", "Total number of successfully processed transactions")
tx_failed = Counter("transactions_failed_total", "Total number of failed transactions")
tx_insufficient = Counter("transactions_insufficient_funds_total", "Transactions failed due to insufficient funds")
tx_not_found = Counter("transactions_account_not_found_total", "Transactions failed due to missing accounts")

# ────────────────────────────
# Kafka topics
TOPIC_VALIDATED = os.getenv("VALIDATED_TOPIC", "validated-transactions")
TOPIC_CONFIRMATION = "transaction-confirmation"

# Ограничение на параллельность
MAX_CONCURRENT_TASKS = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# ────────────────────────────
# Kafka retry logic

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5),
    retry=retry_if_exception_type(KafkaTimeoutError)
)
async def send_confirmation(producer, topic, msg: bytes):
    """Отправка подтверждения транзакции в Kafka с повторными попытками"""
    try:
        logger.info(f"🚚 Отправка сообщения в Kafka: {msg}")
        await producer.send(topic, msg)
        logger.info(f"✅ Сообщение успешно отправлено в Kafka: {msg}")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке сообщения в Kafka: {e}")


async def process_transaction(tx: dict, producer):
    """Основная логика обработки транзакции"""
    sender_account = str(tx["sender_account"])
    receiver_account = str(tx["receiver_account"])
    amount = tx["amount"]
    confirmation_msg = None

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                sender = await conn.fetchrow(
                    "SELECT balance FROM accounts WHERE account_number = $1 FOR UPDATE", sender_account)
                receiver = await conn.fetchrow(
                    "SELECT balance FROM accounts WHERE account_number = $1 FOR UPDATE", receiver_account)

                if sender is None or receiver is None:
                    logger.error(f"❌ Один из пользователей не найден: {sender_account} или {receiver_account}")
                    tx_not_found.inc()
                    return

                if sender["balance"] < amount:
                    logger.error(f"❌ Недостаточно средств на счете {sender_account}: {sender['balance']} < {amount}")
                    tx_insufficient.inc()
                    return

                await conn.execute("UPDATE accounts SET balance = balance - $1 WHERE account_number = $2",
                                   amount, sender_account)
                await conn.execute("UPDATE accounts SET balance = balance + $1 WHERE account_number = $2",
                                   amount, receiver_account)

                timestamp = datetime.utcnow()
                tx_id = await conn.fetchrow(""" 
                    INSERT INTO transactions (sender_account, receiver_account, amount, timestamp) 
                    VALUES ($1, $2, $3, $4) RETURNING id
                """, sender_account, receiver_account, amount, timestamp)

                logger.info(f"💾 Транзакция записана: ID={tx_id['id']}, FROM={sender_account}, TO={receiver_account}, AMOUNT={amount}")
                tx_processed.inc()

                confirmation_msg = {
                    "transaction_id": tx_id['id'],
                    "status": "processed",
                    "sender_account": sender_account,
                    "receiver_account": receiver_account,
                    "amount": amount
                }

        if confirmation_msg:
            logger.info(f"✅ Отправка подтверждения транзакции: {confirmation_msg}")
            # Отправляем сообщение в Kafka, не блокируя выполнение обработки транзакции
            asyncio.create_task(send_confirmation(producer, TOPIC_CONFIRMATION, json.dumps(confirmation_msg).encode("utf-8")))

    except Exception as e:
        logger.exception(f"❌ Ошибка при обработке транзакции: {e}")
        tx_failed.inc()


async def limited_process(tx, producer):
    """Ограничение параллельности для обработки транзакций"""
    async with semaphore:
        await process_transaction(tx, producer)


async def consume():
    """Основной цикл для потребления транзакций из Kafka и обработки их"""
    await db.init()
    logger.info("🚀 Подключение к базе данных инициализировано")

    consumer = await get_kafka_consumer(TOPIC_VALIDATED, group_id="worker-group")
    producer = await get_kafka_producer()
    tasks = set()

    try:
        async for msg in consumer:
            # Логирование получения сообщения из Kafka
            logger.info(f"📥 Получено сообщение из Kafka: {msg.value}")
            tx = json.loads(msg.value)
            task = asyncio.create_task(limited_process(tx, producer))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    except Exception as e:
        logger.exception(f"❌ Ошибка в цикле потребителя: {e}")

    finally:
        logger.info("🛑 Завершение работы consumer'а и producer'а...")
        await consumer.stop()
        await producer.stop()
        await db.close()

        if tasks:
            logger.info("⏳ Ожидание завершения оставшихся задач...")
            await asyncio.gather(*tasks, return_exceptions=True)


# ────────────────────────────
if __name__ == "__main__":
    start_http_server(8001)  # Запуск метрик на порту 8001
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по Ctrl+C")
