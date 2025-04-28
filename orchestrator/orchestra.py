import asyncio
import json
import logging
from enum import Enum
from common.kafka import get_kafka_producer, get_kafka_consumer
from prometheus_client import Counter, start_http_server
import pyfiglet
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError
import redis.asyncio as aioredis

# ────────────────────────────
# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ────────────────────────────
ascii_banner = pyfiglet.figlet_format("TTTS ORCHESTRATOR 0.0.3", font="slant")
print(ascii_banner)

# ────────────────────────────
# METRICS
orch_received = Counter("orchestrator_received_total", "Total transactions received from API")
orch_sent = Counter("orchestrator_sent_total", "Total transactions sent to worker")
orch_queued = Counter("orchestrator_queued_total", "Transactions queued due to active lock")
orch_confirmation_received = Counter("orchestrator_confirmation_received_total", "Total confirmations received")

# ────────────────────────────
TOPIC_API_TO_ORCH = "transaction-events"
TOPIC_CONFIRMATION = "transaction-confirmation"
SHARD_COUNT = 16

REDIS_URL = "redis://redis:6379"  # адаптируй если нужно

MAX_PARALLEL_SENDS = 5000
send_semaphore = asyncio.Semaphore(MAX_PARALLEL_SENDS)

# ────────────────────────────
# ACCOUNT STATE MACHINE

class AccountState(Enum):
    IDLE = "idle"
    BUSY = "busy"

def get_shard(account_id: str) -> int:
    return hash(account_id) % SHARD_COUNT

# ────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5),
    retry=retry_if_exception_type(KafkaTimeoutError)
)
async def send_with_retry(producer, topic, msg: bytes):
    async with send_semaphore:
        await producer.send(topic, msg)

# ────────────────────────────

async def lock_accounts(redis, sender: str, receiver: str) -> bool:
    """Пытаемся занять аккаунты."""
    try:
        pipe = redis.pipeline()
        pipe.setnx(f"lock:{sender}", "locked")
        pipe.setnx(f"lock:{receiver}", "locked")
        results = await pipe.execute()

        logger.info(f"🔒 Попытка залочить аккаунты {sender} и {receiver}: {'успех' if all(results) else 'неудача'}")
        # Если оба аккаунта успешно залочены — успех
        return all(results)
    except Exception as e:
        logger.error(f"❌ Ошибка при блокировке аккаунтов {sender} и {receiver}: {e}")
        return False

async def unlock_accounts(redis, sender: str, receiver: str):
    """Освобождаем аккаунты."""
    try:
        await redis.delete(f"lock:{sender}", f"lock:{receiver}")
        logger.info(f"🔓 Аккаунты {sender} и {receiver} успешно разблокированы")
    except Exception as e:
        logger.error(f"❌ Ошибка при разблокировке аккаунтов {sender} и {receiver}: {e}")

async def enqueue_transaction(redis, tx: dict):
    """Кладем транзакцию в очередь."""
    sender = tx["sender_account"]
    receiver = tx["receiver_account"]
    queue_key = f"queue:{sender}:{receiver}"
    try:
        await redis.rpush(queue_key, json.dumps(tx))
        logger.info(f"📥 Транзакция {tx['transaction_id']} поставлена в очередь {queue_key}")
    except Exception as e:
        logger.error(f"❌ Ошибка помещения транзакции {tx['transaction_id']} в очередь: {e}")

async def dequeue_transaction(redis, sender: str, receiver: str):
    """Вытаскиваем следующую транзакцию из очереди."""
    queue_key = f"queue:{sender}:{receiver}"
    try:
        tx_data = await redis.lpop(queue_key)
        if tx_data:
            tx = json.loads(tx_data)
            logger.info(f"📤 Извлечена транзакция {tx['transaction_id']} из очереди {queue_key}")
            return tx
        else:
            logger.info(f"ℹ️ Очередь {queue_key} пуста")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка извлечения транзакции из очереди {queue_key}: {e}")
        return None

# ────────────────────────────

async def handle_transaction(tx: dict, producer, redis, shard_id: int):
    sender = tx["sender_account"]
    receiver = tx["receiver_account"]
    target_topic = f"shard-{shard_id}-validated"

    try:
        logger.info(f"📤 Отправка транзакции: {tx} в shard {shard_id}")
        await send_with_retry(producer, target_topic, json.dumps(tx).encode("utf-8"))
        orch_sent.inc()
        logger.info(f"✅ Транзакция {tx['transaction_id']} отправлена в shard {shard_id} и ожидает подтверждения")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки транзакции: {e}")
        await unlock_accounts(redis, sender, receiver)

async def try_next_in_queue(redis, producer, sender: str, receiver: str, shard_id: int):
    next_tx = await dequeue_transaction(redis, sender, receiver)
    if next_tx:
        logger.info(f"🔁 Следующая транзакция для пары {sender}-{receiver} в shard {shard_id}")
        await handle_transaction(next_tx, producer, redis, shard_id)
    else:
        await unlock_accounts(redis, sender, receiver)

# ────────────────────────────

async def handle_confirmation(msg: dict, producer, redis):
    transaction_id = msg["transaction_id"]
    sender = msg["sender_account"]
    receiver = msg["receiver_account"]
    shard_id = get_shard(sender)

    logger.info(f"✅ Подтверждение: транзакция ID={transaction_id}, sender={sender}, receiver={receiver}")
    orch_confirmation_received.inc()

    # Переход обратно в IDLE или обработка очереди
    await try_next_in_queue(redis, producer, sender, receiver, shard_id)

# ────────────────────────────

async def orchestrate():
    consumer = await get_kafka_consumer(topic=TOPIC_API_TO_ORCH, group_id="orchestra-group")
    confirmation_consumer = await get_kafka_consumer(topic=TOPIC_CONFIRMATION, group_id="confirmation-group")
    producer = await get_kafka_producer()
    redis = await aioredis.from_url(REDIS_URL)

    try:
        async def consume_transactions():
            async for msg in consumer:
                try:
                    tx = json.loads(msg.value)
                    sender = tx["sender_account"]
                    receiver = tx["receiver_account"]
                    orch_received.inc()

                    logger.info(f"📥 Получена транзакция: {tx}")

                    shard_id = get_shard(sender)

                    locked = await lock_accounts(redis, sender, receiver)
                    if locked:
                        await handle_transaction(tx, producer, redis, shard_id)
                    else:
                        await enqueue_transaction(redis, tx)
                        orch_queued.inc()
                        logger.info(f"⏳ Транзакция {tx['transaction_id']} поставлена в очередь для {sender}-{receiver}")

                except Exception as handle_error:
                    logger.error(f"❌ Ошибка обработки транзакции: {handle_error}")

        async def consume_confirmations():
            async for msg in confirmation_consumer:
                try:
                    confirmation = json.loads(msg.value)
                    await handle_confirmation(confirmation, producer, redis)
                except Exception as confirmation_error:
                    logger.error(f"❌ Ошибка обработки подтверждения: {confirmation_error}")

        await asyncio.gather(consume_transactions(), consume_confirmations())

    except Exception as kafka_error:
        logger.error(f"❌ Ошибка Kafka Consumer: {kafka_error}")

    finally:
        await consumer.stop()
        await confirmation_consumer.stop()
        await producer.stop()
        await redis.close()

# ────────────────────────────

if __name__ == "__main__":
    start_http_server(8002)  # Метрики по адресу :8002/metrics
    asyncio.run(orchestrate())
