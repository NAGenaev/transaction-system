import asyncio
import json
import logging
from enum import Enum
from common.kafka import get_kafka_producer, get_kafka_consumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import pyfiglet
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError
import redis.asyncio as aioredis
import time

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ==================== МЕТРИКИ PROMETHEUS ====================
# Счётчики
orch_received = Counter("orchestrator_received_total", "Total transactions received from API")
orch_sent = Counter("orchestrator_sent_total", "Total transactions sent to worker")
orch_queued = Counter("orchestrator_queued_total", "Transactions queued due to active lock")
orch_confirmation_received = Counter("orchestrator_confirmation_received_total", "Total confirmations received")

# Гейджи
active_locks = Gauge("orchestrator_active_locks", "Current number of active locks")
queue_size = Gauge("orchestrator_queue_size", "Current transactions in queue")

# Гистограммы
processing_time = Histogram("orchestrator_processing_time", "Transaction processing time in seconds", buckets=[0.01, 0.05, 0.1, 0.5, 1, 5])

# ==================== КОНСТАНТЫ ====================
TOPIC_API_TO_ORCH = "transaction-events"
TOPIC_CONFIRMATION = "transaction-confirmation"
SHARD_COUNT = 16
REDIS_URL = "redis://redis:6379"
MAX_PARALLEL_SENDS = 5000
LOCK_TIMEOUT = 30  # секунд

# ==================== REDIS SCRIPTS ====================
LOCK_SCRIPT = """
local sender_key = KEYS[1]
local receiver_key = KEYS[2]
local timeout = ARGV[1]

if redis.call('SET', sender_key, 'locked', 'NX', 'EX', timeout) and 
   redis.call('SET', receiver_key, 'locked', 'NX', 'EX', timeout) then
    return 1
else
    return 0
end
"""

UNLOCK_SCRIPT = """
redis.call('DEL', KEYS[1], KEYS[2])
return 1
"""

class AccountState(Enum):
    IDLE = "idle"
    BUSY = "busy"

def get_shard(account_id: str) -> int:
    return hash(account_id) % SHARD_COUNT

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.1, max=1),
    retry=retry_if_exception_type(KafkaTimeoutError)
)
async def send_with_retry(producer, topic, msg: bytes):
    start_time = time.time()
    try:
        await producer.send_and_wait(topic, msg)
    finally:
        processing_time.observe(time.time() - start_time)

async def lock_accounts(redis, sender: str, receiver: str) -> bool:
    """Атомарная блокировка с обновлением метрик"""
    try:
        script = redis.register_script(LOCK_SCRIPT)
        result = await script(keys=[f"lock:{sender}", f"lock:{receiver}"], args=[LOCK_TIMEOUT])
        if result:
            active_locks.inc(2)  # Увеличиваем на 2 (отправитель + получатель)
        return bool(result)
    except Exception as e:
        logger.error(f"Lock error: {str(e)}")
        return False

async def unlock_accounts(redis, sender: str, receiver: str):
    """Атомарная разблокировка с обновлением метрик"""
    try:
        script = redis.register_script(UNLOCK_SCRIPT)
        await script(keys=[f"lock:{sender}", f"lock:{receiver}"])
        active_locks.dec(2)  # Уменьшаем на 2
    except Exception as e:
        logger.error(f"Unlock error: {str(e)}")

async def update_queue_metrics(redis):
    """Обновление метрик очереди"""
    while True:
        try:
            # Подсчёт всех элементов во всех очередях
            keys = await redis.keys("queue:*")
            total = 0
            if keys:
                lengths = await redis.mget(*keys)
                total = sum(int(length) for length in lengths if length)
            queue_size.set(total)
        except Exception as e:
            logger.error(f"Queue metrics error: {e}")
        await asyncio.sleep(5)  # Обновляем каждые 5 секунд

async def handle_transaction(tx: dict, producer, redis, shard_id: int):
    sender = tx["sender_account"]
    receiver = tx["receiver_account"]
    target_topic = f"shard-{shard_id}-validated"

    try:
        await send_with_retry(producer, target_topic, json.dumps(tx).encode("utf-8"))
        orch_sent.inc()
        logger.info(f"Transaction {tx['transaction_id']} sent to shard {shard_id}")
    except Exception as e:
        logger.error(f"Error sending transaction: {e}")
        await unlock_accounts(redis, sender, receiver)

async def process_transactions(consumer, producer, redis):
    """Обработка входящих транзакций"""
    async for msg in consumer:
        try:
            tx = json.loads(msg.value)
            orch_received.inc()
            
            sender = tx["sender_account"]
            receiver = tx["receiver_account"]
            shard_id = get_shard(sender)

            if await lock_accounts(redis, sender, receiver):
                await handle_transaction(tx, producer, redis, shard_id)
            else:
                orch_queued.inc()
                await redis.rpush(f"queue:{sender}:{receiver}", json.dumps(tx))
                logger.info(f"Transaction {tx['transaction_id']} queued")
        except Exception as e:
            logger.error(f"Transaction processing error: {e}")

async def process_confirmations(consumer, producer, redis):
    """Обработка подтверждений"""
    async for msg in consumer:
        try:
            confirmation = json.loads(msg.value)
            orch_confirmation_received.inc()
            
            sender = confirmation["sender_account"]
            receiver = confirmation["receiver_account"]
            
            next_tx = await redis.lpop(f"queue:{sender}:{receiver}")
            if next_tx:
                tx = json.loads(next_tx)
                shard_id = get_shard(sender)
                await handle_transaction(tx, producer, redis, shard_id)
            else:
                await unlock_accounts(redis, sender, receiver)
        except Exception as e:
            logger.error(f"Confirmation processing error: {e}")

async def orchestrate():
    # Инициализация компонентов
    consumer = await get_kafka_consumer(TOPIC_API_TO_ORCH, "orchestrator-group")
    confirmation_consumer = await get_kafka_consumer(TOPIC_CONFIRMATION, "confirmation-group")
    producer = await get_kafka_producer()
    redis = await aioredis.from_url(REDIS_URL, max_connections=100)

    # Запуск фоновой задачи для обновления метрик очереди
    metrics_task = asyncio.create_task(update_queue_metrics(redis))

    try:
        # Параллельная обработка транзакций и подтверждений
        await asyncio.gather(
            process_transactions(consumer, producer, redis),
            process_confirmations(confirmation_consumer, producer, redis)
        )
    except Exception as e:
        logger.error(f"Orchestrator error: {e}")
    finally:
        metrics_task.cancel()
        await asyncio.gather(
            consumer.stop(),
            confirmation_consumer.stop(),
            producer.stop(),
            redis.close(),
            return_exceptions=True
        )

if __name__ == "__main__":
    # Запуск HTTP сервера для метрик на порту 8002
    start_http_server(8002)
    
    # Печать баннера
    ascii_banner = pyfiglet.figlet_format("TTTS ORCHESTRATOR", font="slant")
    print(ascii_banner)
    
    # Запуск оркестратора
    asyncio.run(orchestrate())