import asyncio
import json
import logging
from collections import defaultdict, deque
from enum import Enum
from common.kafka import get_kafka_producer, get_kafka_consumer
from prometheus_client import Counter, start_http_server
import pyfiglet
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError

# ────────────────────────────
# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
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

MAX_PARALLEL_SENDS = 1000
send_semaphore = asyncio.Semaphore(MAX_PARALLEL_SENDS)

# ────────────────────────────
# ACCOUNT STATE MACHINE

class AccountState(Enum):
    IDLE = "idle"
    PROCESSING = "processing"
    WAITING_CONFIRMATION = "waiting_confirmation"

account_states = defaultdict(lambda: defaultdict(lambda: AccountState.IDLE))  # shard_id -> account_id -> state
account_queues = defaultdict(lambda: defaultdict(deque))  # shard_id -> account_id -> queue

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

async def handle_transaction(tx: dict, producer, shard_id: int):
    sender_account = tx["sender_account"]
    target_topic = f"shard-{shard_id}-validated"

    try:
        logger.info(f"📤 Отправка транзакции: {tx} в shard {shard_id}")
        await send_with_retry(producer, target_topic, json.dumps(tx).encode("utf-8"))
        orch_sent.inc()
        account_states[shard_id][sender_account] = AccountState.WAITING_CONFIRMATION
        logger.info(f"✅ Транзакция {tx['transaction_id']} отправлена в shard {shard_id} и ожидает подтверждения")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке транзакции: {e}")
        account_states[shard_id][sender_account] = AccountState.IDLE

async def try_next_in_queue(shard_id: int, account_id: str, producer):
    queue = account_queues[shard_id][account_id]
    if queue:
        next_tx = queue.popleft()
        logger.info(f"🔁 Следующая транзакция для {account_id} в shard {shard_id}")
        account_states[shard_id][account_id] = AccountState.PROCESSING
        await handle_transaction(next_tx, producer, shard_id)
    else:
        account_states[shard_id][account_id] = AccountState.IDLE

# ────────────────────────────

async def handle_confirmation(msg: dict, producer):
    transaction_id = msg["transaction_id"]
    sender_account = msg["sender_account"]
    shard_id = get_shard(sender_account)

    logger.info(f"✅ Подтверждение: транзакция ID={transaction_id}, account={sender_account}")
    orch_confirmation_received.inc()

    # Переход обратно в IDLE или обработка очереди
    await try_next_in_queue(shard_id, sender_account, producer)

# ────────────────────────────

async def orchestrate():
    consumer = await get_kafka_consumer(topic=TOPIC_API_TO_ORCH, group_id="orchestra-group")
    confirmation_consumer = await get_kafka_consumer(topic=TOPIC_CONFIRMATION, group_id="confirmation-group")
    producer = await get_kafka_producer()

    try:
        async def consume_transactions():
            async for msg in consumer:
                try:
                    tx = json.loads(msg.value)
                    sender_account = tx["sender_account"]
                    orch_received.inc()

                    logger.info(f"📥 Получена транзакция: {tx}")

                    shard_id = get_shard(sender_account)
                    state = account_states[shard_id][sender_account]

                    if state in (AccountState.PROCESSING, AccountState.WAITING_CONFIRMATION):
                        account_queues[shard_id][sender_account].append(tx)
                        orch_queued.inc()
                        logger.info(f"⏳ Очередь в shard {shard_id} для {sender_account}")
                    else:
                        account_states[shard_id][sender_account] = AccountState.PROCESSING
                        await handle_transaction(tx, producer, shard_id)

                except Exception as handle_error:
                    logger.error(f"❌ Ошибка обработки транзакции: {handle_error}")

        async def consume_confirmations():
            async for msg in confirmation_consumer:
                try:
                    confirmation = json.loads(msg.value)
                    await handle_confirmation(confirmation, producer)
                except Exception as confirmation_error:
                    logger.error(f"❌ Ошибка обработки подтверждения: {confirmation_error}")

        await asyncio.gather(consume_transactions(), consume_confirmations())

    except Exception as kafka_error:
        logger.error(f"❌ Ошибка Kafka Consumer: {kafka_error}")

    finally:
        await consumer.stop()
        await confirmation_consumer.stop()
        await producer.stop()

# ────────────────────────────

if __name__ == "__main__":
    start_http_server(8002)  # Метрики по адресу :8002/metrics
    asyncio.run(orchestrate())
