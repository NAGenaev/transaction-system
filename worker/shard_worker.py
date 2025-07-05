import asyncio
import json
import os
import logging
from common.kafka import get_kafka_consumer, get_kafka_producer
from common.db import db
from datetime import datetime
import pyfiglet
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError
from collections import deque
from typing import List, Dict, Any
import time

# ────────────────────────────
# Импорт преременных окружения 
SHARD_ID = os.getenv("SHARD_ID", "0")
WORKER_CONFIRMATION_TOPIC = os.getenv("WORKER_CONFIRMATION_TOPIC")
WORKER_DB_HOST = os.getenv("WORKER_DB_HOST")
WORKER_DB_PORT = int(os.getenv("WORKER_DB_PORT"))
WORKER_DB_USER = os.getenv("WORKER_DB_USER")
WORKER_DB_PASSWORD = os.getenv("WORKER_DB_PASSWORD")
WORKER_DB_DATABASE = os.getenv("WORKER_DB_DATABASE")
WORKER_DBPOOL_MIN_SIZE = int(os.getenv("WORKER_DBPOOL_MIN_SIZE"))
WORKER_DBPOOL_MAX_SIZE = int(os.getenv("WORKER_DBPOOL_MAX_SIZE"))
WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT"))
WORKER_MAX_CONCURRENT_TASKS = int(os.getenv("WORKER_MAX_CONCURRENT_TASKS", "50"))
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "200"))
WORKER_BATCH_TIMEOUT_MS = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "500"))
WORKER_LOGLVL = os.getenv("WORKER_LOGLVL", "INFO").upper()
WORKER_KAFKA_LOGLVL = os.getenv("WORKER_KAFKA_LOGLVL", "INFO").upper()

# ────────────────────────────
# Конфигурация
CONFIG = {
    "MAX_CONCURRENT_TASKS": WORKER_MAX_CONCURRENT_TASKS,
    "BATCH_SIZE": WORKER_BATCH_SIZE,
    "BATCH_TIMEOUT_MS": WORKER_BATCH_TIMEOUT_MS,
    "KAFKA_PRODUCER_CONFIG": {
        "max_batch_size": 32768,
        "linger_ms": 20,
        "compression_type": "snappy",
        "max_request_size": 1048576  # 1MB
    },
    "KAFKA_CONSUMER_CONFIG": {
        "max_poll_records": 200,
        "fetch_max_bytes": 1048576 * 2,  # 2MB
        "session_timeout_ms": 45000
    },
    "VALIDATED_TOPIC": f"validated-transactions-{SHARD_ID}",
    "CONFIRMATION_TOPIC": WORKER_CONFIRMATION_TOPIC,
    "METRICS_PORT": WORKER_METRICS_PORT,
    "DB_CONFIG": {
        "host": WORKER_DB_HOST,
        "port": WORKER_DB_PORT,
        "user": WORKER_DB_USER,
        "password": WORKER_DB_PASSWORD,
        "database": WORKER_DB_DATABASE,
        "min_size": WORKER_DBPOOL_MIN_SIZE,
        "max_size": WORKER_DBPOOL_MAX_SIZE
    }
}

# ────────────────────────────
# Метрики Prometheus
TX_PROCESSED = Counter('shard_transactions_processed', 'Processed transactions', ['shard_id'])
TX_FAILED = Counter('shard_transactions_failed', 'Failed transactions', ['shard_id'])
TX_INSUFFICIENT = Counter('shard_insufficient_funds', 'Insufficient funds', ['shard_id'])
TX_NOT_FOUND = Counter('shard_account_not_found', 'Account not found', ['shard_id'])
PROCESSING_TIME = Histogram('shard_processing_time', 'Transaction processing time', ['shard_id'])
BATCH_SIZE = Gauge('shard_batch_size', 'Current batch size', ['shard_id'])

# ────────────────────────────
# Логгирование
KAFKA_LOGLEVEL = getattr(logging, WORKER_KAFKA_LOGLVL, logging.INFO) # Модуль KAFKA
logging.getLogger("aiokafka").setLevel(KAFKA_LOGLEVEL)

logging.basicConfig(
    level=getattr(logging, WORKER_LOGLVL, logging.INFO), # APP LOGLVL
    format=f"%(asctime)s [%(levelname)s] shard-{SHARD_ID}: %(message)s",
)
logger = logging.getLogger(f"worker-shard-{SHARD_ID}")


class ShardWorker:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_TASKS"])
        self.producer = None
        self.consumer = None
        self.pending_batch = deque()
        self.batch_lock = asyncio.Lock()
        self.metrics_labels = {'shard_id': SHARD_ID}

    async def init_resources(self):
        try:
            await db.initialize()
            self.producer = await get_kafka_producer(**CONFIG["KAFKA_PRODUCER_CONFIG"])
            self.consumer = await get_kafka_consumer(
                CONFIG["VALIDATED_TOPIC"],
                group_id=f"worker-group-{SHARD_ID}",
                **CONFIG["KAFKA_CONSUMER_CONFIG"]
            )
            logger.info(f"Worker for shard {SHARD_ID} initialized")
            logger.debug(f"DB Config: {CONFIG['DB_CONFIG']}")
            logger.debug(f"Kafka Consumer Topic: {CONFIG['VALIDATED_TOPIC']}")
        except Exception as e:
            logger.error(f"Failed to initialize resources: {e}")
            logger.exception("Exception during init_resources")
            raise

    async def _update_balance(self, conn, account: str, amount: int) -> bool:
        result = await conn.execute(
            """
            UPDATE accounts 
            SET balance = balance + $1 
            WHERE account_number = $2 AND balance + $1 >= 0
            RETURNING 1
            """,
            amount, account
        )
        return bool(result)

    async def _process_transaction(self, conn, tx: Dict[str, Any]) -> Dict[str, Any]:
        # Проверяем, нет ли уже такой транзакции в БД
        existing_tx = await conn.fetchrow(
            "SELECT id FROM transactions WHERE transaction_id = $1",
            tx["transaction_id"]  # Используем UUID из запроса
        )
        if existing_tx:
            logger.warning(f"Transaction {tx['transaction_id']} already processed, skipping")
            logger.debug(f"Duplicate transaction detected: {tx}")
            return {
                "status": "duplicate",
                "transaction_id": existing_tx["id"],
                "sender_account": tx["sender_account"],
                "receiver_account": tx["receiver_account"],
                "shard_id": SHARD_ID
            }
        
        start_time = time.time()
        try:
            sender = tx["sender_account"]
            receiver = tx["receiver_account"]
            amount = tx["amount"]

            accounts = await conn.fetch(
                "SELECT account_number FROM accounts WHERE account_number IN ($1, $2) FOR UPDATE",
                sender, receiver
            )
            if len(accounts) != 2:
                TX_NOT_FOUND.labels(**self.metrics_labels).inc()
                raise ValueError("Account not found")

            if not await self._update_balance(conn, sender, -amount):
                TX_INSUFFICIENT.labels(**self.metrics_labels).inc()
                raise ValueError("Insufficient funds")

            await self._update_balance(conn, receiver, amount)

            tx_id = await conn.fetchrow(
                """INSERT INTO transactions 
                (sender_account, receiver_account, amount, timestamp, transaction_id)
                VALUES ($1, $2, $3, $4, $5) RETURNING id""",
                sender, receiver, amount, datetime.utcnow(), tx["transaction_id"]
            )

            PROCESSING_TIME.labels(**self.metrics_labels).observe(time.time() - start_time)
            TX_PROCESSED.labels(**self.metrics_labels).inc()

            return {
                "transaction_id": tx_id['id'],
                "status": "processed",
                "sender_account": sender,
                "receiver_account": receiver,
                "amount": amount,
                "shard_id": SHARD_ID
            }
        except Exception as e:
            TX_FAILED.labels(**self.metrics_labels).inc()
            logger.error(f"Transaction failed: {e}")
            # ВОЗВРАЩАЕМ ИНФОРМАЦИЮ ОБ ОШИБКЕ ДЛЯ ПОДТВЕРЖДЕНИЯ
            return {
                "status": "failed",
                "error": str(e),
                "transaction_id": tx.get("transaction_id", "unknown"),
                "sender_account": tx["sender_account"],
                "receiver_account": tx["receiver_account"],
                "shard_id": SHARD_ID
            }



    async def process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        confirmations = []
        tasks = []

        for tx in batch:
            task = asyncio.create_task(self._process_transaction_with_connection(tx))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            # ОБРАБАТЫВАЕМ КАК УСПЕШНЫЕ, ТАК И ОШИБОЧНЫЕ РЕЗУЛЬТАТЫ
            if not isinstance(result, Exception):
                confirmations.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Task failed with exception: {str(result)}")
                # СОЗДАЕМ ПОДТВЕРЖДЕНИЕ ДЛЯ ОШИБКИ
                # (в реальном коде нужно определить отправителя/получателя)
                confirmations.append({
                    "status": "failed",
                    "error": str(result),
                    "shard_id": SHARD_ID
                })

        BATCH_SIZE.labels(**self.metrics_labels).set(len(confirmations))
        logger.debug(f"Batch results: {confirmations}")
        return confirmations
        
    async def _process_transaction_with_connection(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        try:
            async with db.pool.acquire() as conn:
                async with conn.transaction():
                    result = await self._process_transaction(conn, tx)
                
                    # ЛОГИРУЕМ РЕЗУЛЬТАТ ОБРАБОТКИ
                    if result.get("status") == "processed":
                        logger.info(
                            f"Transaction {tx['transaction_id']} processed successfully: "
                            f"{result['sender_account']} → {result['receiver_account']}, "
                            f"amount: {result['amount']}"
                        )
                    else:
                        logger.warning(
                            f"Transaction {tx['transaction_id']} failed: "
                            f"{result.get('error', 'Unknown error')}"
                        )
                
                    return result
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return {
               "status": "failed",
                "error": str(e),
                "transaction_id": tx.get("transaction_id", "unknown"),
                "sender_account": tx.get("sender_account", "unknown"),
                "receiver_account": tx.get("receiver_account", "unknown"),
                "shard_id": SHARD_ID
            }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(KafkaTimeoutError)
    )

    async def send_confirmation_batch(self, confirmations: List[Dict[str, Any]]):
        if not confirmations:
            return
        
        tasks = [
            self.producer.send_and_wait(
                CONFIG["CONFIRMATION_TOPIC"],
                value=json.dumps(conf).encode("utf-8"),
                key=str(conf["transaction_id"]).encode()
            )
            for conf in confirmations
        ]
        await asyncio.gather(*tasks)
        logger.info(f"Sent {len(confirmations)} confirmations")
        logger.debug(f"Confirmations payload: {confirmations}")

    async def batch_processor(self):
        while True:
            await asyncio.sleep(CONFIG["BATCH_TIMEOUT_MS"] / 1000)
            async with self.batch_lock:
                if not self.pending_batch:
                    continue

                batch = list(self.pending_batch)
                self.pending_batch.clear()

            logger.info(f"Processing batch of {len(batch)} transactions")

            try:
                confirmations = await self.process_batch(batch)
                await self.send_confirmation_batch(confirmations)
                
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")

    async def consume_messages(self):
        async for msg in self.consumer:
            try:
                tx = json.loads(msg.value)
                logger.debug(f"Received transaction: {tx}")
                async with self.batch_lock:
                    self.pending_batch.append(tx)
                    if len(self.pending_batch) >= CONFIG["BATCH_SIZE"]:
                        batch = list(self.pending_batch)
                        self.pending_batch.clear()

                        confirmations = await self.process_batch(batch)
                        await self.send_confirmation_batch(confirmations)
            except Exception as e:
                logger.error(f"Message processing error: {e}")

    async def run(self):
        await self.init_resources()
        processor_task = asyncio.create_task(self.batch_processor())

        try:
            await self.consume_messages()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            processor_task.cancel()

            async with self.batch_lock:
                if self.pending_batch:
                    try:
                        confirmations = await self.process_batch(list(self.pending_batch))
                        await self.send_confirmation_batch(confirmations)
                    except Exception as e:
                        logger.error(f"Final batch processing failed: {e}")

            await self.producer.stop()
            await self.consumer.stop()
            await db.close()


if __name__ == "__main__":
    start_http_server(CONFIG["METRICS_PORT"])
    print(pyfiglet.figlet_format(f"SHARD {SHARD_ID}", font="slant"))
    logger.info(f"Starting ShardWorker for shard {SHARD_ID} with config: {CONFIG}")
    worker = ShardWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
