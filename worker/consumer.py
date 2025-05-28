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
# Конфигурация
SHARD_ID = os.getenv("SHARD_ID", "0")
CONFIG = {
    "MAX_CONCURRENT_TASKS": 50,
    "BATCH_SIZE": 100,
    "BATCH_TIMEOUT_MS": 200,
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
    "CONFIRMATION_TOPIC": "transaction-confirmation",
    "METRICS_PORT": 8001,
    "DB_CONFIG": {
        "host": "postgres",
        "port": 5432,
        "user": "user",
        "password": "password",
        "database": "transactions",
        "min_size": 5,
        "max_size": 20
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
logging.basicConfig(
    level=logging.INFO,
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
        except Exception as e:
            logger.error(f"Failed to initialize resources: {e}")
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
                (sender_account, receiver_account, amount, timestamp) 
                VALUES ($1, $2, $3, $4) RETURNING id""",
                sender, receiver, amount, datetime.utcnow()
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
            raise

    async def process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        confirmations = []
        await db.initialize()
        tasks = []

        for tx in batch:
            task = asyncio.create_task(self._process_transaction_with_connection(tx))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if not isinstance(result, Exception):
                confirmations.append(result)

        BATCH_SIZE.labels(**self.metrics_labels).set(len(confirmations))
        return confirmations

    async def _process_transaction_with_connection(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                return await self._process_transaction(conn, tx)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(KafkaTimeoutError)
    )

    async def send_confirmation_batch(self, confirmations: List[Dict[str, Any]]):
        if not confirmations:
            return

        for conf in confirmations:
            key = str(conf["transaction_id"]).encode()
            value = json.dumps(conf).encode("utf-8")
            await self.producer.send_and_wait(
                CONFIG["CONFIRMATION_TOPIC"],
                value=value,
                key=key
            )
        logger.info(f"Sent {len(confirmations)} confirmations")

    async def batch_processor(self):
        while True:
            await asyncio.sleep(CONFIG["BATCH_TIMEOUT_MS"] / 1000)
            async with self.batch_lock:
                if not self.pending_batch:
                    continue

                batch = list(self.pending_batch)
                self.pending_batch.clear()

            try:
                confirmations = await self.process_batch(batch)
                await self.send_confirmation_batch(confirmations)
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")

    async def consume_messages(self):
        async for msg in self.consumer:
            try:
                tx = json.loads(msg.value)
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

    worker = ShardWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
