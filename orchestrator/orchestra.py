import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from common.kafka import get_kafka_producer, get_kafka_consumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiokafka.errors import KafkaTimeoutError
import redis.asyncio as aioredis
from aiokafka.structs import RecordMetadata
from asyncio import CancelledError, TimeoutError

# ==================== КОНФИГУРАЦИЯ ====================
ORCHESTRATOR_KAFKA_LOGLVL = os.getenv("ORCHESTRATOR_KAFKA_LOGLVL", "INFO").upper()

CONFIG = {
    "TOPIC_API_TO_ORCH": "transaction-events",
    "TOPIC_CONFIRMATION": "transaction-confirmation",
    "SHARD_COUNT": 2,
    "REDIS_URL": "redis://redis:6379",
    "REDIS_TIMEOUT": 5.0,
    "REDIS_CONNECT_TIMEOUT": 3.0,
    "SHUTDOWN_TIMEOUT": 10.0,
    "MAX_PARALLEL_WORKERS": 1000,
    "BATCH_SIZE": 500,
    "LOCK_TIMEOUT": 1,
    "METRICS_PORT": 8002,
    "KAFKA_PRODUCER_CONFIG": {
        "linger_ms": 50,
        "compression_type": "snappy",
        "max_batch_size": 16384,
        "request_timeout_ms": 30000
    },
    "KAFKA_CONSUMER_CONFIG": {
        "max_poll_records": 200,
        "fetch_max_bytes": 1048576
    }
}

# ==================== ИНИЦИАЛИЗАЦИЯ ЛОГГЕРА ====================
KAFKA_LOGLEVEL = getattr(logging, ORCHESTRATOR_KAFKA_LOGLVL, logging.INFO)
logging.getLogger("aiokafka").setLevel(KAFKA_LOGLEVEL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ==================== МЕТРИКИ ====================
class Metrics:
    received = Counter("orchestrator_received_total", "Total transactions received")
    sent = Counter("orchestrator_sent_total", "Total transactions sent")
    confirmations = Counter("orchestrator_confirmation_received_total", "Total confirmations")
    active_locks = Gauge("orchestrator_active_locks", "Current active locks")
    queue_size = Gauge("orchestrator_queue_size", "Current queue size")
    redis_connections = Gauge("orchestrator_redis_connections", "Active Redis connections")
    processing_time = Histogram("orchestrator_processing_time", "Processing time in seconds", 
                              buckets=[0.01, 0.05, 0.1, 0.5, 1, 5])
    kafka_errors = Counter("orchestrator_kafka_errors", "Kafka errors", ["type"])
    redis_errors = Counter("orchestrator_redis_errors", "Redis errors", ["type"])
    redis_queue_items = Gauge("orchestrator_redis_queue_items", "Items in Redis queues")

# ==================== REDIS КЛИЕНТ ====================
class RedisClient:
    _instance = None
    
    def __init__(self):
        self.redis = None
        self._connection_pool = None
        self.lock_script = None
        self.unlock_script = None
    
    @classmethod
    async def get(cls):
        if cls._instance is None:
            cls._instance = RedisClient()
            await cls._instance.initialize()
        return cls._instance
    
    async def initialize(self):
        self._connection_pool = aioredis.ConnectionPool.from_url(
            CONFIG["REDIS_URL"],
            max_connections=CONFIG["MAX_PARALLEL_WORKERS"] + 20,
            socket_timeout=CONFIG["REDIS_TIMEOUT"],
            socket_connect_timeout=CONFIG["REDIS_CONNECT_TIMEOUT"]
        )
        logger.info(f"Redis pool created. Max connections: {self._connection_pool.max_connections}")

        self.redis = aioredis.Redis(connection_pool=self._connection_pool)
        
        # Инициализация скриптов
        self.lock_script = self.redis.register_script("""
            local results = {}
            for i = 1, #KEYS/2 do
                local sender_key = KEYS[2*i-1]
                local receiver_key = KEYS[2*i]
                local timeout = ARGV[1]
                
                -- Атомарная блокировка обоих счетов
                local locked_sender = redis.call('SET', sender_key, 'locked', 'NX', 'EX', timeout)
                local locked_receiver = redis.call('SET', receiver_key, 'locked', 'NX', 'EX', timeout)
                
                if locked_sender and locked_receiver then
                    results[#results+1] = 1
                else
                    -- Откатываем блокировку, если не удалось заблокировать оба счета
                    if locked_sender then
                        redis.call('DEL', sender_key)
                    end
                    if locked_receiver then
                        redis.call('DEL', receiver_key)
                    end
                    results[#results+1] = 0
                end
            end
            return results
        """)
        
        self.unlock_script = self.redis.register_script("""
            for i = 1, #KEYS/2 do
                local sender_key = KEYS[2*i-1]
                local receiver_key = KEYS[2*i]
                redis.call('DEL', sender_key, receiver_key)
            end
            return #KEYS/2
        """)

        # Мониторинг соединений
        asyncio.create_task(self._monitor_connections())
        logger.info("Redis scripts registered")

    async def _monitor_connections(self):
        """Мониторинг состояния соединений Redis"""
        try:
            while True:
                try:
                    if self.redis:
                        info = await self.redis.info('clients')
                        current_connections = info['connected_clients']
                        Metrics.redis_connections.set(current_connections)
                        if not hasattr(self, '_last_connection_count') or current_connections != self._last_connection_count:
                            logger.debug(f"Redis connections: {current_connections}")
                            self._last_connection_count = current_connections
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    logger.info("Redis monitoring task cancelled")
                    break
                except Exception as e:
                    logger.error(f"Redis monitoring error: {e}")
                    await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Fatal monitoring error: {e}")
    
    async def close(self):
        """Корректное закрытие соединений"""
        logger.info(f"Closing Redis connections. In use: {self._connection_pool._in_use_connections}")
        if self.redis:
            await self.redis.aclose()
        if self._connection_pool:
            await self._connection_pool.aclose()
        logger.info("Redis connections closed")
    
    async def batch_lock(self, pairs):
        """Пакетная блокировка счетов с улучшенной обработкой ошибок"""
        try:
            keys = []
            for sender, receiver in pairs:
                keys.extend([f"lock:{sender}", f"lock:{receiver}"])

            results = await asyncio.wait_for(
                self.lock_script(keys=keys, args=[CONFIG["LOCK_TIMEOUT"]]),
                timeout=CONFIG["REDIS_TIMEOUT"]
            )

            locked_pairs = []
            for i, (sender, receiver) in enumerate(pairs):
                if results[i] == 1:
                    locked_pairs.append((sender, receiver))
                    Metrics.active_locks.inc(2)
                    logger.debug(f"Locked accounts: sender={sender}, receiver={receiver}")
                else:
                    logger.debug(f"Failed to lock: sender={sender}, receiver={receiver}")
            return locked_pairs
        except asyncio.TimeoutError:
            logger.warning("Redis lock timeout")
            Metrics.redis_errors.labels("lock_timeout").inc()
            return []
        except Exception as e:
            logger.error(f"Lock error: {e}")
            Metrics.redis_errors.labels("lock_error").inc()
            return []
    
    async def batch_unlock(self, pairs):
        """Пакетная разблокировка счетов с логгированием"""
        try:
            keys = []
            for sender, receiver in pairs:
                keys.extend([f"lock:{sender}", f"lock:{receiver}"])
            
            result = await asyncio.wait_for(
                self.unlock_script(keys=keys),
                timeout=CONFIG["REDIS_TIMEOUT"]
            )
            
            Metrics.active_locks.dec(len(keys))
            logger.debug(f"Unlocked {len(pairs)} account pairs")
            return result
        except asyncio.TimeoutError:
            logger.warning("Redis unlock timeout")
            Metrics.redis_errors.labels("unlock_timeout").inc()
            return 0
        except Exception as e:
            logger.error(f"Unlock error: {e}")
            Metrics.redis_errors.labels("unlock_error").inc()
            return 0

# ==================== ОСНОВНАЯ ЛОГИКА ====================
class Orchestrator:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.confirmation_consumer = None
        self.redis = None
        self.queue = asyncio.Queue(maxsize=CONFIG["MAX_PARALLEL_WORKERS"] * 10)
        self.shutdown_event = asyncio.Event()
        self.pending_unlocks = []
        self.unlock_lock = asyncio.Lock()
        self.workers = []
        self.redis_queue_size = 0  # Трекер размера Redis-очереди

    async def redis_requeue_worker(self):
        """Перезагрузка отложенных транзакций из Redis"""
        while not self.shutdown_event.is_set():
            try:
                keys = await self.redis.redis.keys("queue:*")
                for key in keys:
                    while True:
                        tx_data = await self.redis.redis.lpop(key)
                        if not tx_data: 
                            break
                        try:
                            tx = json.loads(tx_data)
                            await self.queue.put([tx])
                            
                            # Обновляем счетчик очереди Redis
                            self.redis_queue_size -= 1
                            Metrics.redis_queue_items.set(self.redis_queue_size)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in queue: {key}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Queue reload error: {e}")
                await asyncio.sleep(5)

    async def initialize(self):
        """Инициализация ресурсов с обработкой ошибок"""
        try:
            self.producer = await get_kafka_producer(**CONFIG["KAFKA_PRODUCER_CONFIG"])
            self.consumer = await get_kafka_consumer(
                CONFIG["TOPIC_API_TO_ORCH"],
                "orchestrator-group",
                **CONFIG["KAFKA_CONSUMER_CONFIG"]
            )
            self.confirmation_consumer = await get_kafka_consumer(
                CONFIG["TOPIC_CONFIRMATION"],
                "confirmation-group",
                **CONFIG["KAFKA_CONSUMER_CONFIG"]
            )
            self.redis = await RedisClient.get()
            logger.info("Orchestrator resources initialized")
            
            # Инициализация размера очереди Redis
            await self.update_redis_queue_size()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    async def update_redis_queue_size(self):
        """Обновление счетчика элементов в Redis-очередях"""
        try:
            total_size = 0
            keys = await self.redis.redis.keys("queue:*")
            for key in keys:
                size = await self.redis.redis.llen(key)
                total_size += size
            self.redis_queue_size = total_size
            Metrics.redis_queue_items.set(total_size)
        except Exception as e:
            logger.error(f"Failed to update Redis queue size: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.1, max=1),
        retry=retry_if_exception_type(KafkaTimeoutError)
    )
    async def send_transaction_batch(self, transactions):
        """Надежная отправка батча транзакций"""
        start_time = time.time()
        try:
            # Группируем по шардам
            shard_msgs = defaultdict(list)
            for tx in transactions:
                shard_id = hash(tx["sender_account"]) % CONFIG["SHARD_COUNT"]
                topic = f"validated-transactions-{shard_id}"
                value = json.dumps(tx).encode('utf-8')
                key = str(tx.get("transaction_id", "")).encode()
                shard_msgs[topic].append((key, value))

            # Отправляем батчи для каждого шарда
            futures = []
            for topic, messages in shard_msgs.items():
                for key, value in messages:
                    futures.append(self.producer.send(topic, value=value, key=key))
            
            # Ожидаем подтверждения
            await asyncio.gather(*futures)
            Metrics.sent.inc(len(transactions))
            logger.debug(f"Sent {len(transactions)} transactions")
        except Exception as e:
            logger.error(f"Failed to send batch: {e}")
            Metrics.kafka_errors.labels("send_error").inc()
            raise
        finally:
            Metrics.processing_time.observe(time.time() - start_time)

    async def process_batch(self, batch):
        """Обработка батча транзакций с улучшенным трекингом блокировок"""
        if not batch:
            return 0
        
        try:
            # Формируем пары счетов
            pairs = [(tx["sender_account"], tx["receiver_account"]) for tx in batch]
            
            # Блокируем счета
            locked_pairs = await self.redis.batch_lock(pairs)
            logger.info(f"Locked {len(locked_pairs)}/{len(pairs)} account pairs")
            
            # Разделяем транзакции
            processed, queued = [], []
            locked_senders = {sender for sender, _ in locked_pairs}
            
            for tx in batch:
                if tx["sender_account"] in locked_senders:
                    processed.append(tx)
                else:
                    queued.append(tx)
            
            # Отправляем обработанные
            if processed:
                await self.send_transaction_batch(processed)
                logger.info(f"Sent {len(processed)} transactions to shards")
            
            # Сохраняем отложенные в Redis
            if queued:
                pipe = self.redis.redis.pipeline()
                for tx in queued:
                    key = f"queue:{tx['sender_account']}:{tx['receiver_account']}"
                    pipe.rpush(key, json.dumps(tx))
                await pipe.execute()
                
                # Обновляем счетчик очереди Redis
                self.redis_queue_size += len(queued)
                Metrics.redis_queue_items.set(self.redis_queue_size)
                logger.info(f"Queued {len(queued)} transactions")
            
            # Возвращаем количество обработанных транзакций
            return len(processed)
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            Metrics.kafka_errors.labels("process_error").inc()
            return 0

    async def unlock_worker(self):
        """Периодическая разблокировка счетов с логгированием"""
        while not self.shutdown_event.is_set():
            try:
                async with self.unlock_lock:
                    if not self.pending_unlocks:
                        await asyncio.sleep(0.1)
                        continue
                    
                    unlock_batch = self.pending_unlocks.copy()
                    self.pending_unlocks = []
                
                if unlock_batch:
                    unlocked = await self.redis.batch_unlock(unlock_batch)
                    logger.info(f"Unlocked {unlocked} account pairs")
                
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Unlock worker error: {e}")
                await asyncio.sleep(1)

    async def worker(self):
        """Рабочий процесс с обработкой батчей"""
        while not self.shutdown_event.is_set():
            try:
                batch = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                if batch:
                    await self.process_batch(batch)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("Worker received cancellation signal")
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                if not self.queue.empty():
                    self.queue.task_done()

    async def start_workers(self):
        """Запуск рабочих процессов"""
        self.workers = [
            asyncio.create_task(self.worker())
            for _ in range(CONFIG["MAX_PARALLEL_WORKERS"])
        ]
        return self.workers
    
    async def consume_transactions(self):
        """Потребление транзакций с батчингом"""
        batch = []
        last_flush = time.time()
        flush_interval = 0.5  # Интервал флаша в секундах
        
        while not self.shutdown_event.is_set():
            try:
                # Получаем сообщение с таймаутом
                try:
                    msg = await asyncio.wait_for(
                        self.consumer.__anext__(),
                        timeout=0.1
                    )
                except TimeoutError:
                    # Проверяем, нужно ли отправить текущий батч
                    if batch and (time.time() - last_flush) > flush_interval:
                        await self.queue.put(batch.copy())
                        Metrics.queue_size.set(self.queue.qsize())
                        batch.clear()
                        last_flush = time.time()
                    continue
                
                # Обработка сообщения
                try:
                    tx = json.loads(msg.value.decode())
                    batch.append(tx)
                    Metrics.received.inc()
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON received: {msg.value}")
                
                # Проверяем условия флаша
                if len(batch) >= CONFIG["BATCH_SIZE"] or (time.time() - last_flush) > flush_interval:
                    await self.queue.put(batch.copy())
                    Metrics.queue_size.set(self.queue.qsize())
                    batch.clear()
                    last_flush = time.time()
                    
            except CancelledError:
                logger.info("Transaction consumer cancelled")
                break
            except Exception as e:
                logger.error(f"Transaction consumer error: {e}")
                Metrics.kafka_errors.labels("consume_error").inc()
        
        # Отправляем оставшиеся транзакции при завершении
        if batch:
            await self.queue.put(batch)

    async def consume_confirmations(self):
        """Обработка подтверждений о завершённых транзакциях"""
        while not self.shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(
                    self.confirmation_consumer.__anext__(),
                    timeout=0.1
                )
                
                tx = json.loads(msg.value.decode())
                Metrics.confirmations.inc()
                
                sender = tx.get("sender_account")
                receiver = tx.get("receiver_account")
                if sender and receiver:
                    async with self.unlock_lock:
                        self.pending_unlocks.append((sender, receiver))
                    logger.debug(f"Received confirmation for: sender={sender}, receiver={receiver}")
            except TimeoutError:
                continue
            except CancelledError:
                logger.info("Confirmation consumer cancelled")
                break
            except Exception as e:
                logger.error(f"Confirmation consumer error: {e}")
                Metrics.kafka_errors.labels("confirmation_error").inc()

    async def start(self):
        """Запуск всех компонентов"""
        logger.info("Starting Orchestrator service...")
        try:      
            await self.initialize()
            start_http_server(CONFIG["METRICS_PORT"])
            logger.info(f"Metrics server started on port {CONFIG['METRICS_PORT']}")

            # Запускаем все основные задачи
            tasks = [
                # Воркеры для обработки транзакций
                *await self.start_workers(),
                # Воркер для разблокировки счетов
                asyncio.create_task(self.unlock_worker()),
                # Восстановление очередей из Redis
                asyncio.create_task(self.redis_requeue_worker()),
                # Консьюмер транзакций от API
                asyncio.create_task(self.consume_transactions()),
                # Консьюмер подтверждений от воркеров
                asyncio.create_task(self.consume_confirmations())
            ]

            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Service error: {e}")
        finally:
            await self.shutdown()
        
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("Starting shutdown sequence...")
        self.shutdown_event.set()
        
        try:
            # 1. Останавливаем генерацию новых задач
            if self.consumer:
                await self.consumer.stop()
            if self.confirmation_consumer:
                await self.confirmation_consumer.stop()
            
            # 2. Ожидаем обработки текущей очереди
            logger.info(f"Waiting for queue to drain ({self.queue.qsize()} items)...")
            await asyncio.wait_for(self.queue.join(), timeout=5)
            
            # 3. Отменяем все задачи
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            
            # 4. Ожидаем завершения задач
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 5. Выполняем оставшиеся разблокировки
            if self.pending_unlocks:
                logger.info(f"Unlocking remaining {len(self.pending_unlocks)} pairs")
                await self.redis.batch_unlock(self.pending_unlocks)
            
            # 6. Закрываем соединения
            if self.redis:
                await self.redis.close()
            if self.producer:
                await self.producer.stop()
            
            logger.info("All resources released")
        
        except asyncio.TimeoutError:
            logger.error("Graceful shutdown timed out!")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    orchestrator = Orchestrator()

    try:
        asyncio.run(orchestrator.start())
    except KeyboardInterrupt:
        logger.info("Orchestrator shutdown by user")
    except Exception as e:
        logger.exception(f"Fatal error in Orchestrator: {e}")
    finally:
        logger.info("Service stopped")