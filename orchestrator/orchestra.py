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
from aiokafka.structs import RecordMetadata
from collections import defaultdict

# ==================== КОНФИГУРАЦИЯ ====================
CONFIG = {
    "TOPIC_API_TO_ORCH": "transaction-events",
    "TOPIC_CONFIRMATION": "transaction-confirmation",
    "SHARD_COUNT": 16,
    "REDIS_URL": "redis://redis:6379",
    "MAX_PARALLEL_WORKERS": 500,
    "BATCH_SIZE": 100,
    "LOCK_TIMEOUT": 30,
    "METRICS_PORT": 8002,
    "KAFKA_PRODUCER_CONFIG": {
        "linger_ms": 50,
        "compression_type": "snappy",
        "max_batch_size": 16384,  # 16KB
        "request_timeout_ms": 30000
    },
    "KAFKA_CONSUMER_CONFIG": {
        "max_poll_records": 200,
        "fetch_max_bytes": 1048576  # 1MB
    }
}

# ==================== ИНИЦИАЛИЗАЦИЯ ЛОГГЕРА ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ==================== МЕТРИКИ ====================
class Metrics:
    received = Counter("orchestrator_received_total", "Total transactions received")
    sent = Counter("orchestrator_sent_total", "Total transactions sent")
    queued = Counter("orchestrator_queued_total", "Transactions queued")
    confirmations = Counter("orchestrator_confirmation_received_total", "Total confirmations")
    active_locks = Gauge("orchestrator_active_locks", "Current active locks")
    queue_size = Gauge("orchestrator_queue_size", "Current queue size")
    processing_time = Histogram("orchestrator_processing_time", "Processing time in seconds", 
                              buckets=[0.01, 0.05, 0.1, 0.5, 1, 5])

# ==================== REDIS КЛИЕНТ ====================
class RedisClient:
    _instance = None
    
    def __init__(self):
        self.redis = None
        self.lock_script = None
        self.unlock_script = None
    
    @classmethod
    async def get(cls):
        if cls._instance is None:
            cls._instance = RedisClient()
            await cls._instance.initialize()
        return cls._instance
    
    async def initialize(self):
        self.redis = await aioredis.from_url(
            CONFIG["REDIS_URL"],
            max_connections=CONFIG["MAX_PARALLEL_WORKERS"] * 2
        )
        self.lock_script = self.redis.register_script("""
            local results = {}
            for i = 1, #KEYS/2 do
                local sender_key = KEYS[2*i-1]
                local receiver_key = KEYS[2*i]
                local timeout = ARGV[1]
                
                if redis.call('SET', sender_key, 'locked', 'NX', 'EX', timeout) and 
                   redis.call('SET', receiver_key, 'locked', 'NX', 'EX', timeout) then
                    results[#results+1] = 1
                else
                    results[#results+1] = 0
                end
            end
            return results
        """)
        
        self.unlock_script = self.redis.register_script("""
            redis.call('DEL', unpack(KEYS))
            return #KEYS/2
        """)
    
    async def batch_lock(self, pairs):
        """Атомарная блокировка нескольких пар аккаунтов"""
        keys = []
        for sender, receiver in pairs:
            keys.extend([f"lock:{sender}", f"lock:{receiver}"])
        
        try:
            # Результат будет списком [1, 0, 1, ...] где 1 - успешная блокировка
            results = await self.lock_script(keys=keys, args=[CONFIG["LOCK_TIMEOUT"]])
            
            locked_pairs = []
            for i, (sender, receiver) in enumerate(pairs):
                if results[i]:
                    locked_pairs.append((sender, receiver))
                    Metrics.active_locks.inc(2)
            
            return locked_pairs
        except Exception as e:
            logger.error(f"Redis lock error: {e}")
            return []
    
    async def batch_unlock(self, pairs):
        """Атомарная разблокировка нескольких пар аккаунтов"""
        keys = []
        for sender, receiver in pairs:
            keys.extend([f"lock:{sender}", f"lock:{receiver}"])
        
        try:
            await self.unlock_script(keys=keys)
            Metrics.active_locks.dec(len(keys))
        except Exception as e:
            logger.error(f"Redis unlock error: {e}")

    async def warmup_redis_scripts(self):
        """Прогрев Redis скриптов перед использованием"""
        dummy_pairs = [("test1", "test2"), ("test3", "test4")]
        await self.batch_lock(dummy_pairs)
        await self.batch_unlock(dummy_pairs)

# ==================== ОСНОВНАЯ ЛОГИКА ====================
class Orchestrator:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.confirmation_consumer = None
        self.redis = None
        self.pending_transactions = defaultdict(list)
        self.queue = asyncio.Queue(maxsize=CONFIG["MAX_PARALLEL_WORKERS"] * 10)
        self.shard_partitions = {}  # Кэш партиций для шардов
        self.lock = asyncio.Lock()  # Для атомарных операций
    
    async def initialize(self):
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
        
        # Предварительная загрузка Lua-скриптов
        await self.warmup_redis_scripts()

    async def warmup_redis_scripts(self):
        """Предварительная загрузка скриптов Redis"""
        dummy_pairs = [("test1", "test2"), ("test3", "test4")]
        await self.redis.batch_lock(dummy_pairs)
        await self.redis.batch_unlock(dummy_pairs)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.1, max=1),
        retry=retry_if_exception_type(KafkaTimeoutError)
    )

    async def send_transaction_batch(self, transactions):
        """Корректная отправка батча транзакций"""
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
            for topic, messages in shard_msgs.items():
                # Вариант 1: Отправка через send_many (лучший способ)
                if hasattr(self.producer, 'send_many'):
                    await self.producer.send_many(topic, messages)
                # Вариант 2: Альтернативная реализация
                else:
                    for key, value in messages:
                        await self.producer.send(topic, value=value, key=key)

            Metrics.sent.inc(len(transactions))
        except Exception as e:
            logger.error(f"Failed to send batch: {e}")
            raise
        finally:
            Metrics.processing_time.observe(time.time() - start_time)

    async def process_batch(self, batch):
        """Оптимизированная обработка батча"""
        try:
            # Блокируем аккаунты батчем
            pairs = [(tx["sender_account"], tx["receiver_account"]) for tx in batch]
            locked_pairs = await self.redis.batch_lock(pairs)
            
            # Разделяем на обработанные и отложенные
            locked_senders = {sender for sender, _ in locked_pairs}
            processed, queued = [], []
            
            for tx in batch:
                if tx["sender_account"] in locked_senders:
                    processed.append(tx)
                else:
                    queued.append(tx)
            
            # Пакетная отправка и сохранение
            if processed:
                await self.send_transaction_batch(processed)
            
            if queued:
                pipe = self.redis.redis.pipeline()
                for tx in queued:
                    pipe.rpush(
                        f"queue:{tx['sender_account']}:{tx['receiver_account']}",
                        json.dumps(tx)
                    )
                await pipe.execute()
                Metrics.queued.inc(len(queued))
            
            return processed
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return []
    
    async def worker(self):
        while True:
            batch = await self.queue.get()
            try:
                await self.process_batch(batch)
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                self.queue.task_done()
    
    async def start_workers(self):
        workers = [
            asyncio.create_task(self.worker())
            for _ in range(CONFIG["MAX_PARALLEL_WORKERS"])
        ]
        return workers
    
    async def consume_transactions(self):
        async for msg in self.consumer:
            try:
                tx = json.loads(msg.value)
                Metrics.received.inc()
                await self.queue.put(tx)
            except Exception as e:
                logger.error(f"Consume error: {e}")
    
    async def consume_transactions(self):
        """Оптимизированное потребление с батчингом"""
        batch = []
        async for msg in self.consumer:
            try:
                tx = json.loads(msg.value)
                Metrics.received.inc()
                batch.append(tx)
                
                if len(batch) >= CONFIG["BATCH_SIZE"]:
                    await self.queue.put(batch.copy())
                    batch.clear()
            except Exception as e:
                logger.error(f"Consume error: {e}")
        
        if batch:  # Обработка оставшихся сообщений
            await self.queue.put(batch)
    
    async def update_metrics(self):
        while True:
            try:
                keys = await self.redis.redis.keys("queue:*")
                total = 0
                if keys:
                    lengths = await self.redis.redis.mget(*keys)
                    total = sum(int(length) for length in lengths if length)
                Metrics.queue_size.set(total)
            except Exception as e:
                logger.error(f"Metrics error: {e}")
            await asyncio.sleep(5)
    
    async def run(self):
        await self.initialize()
        workers = await self.start_workers()
        metrics_task = asyncio.create_task(self.update_metrics())
        
        try:
            await asyncio.gather(
                self.consume_transactions(),
                self.consume_confirmations()
            )
        finally:
            for worker in workers:
                worker.cancel()
            metrics_task.cancel()
            await asyncio.gather(
                self.consumer.stop(),
                self.confirmation_consumer.stop(),
                self.producer.stop(),
                return_exceptions=True
            )
            
    async def consume_confirmations(self):
        """Обработка подтверждений от воркеров"""
        try:
            async for msg in self.confirmation_consumer:
                try:
                    confirmation = json.loads(msg.value)
                    Metrics.confirmations.inc()
                    
                    sender = confirmation["sender_account"]
                    receiver = confirmation["receiver_account"]
                    
                    # Получаем следующую транзакцию из очереди
                    next_tx = await self.redis.redis.lpop(
                        f"queue:{sender}:{receiver}"
                    )
                    
                    if next_tx:
                        tx = json.loads(next_tx)
                        shard_id = hash(sender) % CONFIG["SHARD_COUNT"]
                        await self.send_transaction(tx, shard_id)
                    else:
                        # Если очередь пуста, разблокируем аккаунты
                        await self.redis.batch_unlock([(sender, receiver)])
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid confirmation message: {msg.value}, error: {e}")
                except KeyError as e:
                    logger.error(f"Missing field in confirmation: {e}")
                except Exception as e:
                    logger.error(f"Error processing confirmation: {e}")
                    
        except Exception as e:
            logger.error(f"Confirmation consumer failed: {e}")
            raise

    async def run(self):
        """Основной цикл работы оркестратора"""
        await self.initialize()
        workers = await self.start_workers()
        metrics_task = asyncio.create_task(self.update_metrics())
        
        try:
            # Запускаем оба потребителя параллельно
            await asyncio.gather(
                self.consume_transactions(),
                self.consume_confirmations()  # Теперь метод существует
            )
        except asyncio.CancelledError:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Orchestrator failed: {e}")
        finally:
            # Корректное завершение
            for worker in workers:
                worker.cancel()
            metrics_task.cancel()
            
            await asyncio.gather(
                self.consumer.stop(),
                self.confirmation_consumer.stop(),
                self.producer.stop(),
                return_exceptions=True
            )
            await self.redis.redis.close()
            logger.info("Orchestrator shutdown complete")

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    start_http_server(CONFIG["METRICS_PORT"])
    
    ascii_banner = pyfiglet.figlet_format("OPTIMIZED ORCHESTRATOR", font="slant")
    print(ascii_banner)
    
    orchestrator = Orchestrator()
    asyncio.run(orchestrator.run())