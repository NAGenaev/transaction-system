import os
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_URI", "kafka:9092")

async def get_kafka_producer():
    """
    Создание и запуск Kafka producer.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks=1,                  # Быстрее, чем "all"
        linger_ms=10,            # Буферизация ~10 мс перед отправкой
        compression_type="snappy",  # Быстрее, чем gzip, без лишнего overhead
    )
    try:
        await producer.start()
        logger.info("Kafka producer успешно запущен")
    except Exception as e:
        logger.error(f"Не удалось запустить Kafka producer: {e}")
        raise
    return producer

async def get_kafka_consumer(topic: str, group_id: str = "default-group"):
    """
    Создание и запуск Kafka consumer.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",  # Начать с самого раннего сообщения
        enable_auto_commit=True,       # Автоматическое подтверждение сообщений
        group_id=group_id,
        heartbeat_interval_ms=3000,    # Интервал heartbeat
        max_poll_interval_ms=60000,    # Время между запросами на чтение
        session_timeout_ms=30000,      # Время на сессию
        request_timeout_ms=40000,      # Время на запросы
    )
    try:
        await consumer.start()
        logger.info(f"Kafka consumer успешно запущен для топика '{topic}'")
    except Exception as e:
        logger.error(f"Не удалось запустить Kafka consumer для топика {topic}: {e}")
        raise
    return consumer

# Пример функции для отправки сообщений в Kafka
async def send_message(producer, topic, message):
    """
    Отправка сообщения в Kafka топик.
    """
    try:
        await producer.send_and_wait(topic, message)
        logger.info(f"Сообщение отправлено в Kafka топик '{topic}'")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в топик '{topic}': {e}")
        raise

# Пример функции для обработки сообщений из Kafka
async def consume_messages(consumer):
    """
    Чтение сообщений из Kafka consumer.
    """
    try:
        async for msg in consumer:
            logger.info(f"Получено сообщение из Kafka: {msg.value.decode('utf-8')}")
            # Обработка сообщения (например, передача в другую очередь или сервис)
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений из Kafka: {e}")
        raise