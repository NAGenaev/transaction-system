import os
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_URI", "kafka:9092")

async def get_kafka_producer(
    acks="all",
    linger_ms=50,
    compression_type="snappy",
    **kwargs
):
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks=acks,
            linger_ms=linger_ms,
            compression_type=compression_type,
            enable_idempotence=True,  # Включаем идемпотентность
            transactional_id="api-producer" if os.getenv("ENABLE_TRANSACTIONS") else None,
            **kwargs
        )
        await producer.start()
        logger.info(f"Kafka producer запущен (compression: {compression_type}, idempotence: True)")
        return producer
    except Exception as e:
        logger.error(f"Ошибка инициализации producer: {e}")
        raise

async def get_kafka_consumer(topic: str, group_id: str = None, **kwargs):
    """Создание consumer с безопасными настройками по умолчанию"""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        **kwargs
    )
    try:
        await consumer.start()
        logger.info(f"Kafka consumer запущен для топика '{topic}'")
        return consumer
    except Exception as e:
        logger.error(f"Ошибка инициализации consumer: {e}")
        raise