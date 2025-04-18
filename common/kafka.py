# common/kafka.py
import os
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from common.constants import TOPIC_API_TO_ORCH, TOPIC_ORCH_TO_WORKER

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_URI", "kafka:9092")

async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Kafka producer started")
    return producer

async def get_kafka_consumer(topic: str, group_id: str = "default-group"):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
    )
    await consumer.start()
    logger.info(f"Kafka consumer started for topic '{topic}'")
    return consumer
