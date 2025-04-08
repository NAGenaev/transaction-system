from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

async def get_producer(bootstrap_servers="kafka:9092"):
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    return producer

async def get_consumer(topic, group_id, bootstrap_servers="kafka:9092"):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    return consumer
