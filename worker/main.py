import asyncio
from common.kafka import get_consumer

async def consume():
    consumer = await get_consumer("transactions", group_id="worker")
    async for msg in consumer:
        print(f"Получена транзакция: {msg.value}")  # Здесь будет обработка

if __name__ == "__main__":
    asyncio.run(consume())