# orchestrator/orchestra.py
import asyncio
import json
from collections import defaultdict, deque

from common.kafka import get_kafka_consumer, get_kafka_producer

import pyfiglet

ascii_banner = pyfiglet.figlet_format("TTTS ORCHESTRATOR 0.0.3", font="slant")
print(ascii_banner)

# Константы для топиков
TOPIC_API_TO_ORCH = "transaction-events"
TOPIC_ORCH_TO_WORKER = "validated-transactions"

# Очереди по аккаунтам и активные аккаунты
account_queues = defaultdict(deque)
active_accounts = set()

async def fraud_check(tx: dict) -> bool:
    # Заглушка: всегда "одобряет"
    await asyncio.sleep(0.01)
    print(f"✅ Проверка ФРОД DONE! {tx}")
    return True

async def coordinate_with_external_service(tx: dict) -> bool:
    # Заглушка: всегда "одобряет"
    await asyncio.sleep(0.01)
    print(f"✅ TTTS Approved {tx} DONE!")
    return True

async def handle_transaction(tx: dict, producer):
    sender_account = tx["sender_account"]

    try:
        # 1. Забронировать средства (в будущем — запрос в БД)
        print(f"🔒 Бронирование суммы для sender_account {sender_account}")

        # 2. Проверка на фрод
        if not await fraud_check(tx):
            print(f"❌ Фрод отклонён: {tx}")
            return

        # 3. Внешнее согласование
        if not await coordinate_with_external_service(tx):
            print(f"❌ Согласование отклонено: {tx}")
            return

        # 4. Отправка в validated-transactions
        await producer.send_and_wait(TOPIC_ORCH_TO_WORKER, json.dumps(tx).encode("utf-8"))
        print(f"✅ Завершено и отправлено в worker (topic:transaction-processed): {tx}")
        print(f"📦 Orchestrator → worker: {tx}")

    finally:
        # Завершено: убираем из активных и запускаем следующую в очереди
        queue = account_queues[sender_account]
        if queue:
            next_tx = queue.popleft()
            asyncio.create_task(handle_transaction(next_tx, producer))
        else:
            active_accounts.remove(sender_account)

async def orchestrate():
    consumer = await get_kafka_consumer(topic=TOPIC_API_TO_ORCH)
    producer = await get_kafka_producer()

    try:
        async for msg in consumer:
            tx = json.loads(msg.value)
            sender_account = tx["sender_account"]

            if sender_account in active_accounts:
                account_queues[sender_account].append(tx)
                print(f"⏳ Поставлено в очередь для sender_account {sender_account}: {tx}")
            else:
                active_accounts.add(sender_account)
                asyncio.create_task(handle_transaction(tx, producer))

    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(orchestrate())
