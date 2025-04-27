import asyncio
import json
from collections import defaultdict, deque

from common.kafka import get_kafka_consumer, get_kafka_producer

import pyfiglet
from prometheus_client import start_http_server, Counter

ascii_banner = pyfiglet.figlet_format("TTTS ORCHESTRATOR 0.0.3", font="slant")
print(ascii_banner)

# ────────────────────────────
# METRICS
orch_received = Counter("orchestrator_received_total", "Total transactions received from API")
orch_fraud_failed = Counter("orchestrator_fraud_failed_total", "Transactions failed on fraud check")
orch_ext_failed = Counter("orchestrator_external_failed_total", "Transactions failed on external coordination")
orch_sent = Counter("orchestrator_sent_total", "Total transactions sent to worker")
orch_queued = Counter("orchestrator_queued_total", "Transactions queued due to active lock")

# ────────────────────────────
TOPIC_API_TO_ORCH = "transaction-events"
TOPIC_ORCH_TO_WORKER = "validated-transactions"

account_queues = defaultdict(deque)
active_accounts = set()

async def fraud_check(tx: dict) -> bool:
    await asyncio.sleep(0.01)
    print(f"✅ Проверка ФРОД DONE! {tx}")
    return True

async def coordinate_with_external_service(tx: dict) -> bool:
    await asyncio.sleep(0.01)
    print(f"✅ TTTS Approved {tx} DONE!")
    return True

async def handle_transaction(tx: dict, producer):
    sender_account = tx["sender_account"]

    try:
        print(f"🔒 Бронирование суммы для sender_account {sender_account}")

        if not await fraud_check(tx):
            print(f"❌ Фрод отклонён: {tx}")
            orch_fraud_failed.inc()
            return

        if not await coordinate_with_external_service(tx):
            print(f"❌ Согласование отклонено: {tx}")
            orch_ext_failed.inc()
            return

        await producer.send_and_wait(TOPIC_ORCH_TO_WORKER, json.dumps(tx).encode("utf-8"))
        print(f"✅ Завершено и отправлено в worker (topic:validated-transactions): {tx}")
        print(f"📦 Orchestrator → worker: {tx}")
        orch_sent.inc()

    except Exception as e:
        print(f"❌ Ошибка при обработке транзакции: {e}")
    finally:
        queue = account_queues[sender_account]
        if queue:
            next_tx = queue.popleft()
            asyncio.create_task(handle_transaction(next_tx, producer))
        else:
            active_accounts.remove(sender_account)

async def orchestrate():
    consumer = await get_kafka_consumer(topic=TOPIC_API_TO_ORCH, group_id="orhestra-group")
    producer = await get_kafka_producer()

    try:
        async for msg in consumer:
            tx = json.loads(msg.value)
            sender_account = tx["sender_account"]

            orch_received.inc()

            if sender_account in active_accounts:
                account_queues[sender_account].append(tx)
                print(f"⏳ Поставлено в очередь для sender_account {sender_account}: {tx}")
                orch_queued.inc()
            else:
                active_accounts.add(sender_account)
                asyncio.create_task(handle_transaction(tx, producer))

    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    start_http_server(8002)  # Метрики будут по адресу :8002/metrics
    asyncio.run(orchestrate())
