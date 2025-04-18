import asyncio
import json
from common.kafka import get_kafka_consumer
from common.db import db
from datetime import datetime
import pyfiglet
from prometheus_client import start_http_server, Counter

# ────────────────────────────
# ASCII-баннер
ascii_banner = pyfiglet.figlet_format("TTTS WORKER 0.0.3", font="slant")
print(ascii_banner)

# ────────────────────────────
# METRICS
tx_processed = Counter("transactions_processed_total", "Total number of successfully processed transactions")
tx_failed = Counter("transactions_failed_total", "Total number of failed transactions")
tx_insufficient = Counter("transactions_insufficient_funds_total", "Transactions failed due to insufficient funds")
tx_not_found = Counter("transactions_account_not_found_total", "Transactions failed due to missing accounts")

# ────────────────────────────
async def process_transaction(tx: dict):
    print(f"✅ Обработка транзакции: {tx}")
    sender_account = str(tx["sender_account"])
    receiver_account = str(tx["receiver_account"])
    amount = tx["amount"]

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # Блокируем строки
                sender = await conn.fetchrow("SELECT balance FROM accounts WHERE account_number = $1 FOR UPDATE", sender_account)
                receiver = await conn.fetchrow("SELECT balance FROM accounts WHERE account_number = $1 FOR UPDATE", receiver_account)

                if sender is None or receiver is None:
                    print("❌ Один из пользователей не найден")
                    tx_not_found.inc()
                    return

                if sender["balance"] < amount:
                    print(f"❌ Недостаточно средств: у пользователя {sender_account} {sender['balance']} < {amount}")
                    tx_insufficient.inc()
                    return

                # Обновление балансов
                await conn.execute("UPDATE accounts SET balance = balance - $1 WHERE account_number = $2", amount, sender_account)
                await conn.execute("UPDATE accounts SET balance = balance + $1 WHERE account_number = $2", amount, receiver_account)

                # Запись транзакции
                timestamp = datetime.utcnow()
                tx_id = await conn.fetchrow("""
                    INSERT INTO transactions (sender_account, receiver_account, amount, timestamp)
                    VALUES ($1, $2, $3, $4) RETURNING id
                """, sender_account, receiver_account, amount, timestamp)

                print(f"💾 Транзакция записана в БД: ID={tx_id['id']}, FROM={sender_account}, TO={receiver_account}, AMOUNT={amount}, TIME={timestamp}")
                tx_processed.inc()
    except Exception as e:
        print(f"❌ Ошибка при обработке транзакции: {e}")
        tx_failed.inc()

# ────────────────────────────
async def consume():
    await db.init()
    print("🚀 Подключение к базе инициализировано")
    consumer = await get_kafka_consumer("validated-transactions", group_id="worker-group")
    try:
        async for msg in consumer:
            tx = json.loads(msg.value)
            await process_transaction(tx)
    finally:
        await consumer.stop()
        await db.close()

# ────────────────────────────
if __name__ == "__main__":
    start_http_server(8001)  # 📊 Метрики будут доступны на :8001/metrics
    asyncio.run(consume())
