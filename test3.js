import http from 'k6/http';
import { check, sleep } from 'k6';

const startNumber = 40817810111322211n;
const totalTransactions = 1000;  // всего 1000 запросов
const vus = 30;                   // 30 виртуальных пользователей

// Для ровного целочисленного деления используем Math.floor
const transactionsPerVU = Math.floor(totalTransactions / vus);

export const options = {
  vus: vus,
  iterations: totalTransactions,
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
    'http_req_failed': ['rate<0.01'],
  },
};

export default function () {
  const localIndex = BigInt(__ITER % transactionsPerVU);  // индекс итерации для VU
  const vuOffset = BigInt(__VU - 1) * BigInt(transactionsPerVU);

  const globalIndex = vuOffset + localIndex;

  // Если globalIndex выходит за totalTransactions - игнорируем (например, последние итерации)
  if (globalIndex >= BigInt(totalTransactions)) {
    sleep(1);  // просто делаем паузу — запрос не посылаем
    return;
  }

  const sender = (startNumber + globalIndex).toString();
  const receiver = (startNumber + BigInt(totalTransactions - 1) - globalIndex).toString();

  const amount = 1;

  const payload = JSON.stringify({
    sender_account: sender,
    receiver_account: receiver,
    amount: amount,
  });

  const res = http.post('http://localhost:8000/transactions/', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  check(res, {
    '📦 статус 200': (r) => r.status === 200,
  });

  if (res.status !== 200) {
    console.error(`❌ Ошибка при транзакции: ${res.status} → ${payload}`);
  }

  sleep(0.01);
}
