import http from 'k6/http';
import { check, sleep } from 'k6';

const startNumber = 40817810111322211n;
const totalTransactions = 1_000_000;
const vus = 200;

export const options = {
  scenarios: {
    transactions: {
      executor: 'per-vu-iterations', // Каждый VU выполняет свои итерации
      vus: vus,
      iterations: totalTransactions / vus, // 2500 итераций на VU
      maxDuration: '1m', // Максимальное время выполнения
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
    'http_req_failed': ['rate<0.01'],
  },
  discardResponseBodies: true, // Уменьшаем использование памяти
};

export default function () {
  const vuIterations = totalTransactions / vus;
  const globalIndex = BigInt((__VU - 1) * vuIterations + __ITER);

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

  sleep(0.005);
}