//тест на 2 млн транзакций из пула 1 млн крест на крест
import http from 'k6/http';
import { check, sleep } from 'k6';

const startNumber = 40817810111322211n;
const totalTransactions = 2_000_000; // Увеличили до 2 млн
const vus = 200;

export const options = {
  scenarios: {
    transactions: {
      executor: 'per-vu-iterations',
      vus: vus,
      iterations: totalTransactions / vus, // Теперь 10000 итераций на VU
      maxDuration: '120m', // Увеличили максимальное время выполнения
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
    'http_req_failed': ['rate<0.01'],
  },
  discardResponseBodies: true,
};

export default function () {
  const vuIterations = totalTransactions / vus;
  const globalIndex = BigInt((__VU - 1) * vuIterations + __ITER);

  // Используем модуль от исходного общего количества транзакций (1 млн),
  // чтобы номера счетов повторялись в том же диапазоне
  const modIndex = globalIndex % BigInt(1_000_000);
  
  const sender = (startNumber + modIndex).toString();
  const receiver = (startNumber + BigInt(1_000_000 - 1) - modIndex).toString();

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

  sleep(0.001);
}