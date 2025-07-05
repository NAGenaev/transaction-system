//тест на максимальный рпс
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Диапазон существующих счетов
const FIRST_ACCOUNT = 40817810111322111;
const LAST_ACCOUNT = 40817810112322210;
const TOTAL_ACCOUNTS = LAST_ACCOUNT - FIRST_ACCOUNT + 1;

export const options = {
  scenarios: {
    stress: {
      executor: 'constant-arrival-rate',
      rate: 5000, // Целевой RPS
      timeUnit: '1s',
      duration: '1m',
      preAllocatedVUs: 100,
      maxVUs: 1000,
    }
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // <1% ошибок
    http_req_duration: ['p(95)<1000'],
  },
  discardResponseBodies: true,
};

function getRandomAccount() {
  const randomOffset = Math.floor(Math.random() * TOTAL_ACCOUNTS);
  return (FIRST_ACCOUNT + randomOffset).toString();
}

export default function () {
  // Генерируем уникальные счета в пределах диапазона
  let sender, receiver;
  do {
    sender = getRandomAccount();
    receiver = getRandomAccount();
  } while (sender === receiver); // Гарантируем разные счета

  const payload = JSON.stringify({
    sender_account: sender,
    receiver_account: receiver,
    amount: 1,
    transaction_id: uuidv4() // Глобально уникальный ID
  });

  const res = http.post('http://localhost:8002/transactions/', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  check(res, {
    'status 200': (r) => r.status === 200,
    'response time': (r) => r.timings.duration < 2000,
  });

  if (res.status !== 200) {
    console.error(`❌ ${res.status} | ${res.body} | ${payload}`);
  }
}