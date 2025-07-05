//нагрузочный тест, который будет повторять переводы между случайными счетами из фиксированного пула: 1000
import http from 'k6/http';
import { check, sleep } from 'k6';

// Фиксированный пул счетов
const accountPool = Array.from({length: 1000}, (_, i) => 
    (40817810111322211n + BigInt(i)).toString()
);

const totalTransactions = 2_000_000;
const vus = 200;

export const options = {
  scenarios: {
    repeated_transfers: {
      executor: 'per-vu-iterations',
      vus: vus,
      iterations: totalTransactions / vus,
      maxDuration: '120m',
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
    'http_req_failed': ['rate<0.01'],
  },
  discardResponseBodies: true,
};

export default function () {
  // Выбираем случайного отправителя и получателя из пула
  const senderIndex = Math.floor(Math.random() * accountPool.length);
  let receiverIndex;
  
  // Гарантируем, что получатель отличается от отправителя
  do {
    receiverIndex = Math.floor(Math.random() * accountPool.length);
  } while (receiverIndex === senderIndex);

  const sender = accountPool[senderIndex];
  const receiver = accountPool[receiverIndex];
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