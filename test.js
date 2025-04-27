import http from 'k6/http';
import { check, sleep } from 'k6';

const accounts = Array.from({ length: 1000000 }, (_, i) => `40817810111322${111 + i}`);

export const options = {
  vus: 10,           // количество виртуальных пользователей
  duration: '2m',   // длительность теста
  thresholds: {
    'http_req_duration': ['p(95)<500'],  // 95% запросов должны быть выполнены за 500 мс
  },
};

export default function () {
  // Генерация случайных индексов для аккаунтов
  let senderIndex = Math.floor(Math.random() * accounts.length);
  let receiverIndex = senderIndex;
  while (receiverIndex === senderIndex) {
    receiverIndex = Math.floor(Math.random() * accounts.length);
  }

  // Генерация случайной суммы для перевода
  const amount = Math.floor(Math.random() * 100) + 1;

  const payload = JSON.stringify({
    sender_account: accounts[senderIndex],
    receiver_account: accounts[receiverIndex],
    amount: amount,
  });

  const res = http.post('http://localhost:8000/transactions/', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',  // увеличенный тайм-аут на запрос
  });

  // Проверка на успешный ответ и логирование в случае ошибки
  check(res, {
    '📦 статус 200': (r) => r.status === 200,
  });

  if (res.status !== 200) {
    console.error(`Ошибка при транзакции: ${res.status} для ${payload}`);
  }

  // Логирование аккаунтов для отслеживания
  console.log(`Транзакция от ${accounts[senderIndex]} к ${accounts[receiverIndex]} на сумму ${amount}`);

  // Уменьшаем время ожидания между запросами для увеличения RPS
  sleep(0.01);
}
