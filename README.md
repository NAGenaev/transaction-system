# 💸 Transaction Service

**Transaction Service** — это имитация банковской системы переводов с использованием микросервисной архитектуры. Сервис реализован на FastAPI и поддерживает асинхронную обработку транзакций, очередь сообщений через RabbitMQ и сбор метрик через Prometheus + Grafana.

---

## 🚀 Ветки и версии

| Ветка | Версия | Описание |
|-------|--------|----------|
| `master` | 0.0.2 | **Актуальная стабильная версия**. Асинхронная реализация с `aio_pika`, `asyncpg`, метриками и мониторингом. |
| `develop` | 0.0.3-dev | Ветка для тестирования, разработки и экспериментов. |

---

## ⚙️ Архитектура (v0.0.2)

- **FastAPI** — REST API для обработки транзакций
- **PostgreSQL** — Хранение пользователей и транзакций
- **RabbitMQ** — Очередь сообщений о транзакциях
- **Prometheus + Grafana** — Сбор и визуализация метрик
- **aio_pika** — Асинхронная интеграция с RabbitMQ
- **asyncpg** — Асинхронное подключение к PostgreSQL

---

## 📦 Запуск (v0.0.2)

```bash
# Клонируем репозиторий
git clone https://github.com/your-user/transaction-service.git
cd transaction-service

# Запускаем docker-compose
docker-compose up --build
```

> После запуска:
- API будет доступно по адресу: http://localhost:8000/docs
- Интерфейс Grafana: http://localhost:3000 (логин/пароль: admin/admin)
- Интерфейс RabbitMQ: http://localhost:15672 (логин/пароль: admin/strongpassword)
- Prometheus: http://localhost:9090

---

## 📥 Пример запроса

```json
POST /transactions
{
  "sender": "user1",
  "recipient": "user2",
  "amount": 100.0
}
```

---

## 📁 Структура проекта

```
.
├── main.py                # FastAPI-приложение
├── Dockerfile             # Сборка образа сервиса
├── docker-compose.yml     # Инфраструктура
├── requirements.txt       # Зависимости
├── prometheus.yml         # Конфигурация Prometheus
├── rabbitmq.conf          # Конфигурация RabbitMQ
└── postgres-data/         # Данные PostgreSQL
```

---

## 📈 Метрики

Используется [`prometheus_fastapi_instrumentator`](https://github.com/trallnag/prometheus-fastapi-instrumentator) для автоматической генерации метрик FastAPI.

Prometheus настроен на сбор метрик с:
- `transaction-service:8000`
- `rabbitmq_exporter:9419`

---

## 🧪 Версия 0.0.1 (архивная)

Находится в [старой ревизии ветки](https://github.com/NAGenaev/transaction-system/tree/8ccbca08279526498647962b6b00054309b9f765).
PPPS (TS) v0.0.1-alpha (https://github.com/NAGenaev/transaction-system/releases/tag/0.0.1)

Особенности:
- Синхронная реализация с `pika`
- Без мониторинга
- Только базовый FastAPI + PostgreSQL + RabbitMQ

---