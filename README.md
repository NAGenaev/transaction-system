`transaction-system PPPS`
---

# 🚀 Сервис перевода денег между пользователями  

## 📌 Описание  
Этот сервис отвечает за переводы денег между пользователями с использованием **FastAPI**, **PostgreSQL** и **RabbitMQ**.  
Основная цель — **гарантировать надежные и быстрые транзакции**, поддерживая асинхронную обработку и масштабируемость.  

## ⚙ Логика работы  

1️⃣ **Пользователь отправляет запрос на перевод** (через HTTP API).  
2️⃣ **Проверяется баланс отправителя** в базе данных.  
3️⃣ **Запрос ставится в очередь** (RabbitMQ) для асинхронной обработки.  
4️⃣ **Обработчик забирает сообщение** из очереди и выполняет перевод.  
5️⃣ **Результат записывается в базу данных** (PostgreSQL).  
6️⃣ **Система уведомляет клиента** о статусе транзакции.  

## 🛠 Используемый стек  

| Технология              | Зачем используется? |
|-------------------------|--------------------|
| **FastAPI (Python)**    | Обрабатывает HTTP-запросы, обеспечивает высокую скорость API. |
| **PostgreSQL**          | Хранит пользователей и их балансы, гарантирует целостность данных (ACID). |
| **RabbitMQ**            | Обеспечивает асинхронную обработку транзакций, разгружает API. |
| **Docker + Docker Compose** | Упрощает развертывание и управление сервисами. |
| **Prometheus + Grafana (в перспективе)** | Мониторинг производительности сервиса. |

## 🤔 Почему именно такой стек?  

✔ **FastAPI** – легковесный и быстрый фреймворк, идеально подходит для высоконагруженных API.  
✔ **PostgreSQL** – надежная СУБД с поддержкой транзакций, идеально подходит для финансовых операций.  
✔ **RabbitMQ** – позволяет разгрузить API и избежать блокировок базы данных.  
✔ **Docker** – упрощает управление зависимостями и развертывание сервиса.  
✔ **Prometheus/Grafana** – необходимы для мониторинга и контроля состояния сервиса.  

⚡ Этот стек выбран для **быстрой, надежной и отказоустойчивой работы** приложения.  

---
