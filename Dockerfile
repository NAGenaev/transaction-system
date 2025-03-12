# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем зависимости
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY . /app/

# Устанавливаем переменные окружения
ENV POSTGRES_DSN=postgresql://user:password@postgres/transactions
ENV MONGO_URI=mongodb://mongo:27017
ENV RABBITMQ_URI=amqp://user:password@rabbitmq:5672

# Запускаем приложение
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
