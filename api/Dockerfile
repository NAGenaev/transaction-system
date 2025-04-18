# api/Dockerfile
FROM python:3.11-slim
# Устанавливаем зависимости
WORKDIR /app
COPY ./api/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
# Копируем код приложения
COPY ./api /app/api
COPY ./common /app/common
# Устанавливаем переменные окружения
ENV POSTGRES_DSN=postgresql://user:password@postgres/transactions
ENV KAFKA_URI=kafka:9092
ENV PYTHONUNBUFFERED=1
# Запускаем приложение
# CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD sleep 10 && uvicorn api.main:app --host 0.0.0.0 --port 8000