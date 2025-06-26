FROM python:3.11-slim

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y libsnappy-dev gcc && apt-get clean

# Устанавливаем Python-зависимости
WORKDIR /app
COPY ./worker/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install python-snappy

# Копируем код приложения
COPY ./common /app/common
COPY ./worker /app/worker

# Запускаем приложение (после ожидания, чтобы Kafka и orchestrator успели подняться)
CMD sleep 1 && python -u worker/shard_worker.py
