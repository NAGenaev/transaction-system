FROM python:3.11-slim

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y libsnappy-dev gcc && apt-get clean

# Устанавливаем Python-зависимости
WORKDIR /app
COPY ./orchestrator/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install python-snappy

# Копируем код приложения
COPY ./common /app/common
COPY ./orchestrator /app/orchestrator

# Запускаем приложение (с задержкой, если нужно дождаться Kafka)
CMD sleep 1 && python -u orchestrator/orchestra.py