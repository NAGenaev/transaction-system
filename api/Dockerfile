FROM python:3.11-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем Python-зависимости
WORKDIR /app
COPY ./api/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn[uvicorn]

# Устанавливаем поддержку snappy
RUN pip install python-snappy

# Копируем код приложения
COPY ./api /app/api
COPY ./common /app/common

# Запуск приложения
CMD ["gunicorn", "api.main:app", "--worker-class", "uvicorn.workers.UvicornWorker", "--workers", "4", "--bind", "0.0.0.0:8000"]

# Запуск приложения (без gunicorn, через uvicorn напрямую)
# CMD ["uvicorn", "api.main:app", "--workers", "4", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--http", "httptools"]
