# Базовый образ
FROM python:3.11-slim

# Устанавливаем системные зависимости и чистим кэш
RUN apt-get update && apt-get install -y \
    gcc \
    libsnappy-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл зависимостей и устанавливаем зависимости
COPY ./api/requirements.txt .

# Установка зависимостей Python
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn[uvicorn] python-snappy

# Копируем исходный код
COPY ./api ./api
COPY ./common ./common

# Указываем команду запуска
CMD ["gunicorn", "api.main:app", "--worker-class", "uvicorn.workers.UvicornWorker", "--workers", "1", "--bind", "0.0.0.0:8000"]
