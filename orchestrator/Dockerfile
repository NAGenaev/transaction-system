# orchestrator/Dockerfile
FROM python:3.11-slim
# Устанавливаем зависимости
WORKDIR /app
COPY ./orchestrator/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
# Копируем код приложения
COPY ./common /app/common
COPY ./orchestrator /app/orchestrator
# Устанавливаем переменные окружения
ENV KAFKA_URI=kafka:9092
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
# Запускаем приложение
#CMD ["python", "-u", "orchestrator/orchestra.py"]
CMD sleep 15 && python -u orchestrator/orchestra.py