# NOT USED IN THIS VERSION!!!
# common/logger.py
import aiologger
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.handlers.files import AsyncFileHandler

# Создаем асинхронный логгер
logger = aiologger.Logger(level="INFO")

# Создаем обработчики
file_handler = AsyncFileHandler("app.log")
stream_handler = AsyncStreamHandler()

# Добавляем обработчики в логгер
logger.add(file_handler)
logger.add(stream_handler)
