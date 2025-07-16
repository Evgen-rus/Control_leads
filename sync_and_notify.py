#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Объединённый скрипт для синхронизации Google-таблиц и отправки уведомлений в Telegram.

Последовательность выполнения:
1. Синхронизация данных между таблицами (sheet_transfer.py)
2. Отправка уведомлений о новых лидах в Telegram (notifier.py)

Требования:
- Настроенные переменные окружения в .env файле
- Файл credentials.json для Google Sheets API
- Токен Telegram-бота и Chat ID
"""

import asyncio
import logging
import sys
from datetime import datetime
from notifier import notify_new_rows

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

async def main():
    """
    Основная функция для запуска синхронизации и уведомлений.
    """
    try:
        start_time = datetime.now()
        logger.info("ЗАПУСК СИНХРОНИЗАЦИИ И УВЕДОМЛЕНИЙ")
        logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Запускаем синхронизацию и уведомления
        await notify_new_rows()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"СИНХРОНИЗАЦИЯ И УВЕДОМЛЕНИЯ ЗАВЕРШЕНЫ")
        logger.info(f"Время выполнения: {duration:.2f} секунд")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())