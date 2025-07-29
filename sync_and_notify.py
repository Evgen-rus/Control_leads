#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Объединённый скрипт для синхронизации Google-таблиц, отправки уведомлений в Telegram и создания лидов в Битрикс24.

Последовательность выполнения:
1. Синхронизация данных между таблицами (sheet_transfer.py)
2. Отправка уведомлений о новых лидах в Telegram (notifier.py)
3. Создание лидов в Битрикс24 (bitrix24_upload.py)

Требования:
- Настроенные переменные окружения в .env файле
- Файл credentials.json для Google Sheets API
- Токен Telegram-бота и Chat ID
- Настроенный вебхук Битрикс24
"""

import asyncio
import logging
import sys
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path
from notifier import notify_new_rows
from bitrix24_upload import BitrixLeadUploader
from sheet_transfer import sync_and_return_new_rows

# Создаём папку для логов если её нет
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Настройка логирования с записью в файл
log_filename = logs_dir / f"sync_and_notify_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(log_filename), encoding='utf-8'),  # Запись в файл
        logging.StreamHandler()  # Вывод в консоль
    ]
)
logger = logging.getLogger(__name__)


async def notify_rows_data(new_rows: List[List[str]]) -> bool:
    """
    Отправляет уведомления в Telegram для переданных строк данных.
    
    Args:
        new_rows (List[List[str]]): Список новых строк для отправки
        
    Returns:
        bool: True если успешно, False при ошибке
    """
    from aiogram import Bot
    from aiogram.utils.text_decorations import html_decoration
    import os
    from dotenv import load_dotenv
    
    def escape_html(text):
        """Простая функция для экранирования HTML символов"""
        if not text:
            return ""
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    # Загружаем переменные окружения
    load_dotenv(override=True)
    
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN_ASSISTANT')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN_ASSISTANT не найден в переменных окружения")
        return False
    
    if not TELEGRAM_CHAT_ID:
        logger.error("TELEGRAM_CHAT_ID не найден в переменных окружения")
        return False
    
    # Преобразуем Chat ID в число для правильной работы с Telegram API
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
    except ValueError:
        logger.error(f"TELEGRAM_CHAT_ID должен быть числом, получено: {TELEGRAM_CHAT_ID}")
        return False
    
    if not new_rows:
        logger.info("Новых строк для Telegram нет")
        return True
    
    async with Bot(token=TELEGRAM_BOT_TOKEN) as bot:
        try:
            logger.info(f"Найдено {len(new_rows)} новых лидов для отправки в Telegram")
            logger.info(f"Используем Chat ID: {TELEGRAM_CHAT_ID}")

            for i, row in enumerate(new_rows, 1):
                try:
                    # Формируем сообщение с проверкой длины строки
                    name = row[2] if len(row) > 2 else "Не указано"
                    phone = row[3] if len(row) > 3 else "Не указано"
                    comment = row[4] if len(row) > 4 else "Не указано"
                    additional_comment = row[5] if len(row) > 5 else "Не указано"
                    audio_link = row[6] if len(row) > 6 else "Не указано"
                    date = row[0] if len(row) > 0 else "Не указано"
                    
                    # Экранируем HTML-символы для безопасности
                    name_escaped = escape_html(name)
                    phone_escaped = escape_html(phone)
                    comment_escaped = escape_html(comment)
                    additional_comment_escaped = escape_html(additional_comment)
                    audio_link_escaped = escape_html(audio_link)
                    date_escaped = escape_html(date)
                    
                    message = (
                        f"Новый лид: {name_escaped} ({phone_escaped})\n\n"
                        f"Имя: {name_escaped}\n\n"
                        f"Телефон: {phone_escaped}\n\n"
                        f"Комментарий: {comment_escaped}\n\n"
                        f"Доп. комментарий: {additional_comment_escaped}\n\n"
                        f"Ссылка на запись: {audio_link_escaped}\n\n"
                        f"Дата лида: {date_escaped}"
                    )
                    
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID, 
                        text=message, 
                        parse_mode="HTML"
                    )
                    
                    logger.info(f"Отправлено уведомление {i}/{len(new_rows)} для лида: {name} ({phone})")
                    
                    # Задержка между сообщениями
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке строки {i}: {e}")
                    logger.error(f"Данные строки: {row}")
                    
            logger.info(f"Завершена отправка уведомлений. Обработано {len(new_rows)} лидов.")
            return True
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке в Telegram: {e}")
            return False


def upload_rows_to_bitrix(new_rows: List[List[str]]) -> Dict[str, int]:
    """
    Создаёт лиды в Битрикс24 для переданных строк данных.
    
    Args:
        new_rows (List[List[str]]): Список новых строк для отправки
        
    Returns:
        Dict[str, int]: Статистика отправки (created, failed)
    """
    try:
        if not new_rows:
            logger.info("Новых строк для отправки в Битрикс24 нет")
            return {"created": 0, "failed": 0}
        
        logger.info(f"Получено {len(new_rows)} новых лидов для отправки в Битрикс24")
        
        # Создаём экземпляр загрузчика
        uploader = BitrixLeadUploader()
        
        # Обрабатываем новые строки
        result = uploader.process_new_rows(new_rows)
        
        return result
        
    except Exception as e:
        logger.error(f"Критическая ошибка при отправке в Битрикс24: {e}")
        return {"created": 0, "failed": 0}


async def main():
    """
    Основная функция для запуска синхронизации, уведомлений и отправки в Битрикс24.
    """
    try:
        start_time = datetime.now()
        logger.info("=== ЗАПУСК ПОЛНОГО ЦИКЛА ОБРАБОТКИ ЛИДОВ ===")
        logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Логи записываются в: {log_filename}")
        
        # Счётчики для статистики
        sync_success = False
        telegram_success = False
        bitrix_result = {"created": 0, "failed": 0}
        new_rows = []
        
        # Этап 1: Синхронизация Google-таблиц
        try:
            logger.info("🔄 ЭТАП 1: Синхронизация Google-таблиц")
            new_rows = sync_and_return_new_rows()
            sync_success = True
            logger.info(f"✅ Этап 1 завершён успешно. Найдено новых лидов: {len(new_rows)}")
        except Exception as e:
            logger.error(f"❌ Ошибка на этапе синхронизации: {e}")
            sync_success = False
            new_rows = []
        
        # Этап 2: Уведомления в Telegram (только если есть новые данные)
        if sync_success and new_rows:
            try:
                logger.info("📱 ЭТАП 2: Отправка уведомлений в Telegram")
                telegram_success = await notify_rows_data(new_rows)
                if telegram_success:
                    logger.info("✅ Этап 2 завершён успешно")
                else:
                    logger.error("❌ Этап 2 завершён с ошибками")
            except Exception as e:
                logger.error(f"❌ Ошибка на этапе уведомлений в Telegram: {e}")
                telegram_success = False
        else:
            logger.info("📱 ЭТАП 2: Пропущен (нет новых данных)")
            telegram_success = True  # Считаем успешным, так как нет данных для обработки
        
        # Этап 3: Отправка лидов в Битрикс24 (только если есть новые данные)
        if sync_success and new_rows:
            try:
                logger.info("🚀 ЭТАП 3: Отправка лидов в Битрикс24")
                bitrix_result = upload_rows_to_bitrix(new_rows)
                logger.info("✅ Этап 3 завершён успешно")
            except Exception as e:
                logger.error(f"❌ Ошибка при отправке лидов в Битрикс24: {e}")
                bitrix_result = {"created": 0, "failed": 0}
        else:
            logger.info("🚀 ЭТАП 3: Пропущен (нет новых данных)")
        
        # Итоговая статистика
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 60)
        logger.info(f"Время выполнения: {duration:.2f} секунд")
        logger.info(f"Синхронизация: {'✅ Успешно' if sync_success else '❌ Ошибка'}")
        logger.info(f"Найдено новых лидов: {len(new_rows)}")
        logger.info(f"Telegram уведомления: {'✅ Успешно' if telegram_success else '❌ Ошибка'}")
        logger.info(f"Лидов создано в Битрикс24: {bitrix_result['created']}")
        logger.info(f"Ошибок при создании лидов: {bitrix_result['failed']}")
        
        if bitrix_result['created'] > 0 or bitrix_result['failed'] > 0:
            total_processed = bitrix_result['created'] + bitrix_result['failed']
            success_rate = (bitrix_result['created'] / total_processed * 100) if total_processed > 0 else 0
            logger.info(f"Процент успеха в Битрикс24: {success_rate:.1f}%")
        
        logger.info("=" * 60)
        logger.info("🎉 ПОЛНЫЙ ЦИКЛ ОБРАБОТКИ ЛИДОВ ЗАВЕРШЁН")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())