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

# Создаём папку для логов (абсолютный путь рядом со скриптом)
logs_dir = (Path(__file__).resolve().parent / "logs")
logs_dir.mkdir(exist_ok=True)

# Настройка логирования с записью в файл
log_filename = logs_dir / f"sync_and_notify_{datetime.now().strftime('%Y%m%d')}.log"

# WARNING: при успешном прогоне лог молчит; пишем только ошибки и предупреждения
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(log_filename), encoding='utf-8'),  # Запись в файл
        logging.StreamHandler()  # Вывод в консоль
    ],
    force=True  # Принудительно переопределяем конфигурацию, если она уже была настроена при импорте
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
        return True
    
    async with Bot(token=TELEGRAM_BOT_TOKEN) as bot:
        try:
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
                    
                    # Задержка между сообщениями
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке строки {i}: {e}")
                    logger.error(f"Данные строки: {row}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке в Telegram: {e}")
            return False


def upload_rows_to_bitrix(new_rows: List[List[str]]) -> Dict[str, Any]:
    """
    Создаёт лиды в Битрикс24 для переданных строк данных.
    
    Args:
        new_rows (List[List[str]]): Список новых строк для отправки
        
    Returns:
        Dict[str, Any]: Статистика отправки (created, failed, leads)
    """
    try:
        if not new_rows:
            return {"created": 0, "failed": 0, "leads": []}
        
        # Создаём экземпляр загрузчика
        uploader = BitrixLeadUploader()
        
        # Обрабатываем новые строки
        result = uploader.process_new_rows(new_rows)
        
        return result
        
    except Exception as e:
        logger.error(f"Критическая ошибка при отправке в Битрикс24: {e}")
        return {"created": 0, "failed": 0, "leads": []}


async def send_bitrix_notification(bitrix_result: Dict[str, Any]) -> bool:
    """
    Отправляет уведомление в Telegram о результатах создания лидов в Битрикс24.
    
    Args:
        bitrix_result (Dict[str, Any]): Результат обработки лидов в Битрикс24
        
    Returns:
        bool: True если успешно, False при ошибке
    """
    from aiogram import Bot
    import os
    from dotenv import load_dotenv
    
    # Загружаем переменные окружения
    load_dotenv(override=True)
    
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN_ASSISTANT')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    BITRIX_DOMAIN = os.getenv('BITRIX_WEBHOOK_URL', '').split('/rest/')[0] if os.getenv('BITRIX_WEBHOOK_URL') else ""
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Отсутствуют настройки Telegram для уведомлений о Битрикс24")
        return False
    
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
    except ValueError:
        logger.error(f"TELEGRAM_CHAT_ID должен быть числом: {TELEGRAM_CHAT_ID}")
        return False
    
    # Проверяем, есть ли что отправлять
    if bitrix_result.get("created", 0) == 0 and bitrix_result.get("failed", 0) == 0:
        return True
    
    async with Bot(token=TELEGRAM_BOT_TOKEN) as bot:
        try:
            # Формируем основное сообщение
            created = bitrix_result.get("created", 0)
            failed = bitrix_result.get("failed", 0)
            leads = bitrix_result.get("leads", [])
            
            # Заголовок сообщения
            if created > 0:
                message = f"🚀 <b>Результаты отправки в Битрикс24</b>\n\n"
                message += f"✅ Успешно создано лидов: <b>{created}</b>\n"
                if failed > 0:
                    message += f"❌ Ошибок при создании: <b>{failed}</b>\n"
                
                # Добавляем ссылки на успешно созданные лиды
                successful_leads = [lead for lead in leads if lead.get("success") and lead.get("lead_id")]
                
                if successful_leads and BITRIX_DOMAIN:
                    message += "🔗 <b>Ссылки на созданные лиды:</b>\n"
                    for lead in successful_leads[:5]:  # Ограничиваем количество ссылок
                        lead_id = lead["lead_id"]
                        name = lead["name"]
                        phone = lead["phone"]
                        lead_url = f"{BITRIX_DOMAIN}/crm/lead/details/{lead_id}/"
                        message += f"• <a href='{lead_url}'>{name} ({phone})</a>\n"
                    
                    if len(successful_leads) > 5:
                        message += f"... и ещё {len(successful_leads) - 5} лидов\n"
                
            else:
                message = f"❌ <b>Ошибки при создании лидов в Битрикс24</b>\n\n"
                message += f"Не удалось создать ни одного лида из {failed} попыток"
            
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления о Битрикс24: {e}")
            return False


async def main():
    """
    Основная функция для запуска синхронизации, уведомлений и отправки в Битрикс24.
    """
    try:
        # Счётчики для статистики
        sync_success = False
        telegram_success = False
        bitrix_result = {"created": 0, "failed": 0, "leads": []}
        bitrix_notification_success = False
        new_rows = []
        
        # Этап 1: Синхронизация Google-таблиц
        try:
            new_rows = sync_and_return_new_rows()
            sync_success = True
        except Exception as e:
            logger.error(f"Ошибка на этапе синхронизации: {e}")
            sync_success = False
            new_rows = []
        
        # Этап 2: Уведомления в Telegram (только если есть новые данные)
        if sync_success and new_rows:
            try:
                telegram_success = await notify_rows_data(new_rows)
                if not telegram_success:
                    logger.error("Этап 2 (Telegram) завершён с ошибками")
            except Exception as e:
                logger.error(f"Ошибка на этапе уведомлений в Telegram: {e}")
                telegram_success = False
        else:
            telegram_success = True  # Считаем успешным, так как нет данных для обработки
        
        # Этап 3: Отправка лидов в Битрикс24 (только если есть новые данные)
        if sync_success and new_rows:
            try:
                bitrix_result = upload_rows_to_bitrix(new_rows)
                
                # Этап 4: Уведомление о результатах Битрикс24
                bitrix_notification_success = await send_bitrix_notification(bitrix_result)
                if not bitrix_notification_success:
                    logger.error("Этап 4 (уведомление о Битрикс24) завершён с ошибками")
                    
            except Exception as e:
                logger.error(f"Ошибка при отправке лидов в Битрикс24: {e}")
                bitrix_result = {"created": 0, "failed": 0, "leads": []}
        else:
            # Этапы 3–4 пропущены — это не ошибка
            bitrix_notification_success = True
        
        # Итог пишем только если что-то пошло не так
        created = bitrix_result.get("created", 0)
        failed = bitrix_result.get("failed", 0)
        if (
            not sync_success
            or not telegram_success
            or not bitrix_notification_success
            or failed > 0
        ):
            logger.error(
                f"Цикл завершён с проблемами: sync={'ok' if sync_success else 'fail'}, "
                f"Telegram={'ok' if telegram_success else 'fail'}, "
                f"уведомление Битрикс={'ok' if bitrix_notification_success else 'fail'}, "
                f"новых строк={len(new_rows)}, создано={created}, ошибок Битрикс={failed}"
            )
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())