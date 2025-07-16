import asyncio
import os
import html
from aiogram import Bot
from dotenv import load_dotenv
from sheet_transfer import sync_and_return_new_rows

# Принудительно перезаписываем переменные из .env файла
load_dotenv(override=True)

# Используем правильное название переменной из .env файла
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Проверяем наличие обязательных переменных
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN_ASSISTANT не найден в переменных окружения")

if not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_CHAT_ID не найден в переменных окружения")

# Преобразуем Chat ID в число для правильной работы с Telegram API
try:
    TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
except ValueError:
    raise ValueError(f"TELEGRAM_CHAT_ID должен быть числом, получено: {TELEGRAM_CHAT_ID}")

def escape_html(text):
    """
    Экранирует HTML-символы для безопасной отправки в Telegram.
    
    Args:
        text (str): Текст для экранирования
        
    Returns:
        str: Экранированный текст
    """
    if not text or text == "Не указано":
        return text
    return html.escape(str(text))

async def notify_new_rows():
    """
    Выполняет синхронизацию данных и отправляет уведомления о новых лидах в Telegram.
    
    Структура данных строки:
    - row[0]: Дата Лида
    - row[1]: Номер Лида  
    - row[2]: Имя Лида
    - row[3]: Телефон Лида
    - row[4]: Комментарий лида
    - row[5]: Доп.комментарий
    - row[6]: Ссылка на аудио
    """
    # Используем async context manager для правильного управления соединениями
    async with Bot(token=TELEGRAM_BOT_TOKEN) as bot:
        try:
            print("Запуск синхронизации и уведомлений...")
            new_rows = sync_and_return_new_rows()
            
            if not new_rows:
                print("Новых строк нет, ничего не отправляем.")
                return

            print(f"Найдено {len(new_rows)} новых лидов для отправки в Telegram")
            print(f"Используем Chat ID: {TELEGRAM_CHAT_ID}")

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
                    
                    print(f"Отправлено уведомление {i}/{len(new_rows)} для лида: {name} ({phone})")
                    
                    # Задержка между сообщениями
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"Ошибка при отправке строки {i}: {e}")
                    print(f"Данные строки: {row}")
                    
            print(f"Завершена отправка уведомлений. Обработано {len(new_rows)} лидов.")
            
        except Exception as e:
            print(f"Критическая ошибка в notify_new_rows(): {e}")
            raise

if __name__ == "__main__":
    asyncio.run(notify_new_rows())
