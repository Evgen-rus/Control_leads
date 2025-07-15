import asyncio
import os
from aiogram import Bot
from dotenv import load_dotenv
from sheet_transfer import sync_and_return_new_rows

load_dotenv()

# Используем правильное название переменной из .env файла
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Проверяем наличие обязательных переменных
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN_ASSISTANT не найден в переменных окружения")

if not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_CHAT_ID не найден в переменных окружения")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

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
    try:
        print("Запуск синхронизации и уведомлений...")
        new_rows = sync_and_return_new_rows()
        
        if not new_rows:
            print("Новых строк нет, ничего не отправляем.")
            return

        print(f"Найдено {len(new_rows)} новых лидов для отправки в Telegram")

        for i, row in enumerate(new_rows, 1):
            try:
                # Формируем сообщение с проверкой длины строки
                name = row[2] if len(row) > 2 else "Не указано"
                phone = row[3] if len(row) > 3 else "Не указано"
                comment = row[4] if len(row) > 4 else "Не указано"
                additional_comment = row[5] if len(row) > 5 else "Не указано"
                audio_link = row[6] if len(row) > 6 else "Не указано"
                date = row[0] if len(row) > 0 else "Не указано"
                
                message = (
                    f"🆕 *Новый лид: {name}\\_{phone}*\n\n"
                    f"👤 *Имя:* {name}\n\n"
                    f"📱 *Телефон:* {phone}\n\n"
                    f"💬 *Комментарий:* {comment}\n\n"
                    f"📝 *Доп\\. комментарий:* {additional_comment}\n\n"
                    f"🎧 *Ссылка на запись:* {audio_link}\n\n"
                    f"📅 *Дата лида:* {date}"
                )
                
                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID, 
                    text=message, 
                    parse_mode="MarkdownV2"
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
