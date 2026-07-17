#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Планировщик запуска sync_and_notify.py

Запускает sync_and_notify.py через заданный интервал,
но только в рабочие часы по Москве: пн–пт, с 08:00 до 18:00.
Вне этого окна и в выходные скрипт не запускается.
Поддерживает загрузку переменных из .env файла.
"""

import time
import subprocess
import logging
import signal
import sys
import os
from datetime import datetime
from pathlib import Path

import pytz

# Загружаем переменные из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    print("python-dotenv не установлен. Используем системные переменные окружения.")

# Создаём папку для логов рядом со скриптом (абсолютный путь)
logs_dir = (Path(__file__).resolve().parent / "logs")
logs_dir.mkdir(exist_ok=True)

# Настройка логирования с записью в файл
log_filename = logs_dir / f"scheduler_{datetime.now().strftime('%Y%m%d')}.log"

# WARNING: при успешной работе лог молчит; пишем только ошибки и предупреждения
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(str(log_filename), encoding='utf-8'),  # Запись в файл
        logging.StreamHandler(sys.stdout)  # Вывод в консоль
    ]
)
logger = logging.getLogger('scheduler')

# Интервал запуска скрипта в секундах (по умолчанию 5 минут)
INTERVAL_SECONDS = int(os.getenv('SYNC_INTERVAL_SECONDS', 300))

# Рабочие часы по Москве: с 8:00 включительно до 18:00 невключая
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
WORK_START_HOUR = int(os.getenv('WORK_START_HOUR', 8))
WORK_END_HOUR = int(os.getenv('WORK_END_HOUR', 18))

# Флаг для отслеживания запроса на завершение
terminate = False


def is_working_time(now=None):
    """
    Проверяет, сейчас ли рабочее время по Москве (пн–пт, 08:00–18:00).

    Returns:
        bool: True если можно запускать sync_and_notify.py
    """
    if now is None:
        now = datetime.now(MOSCOW_TZ)
    elif now.tzinfo is None:
        now = MOSCOW_TZ.localize(now)
    else:
        now = now.astimezone(MOSCOW_TZ)

    # weekday(): пн=0 ... вс=6
    if now.weekday() >= 5:
        return False

    work_start = now.replace(
        hour=WORK_START_HOUR, minute=0, second=0, microsecond=0
    )
    work_end = now.replace(
        hour=WORK_END_HOUR, minute=0, second=0, microsecond=0
    )
    return work_start <= now < work_end


def run_sync_and_notify_script():
    """
    Запускает скрипт sync_and_notify.py для синхронизации и отправки уведомлений.
    
    Returns:
        bool: True если скрипт выполнился успешно, False в случае ошибки
    """
    try:
        # Полный путь к скрипту
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_and_notify.py")
        
        if not os.path.exists(script_path):
            logger.error(f"Файл скрипта не найден: {script_path}")
            return False
        
        # Запуск скрипта как отдельного процесса
        # Определяем кодировку в зависимости от системы
        import locale
        system_encoding = locale.getpreferredencoding()
        
        try:
            # Устанавливаем переменные окружения для UTF-8
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            
            # Пробуем запустить с UTF-8
            result = subprocess.run(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env,
                cwd=os.path.dirname(script_path)  # гарантируем запуск в директории проекта
            )
        except UnicodeDecodeError:
            # Если не получилось, используем системную кодировку
            logger.warning(f"Проблема с UTF-8, переключаемся на {system_encoding}")
            env['PYTHONIOENCODING'] = system_encoding
            result = subprocess.run(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding=system_encoding,
                errors='replace',
                env=env,
                cwd=os.path.dirname(script_path)  # гарантируем запуск в директории проекта
            )
        
        # Логи дочернего скрипта — только если он упал
        if result.returncode != 0:
            logger.error(f"Ошибка при выполнении скрипта (код возврата: {result.returncode})")
            if result.stdout:
                for line in result.stdout.splitlines():
                    if line.strip():
                        logger.error(f"[script] {line}")
            return False

        return True
            
    except FileNotFoundError:
        logger.error("Python интерпретатор не найден")
        return False
    except subprocess.TimeoutExpired:
        logger.error("Превышено время ожидания выполнения скрипта")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запуске скрипта: {e}")
        return False


def signal_handler(sig, frame):
    """
    Обработчик сигналов для корректного завершения работы планировщика.
    """
    global terminate
    terminate = True


def main():
    """
    Основная функция планировщика.
    """   
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['GOOGLE_CREDENTIALS_FILE', 'SRC_ID', 'DST_ID', 'SRC_SHEET', 'DST_SHEET', 'TELEGRAM_BOT_TOKEN_ASSISTANT', 'TELEGRAM_CHAT_ID']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        logger.error("Проверьте файл .env или системные переменные окружения")
        return 1
    
    # Регистрируем обработчик сигналов для корректного завершения
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Первый запуск — только в рабочее время по Москве
    last_run_time = 0  # 0 = ещё не запускали; при входе в окно запустится сразу
    if is_working_time():
        success = run_sync_and_notify_script()
        last_run_time = time.time()
        if not success:
            logger.warning("Первый запуск завершился с ошибкой, но планировщик продолжит работу")
    
    # Основной цикл планировщика
    global terminate
    while not terminate:
        current_time = time.time()

        # Вне пн–пт 08:00–18:00 МСК синхронизацию не запускаем
        if is_working_time() and (current_time - last_run_time >= INTERVAL_SECONDS):
            success = run_sync_and_notify_script()
            last_run_time = time.time()
            
            if not success:
                logger.warning("Запуск завершился с ошибкой, следующая попытка через установленный интервал")
        
        # Спим 1 минуту для снижения нагрузки на процессор
        # Проверяем флаг завершения каждые 5 секунд
        for _ in range(12):  # 12 * 5 = 60 секунд (1 минута)
            if terminate:
                break
            time.sleep(5)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
