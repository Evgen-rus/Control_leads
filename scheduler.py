#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Планировщик запуска sheet_transfer.py

Выполняет запуск скрипта sheet_transfer.py каждый час (3600 секунд).
Первый запуск происходит сразу после старта планировщика.
Поддерживает загрузку переменных из .env файла.
"""

import time
import subprocess
import logging
import signal
import sys
import os
from datetime import datetime

# Загружаем переменные из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
    logging.info("Переменные окружения загружены из .env файла")
except ImportError:
    logging.warning("python-dotenv не установлен. Используем системные переменные окружения.")

# Настройка логирования для планировщика
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('scheduler')

# Интервал запуска скрипта в секундах (по умолчанию 10 минут)
INTERVAL_SECONDS = int(os.getenv('SYNC_INTERVAL_SECONDS', 600))

# Флаг для отслеживания запроса на завершение
terminate = False

def run_transfer_script():
    """
    Запускает основной скрипт sheet_transfer.py.
    Выводит логи скрипта напрямую в консоль.
    
    Returns:
        bool: True если скрипт выполнился успешно, False в случае ошибки
    """
    try:
        start_time = datetime.now()
        logger.info("=" * 50)
        logger.info(f"Запуск скрипта sheet_transfer.py в {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        
        # Полный путь к скрипту
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sheet_transfer.py")
        
        if not os.path.exists(script_path):
            logger.error(f"Файл скрипта не найден: {script_path}")
            return False
        
        # Запуск скрипта как отдельного процесса
        result = subprocess.run(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8'
        )
        
        # Выводим логи скрипта
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"[script] {line}")
        
        # Проверка успешного выполнения
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if result.returncode == 0:
            logger.info(f"Скрипт успешно выполнен за {duration:.2f} секунд")
            return True
        else:
            logger.error(f"Ошибка при выполнении скрипта (код возврата: {result.returncode})")
            logger.error(f"Время выполнения: {duration:.2f} секунд")
            return False
            
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
    logger.info("Получен сигнал на завершение работы. Завершаем планировщик...")
    terminate = True

def format_time_interval(seconds):
    """
    Форматирует интервал времени в удобочитаемый вид.
    
    Args:
        seconds (int): Количество секунд
        
    Returns:
        str: Форматированная строка
    """
    if seconds < 60:
        return f"{seconds} секунд"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} минут"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if minutes > 0:
            return f"{hours} часов {minutes} минут"
        else:
            return f"{hours} часов"

def main():
    """
    Основная функция планировщика.
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПЛАНИРОВЩИКА СИНХРОНИЗАЦИИ GOOGLE SHEETS")
    logger.info("=" * 60)
    logger.info(f"Интервал синхронизации: {format_time_interval(INTERVAL_SECONDS)}")
    logger.info(f"Скрипт: sheet_transfer.py")
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['GOOGLE_CREDENTIALS_FILE', 'SRC_ID', 'DST_ID', 'SRC_SHEET', 'DST_SHEET']
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
    
    # Запускаем скрипт сразу при старте планировщика
    logger.info("Выполняем первый запуск скрипта при старте планировщика")
    success = run_transfer_script()
    
    if not success:
        logger.warning("Первый запуск завершился с ошибкой, но планировщик продолжит работу")
    
    logger.info("Планировщик запущен. Для завершения нажмите Ctrl+C")
    
    # Время последнего запуска
    last_run_time = time.time()
    
    # Основной цикл планировщика
    global terminate
    while not terminate:
        # Текущее время
        current_time = time.time()
        
        # Проверяем, прошел ли интервал времени с последнего запуска
        if current_time - last_run_time >= INTERVAL_SECONDS:
            success = run_transfer_script()
            last_run_time = time.time()
            
            if not success:
                logger.warning("Запуск завершился с ошибкой, следующая попытка через установленный интервал")
        
        # Вычисляем время до следующего запуска
        time_to_next_run = INTERVAL_SECONDS - (current_time - last_run_time)
        if time_to_next_run > 0:
            next_run_time = datetime.fromtimestamp(last_run_time + INTERVAL_SECONDS)
            logger.info(f"Следующий запуск: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} "
                       f"(через {format_time_interval(int(time_to_next_run))})")
        
        # Спим 1 минуту для снижения нагрузки на процессор
        # Проверяем флаг завершения каждые 5 секунд
        for _ in range(12):  # 12 * 5 = 60 секунд (1 минута)
            if terminate:
                break
            time.sleep(5)
    
    logger.info("Планировщик завершил работу")
    return 0

if __name__ == "__main__":
    sys.exit(main())
