# -*- coding: utf-8 -*-

"""
Скрипт для синхронизации данных между Google-таблицами.

Переносит только новые строки из источника в приёмник на основе уникальности 
по столбцу D (Телефон Лида). Копирует данные из столбцов A-G.

Требования:
  - python-dotenv для загрузки .env файла
  - google-api-python-client google-auth-httplib2 google-auth-oauthlib
  - Файл .env с настройками
  - Файл credentials.json для Google Sheets API
"""

import os
import logging
from typing import List, Set, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Загружаем переменные из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logging.warning("python-dotenv не установлен. Используем системные переменные окружения.")

# Константы
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
PHONE_COLUMN_INDEX = 3  # Столбец D (Телефон Лида) - индекс 3
MAX_COLUMNS = 7  # Копируем столбцы A-G (индексы 0-6)

# Настройки из переменных окружения
SRC_ID = os.getenv("SRC_ID")
DST_ID = os.getenv("DST_ID")
SRC_SHEET = os.getenv("SRC_SHEET")
DST_SHEET = os.getenv("DST_SHEET")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def create_sheets_service():
    """
    Создаёт сервис для работы с Google Sheets API.
    
    Returns:
        googleapiclient.discovery.Resource: Сервис Google Sheets
        
    Raises:
        RuntimeError: Если не найден файл credentials или переменная окружения
        Exception: При ошибках авторизации
    """
    try:
        creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
        if not creds_file:
            raise RuntimeError("Не задана переменная среды GOOGLE_CREDENTIALS_FILE")
        
        # Преобразуем относительный путь в абсолютный от корня проекта
        if not os.path.isabs(creds_file):
            project_root = os.path.dirname(os.path.abspath(__file__))
            creds_file = os.path.join(project_root, creds_file)
        
        if not os.path.exists(creds_file):
            raise RuntimeError(f"Файл credentials не найден: {creds_file}")
        
        logger.info(f"Загружаем credentials из: {creds_file}")
        creds = service_account.Credentials.from_service_account_file(
            creds_file, scopes=SCOPES
        )
        
        service = build("sheets", "v4", credentials=creds)
        logger.info("Сервис Google Sheets успешно создан")
        return service
        
    except Exception as e:
        logger.error(f"Ошибка создания сервиса Google Sheets: {e}")
        raise


def get_sheet_data(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    """
    Получает данные из указанного листа Google Таблицы.
    
    Args:
        service: Сервис Google Sheets
        spreadsheet_id: ID таблицы
        sheet_name: Название листа
        
    Returns:
        List[List[str]]: Список строк с данными
        
    Raises:
        HttpError: При ошибках API Google Sheets
    """
    try:
        logger.info(f"Читаем данные из листа '{sheet_name}' (ID: {spreadsheet_id[:10]}...)")
        
        response = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        rows = response.get("values", [])
        logger.info(f"Получено {len(rows)} строк из листа '{sheet_name}'")
        return rows
        
    except HttpError as e:
        logger.error(f"Ошибка при чтении листа '{sheet_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при чтении данных: {e}")
        raise


def normalize_row(row: List[str], max_columns: int) -> List[str]:
    """
    Нормализует строку до указанного количества столбцов.
    Добавляет пустые ячейки если строка короче, обрезает если длиннее.
    
    Args:
        row: Исходная строка
        max_columns: Максимальное количество столбцов
        
    Returns:
        List[str]: Нормализованная строка
    """
    normalized = row[:max_columns]  # Обрезаем до max_columns
    while len(normalized) < max_columns:  # Дополняем пустыми ячейками
        normalized.append("")
    return normalized


def extract_phone_numbers(rows: List[List[str]]) -> Set[str]:
    """
    Извлекает номера телефонов из строк (столбец D, индекс 3).
    
    Args:
        rows: Список строк с данными
        
    Returns:
        Set[str]: Множество номеров телефонов
    """
    phones = set()
    for row in rows:
        if len(row) > PHONE_COLUMN_INDEX and row[PHONE_COLUMN_INDEX].strip():
            phone = row[PHONE_COLUMN_INDEX].strip()
            phones.add(phone)
    
    logger.info(f"Найдено {len(phones)} уникальных телефонов")
    return phones


def filter_new_rows(src_rows: List[List[str]], existing_phones: Set[str]) -> List[List[str]]:
    """
    Фильтрует новые строки, которых ещё нет в приёмнике.
    Сравнение происходит по столбцу D (Телефон Лида).
    
    Args:
        src_rows: Строки из источника (без заголовка)
        existing_phones: Множество существующих телефонов в приёмнике
        
    Returns:
        List[List[str]]: Список новых строк для добавления
    """
    new_rows = []
    
    for row in src_rows:
        # Нормализуем строку до 7 столбцов (A-G)
        normalized_row = normalize_row(row, MAX_COLUMNS)
        
        # Проверяем наличие телефона в столбце D
        if len(normalized_row) > PHONE_COLUMN_INDEX:
            phone = normalized_row[PHONE_COLUMN_INDEX].strip()
            
            # Если телефон не пустой и его нет в приёмнике
            if phone and phone not in existing_phones:
                new_rows.append(normalized_row)
                logger.debug(f"Новая строка: {normalized_row[:3]}... (телефон: {phone})")
    
    logger.info(f"Найдено {len(new_rows)} новых строк для добавления")
    return new_rows


def append_rows_to_sheet(service, spreadsheet_id: str, sheet_name: str, rows: List[List[str]]) -> int:
    """
    Добавляет строки в конец указанного листа.
    
    Args:
        service: Сервис Google Sheets
        spreadsheet_id: ID таблицы
        sheet_name: Название листа
        rows: Строки для добавления
        
    Returns:
        int: Количество добавленных строк
        
    Raises:
        HttpError: При ошибках API Google Sheets
    """
    if not rows:
        logger.info("Новых строк нет — ничего не добавляем")
        return 0
    
    try:
        logger.info(f"Добавляем {len(rows)} строк в лист '{sheet_name}'")
        
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()
        
        logger.info(f"Успешно добавлено {len(rows)} строк")
        return len(rows)
        
    except HttpError as e:
        logger.error(f"Ошибка при добавлении строк в лист '{sheet_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при добавлении строк: {e}")
        raise


def main():
    """
    Основная функция скрипта.
    Выполняет синхронизацию данных между таблицами.
    """
    try:
        logger.info("=== Запуск синхронизации данных ===")
        logger.info(f"Источник: {SRC_SHEET} (ID: {SRC_ID[:10]}...)")
        logger.info(f"Приёмник: {DST_SHEET} (ID: {DST_ID[:10]}...)")
        
        # Создаём сервис Google Sheets
        service = create_sheets_service()
        
        # Читаем данные из источника и приёмника
        src_rows = get_sheet_data(service, SRC_ID, SRC_SHEET)
        dst_rows = get_sheet_data(service, DST_ID, DST_SHEET)
        
        # Проверяем, что источник не пуст
        if not src_rows:
            logger.warning("Источник пуст — нечего синхронизировать")
            return
        
        if len(src_rows) <= 1:
            logger.warning("В источнике только заголовок — нет данных для синхронизации")
            return
        
        # Извлекаем существующие телефоны из приёмника (пропускаем заголовок)
        existing_phones = extract_phone_numbers(dst_rows[1:] if len(dst_rows) > 1 else [])
        
        # Фильтруем новые строки из источника (пропускаем заголовок)
        new_rows = filter_new_rows(src_rows[1:], existing_phones)
        
        # Добавляем новые строки в приёмник
        added_count = append_rows_to_sheet(service, DST_ID, DST_SHEET, new_rows)
        
        logger.info(f"=== Синхронизация завершена. Добавлено {added_count} новых строк ===")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}")
        raise


if __name__ == "__main__":
    main()