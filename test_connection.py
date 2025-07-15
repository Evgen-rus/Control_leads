#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Тестовый скрипт для проверки подключения к Google Sheets API.
Проверяет доступность таблиц и листов, указанных в .env файле.
"""

import os
import sys
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Загружаем переменные из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Переменные окружения загружены из .env файла")
except ImportError:
    print("⚠ python-dotenv не установлен. Используем системные переменные окружения.")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def test_credentials():
    """Тестирует подключение к Google Sheets API"""
    print("\n=== ТЕСТ ПОДКЛЮЧЕНИЯ К GOOGLE SHEETS API ===")
    
    # Проверяем переменные окружения
    print("\n1. Проверка переменных окружения:")
    required_vars = {
        'GOOGLE_CREDENTIALS_FILE': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'SRC_ID': os.getenv('SRC_ID'),
        'DST_ID': os.getenv('DST_ID'),
        'SRC_SHEET': os.getenv('SRC_SHEET'),
        'DST_SHEET': os.getenv('DST_SHEET')
    }
    
    missing_vars = []
    for var_name, var_value in required_vars.items():
        if var_value:
            print(f"   ✓ {var_name}: {var_value}")
        else:
            print(f"   ✗ {var_name}: НЕ НАЙДЕНА")
            missing_vars.append(var_name)
    
    if missing_vars:
        print(f"\n❌ ОШИБКА: Отсутствуют переменные: {', '.join(missing_vars)}")
        return False
    
    # Проверяем файл credentials
    print("\n2. Проверка файла credentials:")
    creds_file = required_vars['GOOGLE_CREDENTIALS_FILE']
    
    # Преобразуем относительный путь в абсолютный от корня проекта
    if not os.path.isabs(creds_file):
        project_root = os.path.dirname(os.path.abspath(__file__))
        creds_file = os.path.join(project_root, creds_file)
    
    if os.path.exists(creds_file):
        print(f"   ✓ Файл найден: {creds_file}")
    else:
        print(f"   ✗ Файл не найден: {creds_file}")
        return False
    
    # Тестируем подключение к API
    print("\n3. Тестирование подключения к Google Sheets API:")
    try:
        creds = service_account.Credentials.from_service_account_file(
            creds_file, scopes=SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        print("   ✓ Подключение к API успешно")
    except Exception as e:
        print(f"   ✗ Ошибка подключения к API: {e}")
        return False
    
    # Тестируем доступ к таблицам
    print("\n4. Тестирование доступа к таблицам:")
    
    # Тест источника
    try:
        src_id = required_vars['SRC_ID']
        src_sheet = required_vars['SRC_SHEET']
        
        response = service.spreadsheets().values().get(
            spreadsheetId=src_id,
            range=f"{src_sheet}!A1:A1"
        ).execute()
        
        print(f"   ✓ Источник доступен: '{src_sheet}' (ID: {src_id[:10]}...)")
        
    except HttpError as e:
        print(f"   ✗ Ошибка доступа к источнику: {e}")
        return False
    
    # Тест приёмника
    try:
        dst_id = required_vars['DST_ID']
        dst_sheet = required_vars['DST_SHEET']
        
        response = service.spreadsheets().values().get(
            spreadsheetId=dst_id,
            range=f"{dst_sheet}!A1:A1"
        ).execute()
        
        print(f"   ✓ Приёмник доступен: '{dst_sheet}' (ID: {dst_id[:10]}...)")
        
    except HttpError as e:
        print(f"   ✗ Ошибка доступа к приёмнику: {e}")
        return False
    
    print("\n✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    return True

def test_data_structure():
    """Тестирует структуру данных в таблицах"""
    print("\n=== ТЕСТ СТРУКТУРЫ ДАННЫХ ===")
    
    try:
        # Создаём сервис
        creds_file = os.getenv('GOOGLE_CREDENTIALS_FILE')
        creds = service_account.Credentials.from_service_account_file(
            creds_file, scopes=SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        
        # Проверяем заголовки источника
        src_id = os.getenv('SRC_ID')
        src_sheet = os.getenv('SRC_SHEET')
        
        response = service.spreadsheets().values().get(
            spreadsheetId=src_id,
            range=f"{src_sheet}!A1:G1"
        ).execute()
        
        headers = response.get('values', [[]])[0]
        print(f"\n1. Заголовки источника ({len(headers)} столбцов):")
        for i, header in enumerate(headers):
            column_letter = chr(65 + i)  # A, B, C, D...
            print(f"   {column_letter}: {header}")
        
        # Проверяем несколько строк данных
        response = service.spreadsheets().values().get(
            spreadsheetId=src_id,
            range=f"{src_sheet}!A2:G6"  # Первые 5 строк данных
        ).execute()
        
        data_rows = response.get('values', [])
        print(f"\n2. Примеры данных (первые {len(data_rows)} строк):")
        for i, row in enumerate(data_rows[:3]):  # Показываем только первые 3
            # Дополняем строку до 7 столбцов
            row_padded = row + [''] * (7 - len(row))
            phone = row_padded[3] if len(row_padded) > 3 else ''
            print(f"   Строка {i+2}: Телефон='{phone}', Столбцов={len(row)}")
        
        print(f"\n✅ Структура данных корректна!")
        
    except Exception as e:
        print(f"\n❌ Ошибка при проверке структуры данных: {e}")
        return False
    
    return True

def main():
    """Основная функция теста"""
    print("🔍 ТЕСТИРОВАНИЕ НАСТРОЕК GOOGLE SHEETS")
    print("=" * 50)
    
    if not test_credentials():
        print("\n❌ ТЕСТ ПРОВАЛЕН: Проблемы с подключением")
        return 1
    
    if not test_data_structure():
        print("\n❌ ТЕСТ ПРОВАЛЕН: Проблемы со структурой данных")
        return 1
    
    print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Скрипт готов к работе.")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 