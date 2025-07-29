"""
Скрипт для отправки лидов в Битрикс24 клиента "ПрактикМ".

Получает новые строки из синхронизации Google-таблиц и создаёт лиды в Битрикс24
с заполнением всех необходимых полей согласно требованиям клиента.

Требования:
- requests для работы с API Битрикс24
- Настроенная синхронизация Google-таблиц (sheet_transfer.py)
"""

import requests
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from sheet_transfer import sync_and_return_new_rows

# Загружаем переменные окружения
load_dotenv(override=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Загружаем настройки Битрикс24 из переменных окружения
BITRIX_WEBHOOK_URL = os.getenv('BITRIX_WEBHOOK_URL')
if not BITRIX_WEBHOOK_URL:
    raise ValueError("BITRIX_WEBHOOK_URL не найден в переменных окружения")

# Константы для Битрикс24
RESPONSIBLE_ID = 109  # Михаил
SOURCE_ID = "10"  # ЛидгенБюро
UTM_SOURCE = "leadgenburo"
PIPELINE_STAGE = "NEW"


class BitrixLeadUploader:
    """Класс для отправки лидов в Битрикс24"""
    
    def __init__(self, webhook_url: str = BITRIX_WEBHOOK_URL):
        """
        Инициализация клиента для работы с API Битрикс24
        
        Args:
            webhook_url (str): URL вебхука Битрикс24
        """
        self.webhook_url = webhook_url.rstrip('/')
        self.leads_created = 0
        self.leads_failed = 0
        
    def _make_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Выполняет запрос к API Битрикс24
        
        Args:
            method (str): Метод API (например, 'crm.lead.add')
            params (Dict): Параметры запроса
            
        Returns:
            Dict: Ответ от API Битрикс24
            
        Raises:
            requests.exceptions.RequestException: При ошибке запроса
        """
        url = f"{self.webhook_url}/{method}"
        
        try:
            logger.debug(f"Отправка запроса: {method}")
            logger.debug(f"Параметры: {params}")
            
            response = requests.post(url, json=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            logger.debug(f"Ответ API: {result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при выполнении запроса {method}: {e}")
            logger.error(f"URL запроса: {url}")
            if hasattr(response, 'text'):
                logger.error(f"Ответ сервера: {response.text[:500]}...")
            raise
    
    def _format_comment(self, row_data: List[str]) -> str:
        """
        Форматирует комментарий для лида согласно требованиям
        
        Args:
            row_data (List[str]): Данные строки из Google-таблицы
            
        Returns:
            str: Отформатированный комментарий
        """
        # Структура данных из notifier.py:
        # row[0]: Дата Лида
        # row[1]: Номер Лида  
        # row[2]: Имя Лида
        # row[3]: Телефон Лида
        # row[4]: Комментарий лида
        # row[5]: Доп.комментарий
        # row[6]: Ссылка на аудио
        
        # Безопасное извлечение данных с проверкой длины массива
        date_lead = row_data[0] if len(row_data) > 0 else ""
        lead_number = row_data[1] if len(row_data) > 1 else ""
        name = row_data[2] if len(row_data) > 2 else ""
        phone = row_data[3] if len(row_data) > 3 else ""
        comment = row_data[4] if len(row_data) > 4 else ""
        additional_comment = row_data[5] if len(row_data) > 5 else ""
        audio_link = row_data[6] if len(row_data) > 6 else ""
        
        # Формируем комментарий в требуемом формате
        formatted_comment = f"""Имя: {name}
Телефон: {phone}
Комментарий: {comment}
Доп. комментарий: {additional_comment}
Ссылка на запись: {audio_link}
Дата лида: {date_lead}"""
        
        return formatted_comment
    
    def create_lead(self, row_data: List[str]) -> Dict[str, Any]:
        """
        Создаёт лид в Битрикс24 на основе данных из строки
        
        Args:
            row_data (List[str]): Данные строки из Google-таблицы
            
        Returns:
            Dict[str, Any]: Результат создания лида {success: bool, lead_id: int|None, name: str, phone: str}
        """
        try:
            # Извлекаем основные данные
            name = row_data[2] if len(row_data) > 2 else "Без имени"
            phone = row_data[3] if len(row_data) > 3 else ""
            
            if not phone:
                logger.warning(f"Пропущен лид без телефона: {name}")
                return {"success": False, "lead_id": None, "name": name, "phone": phone, "error": "Отсутствует телефон"}
            
            # Формируем комментарий
            formatted_comment = self._format_comment(row_data)
            
            # Параметры для создания лида
            lead_params = {
                "fields": {
                    "TITLE": f"Лидгенбюро_{phone}_{name}",  # Название лида
                    "NAME": name,  # Имя контакта
                    "PHONE": [{"VALUE": phone, "VALUE_TYPE": "WORK"}],  # Телефон
                    "ASSIGNED_BY_ID": RESPONSIBLE_ID,  # Ответственный (Михаил)
                    "SOURCE_ID": SOURCE_ID,  # Источник (ЛидгенБюро)
                    "STATUS_ID": PIPELINE_STAGE,  # Этап (NEW)
                    "UTM_SOURCE": UTM_SOURCE,  # UTM-источник
                    "COMMENTS": formatted_comment,  # Комментарий
                }
            }
            
            # Отправляем запрос на создание лида
            result = self._make_request("crm.lead.add", lead_params)
            
            # Проверяем результат
            if result.get("result"):
                lead_id = result["result"]
                self.leads_created += 1
                logger.info(f"✅ Лид успешно создан. ID: {lead_id}, Имя: {name}, Телефон: {phone}")
                return {"success": True, "lead_id": lead_id, "name": name, "phone": phone}
            else:
                logger.error(f"❌ Ошибка создания лида для {name}: {result}")
                self.leads_failed += 1
                return {"success": False, "lead_id": None, "name": name, "phone": phone, "error": str(result)}
                
        except Exception as e:
            logger.error(f"❌ Исключение при создании лида для {row_data}: {e}")
            self.leads_failed += 1
            name = row_data[2] if len(row_data) > 2 else "Без имени"
            phone = row_data[3] if len(row_data) > 3 else ""
            return {"success": False, "lead_id": None, "name": name, "phone": phone, "error": str(e)}
    
    def process_new_rows(self, new_rows: List[List[str]]) -> Dict[str, Any]:
        """
        Обрабатывает массив новых строк и создаёт лиды в Битрикс24
        
        Args:
            new_rows (List[List[str]]): Список новых строк из синхронизации
            
        Returns:
            Dict[str, Any]: Статистика обработки (created, failed, leads)
        """
        if not new_rows:
            logger.info("Новых строк для отправки в Битрикс24 нет")
            return {"created": 0, "failed": 0, "leads": []}
        
        logger.info(f"=== Начало отправки {len(new_rows)} лидов в Битрикс24 ===")
        
        # Сбрасываем счётчики
        self.leads_created = 0
        self.leads_failed = 0
        
        # Список для сбора результатов
        leads_results = []
        
        # Обрабатываем каждую строку
        for i, row in enumerate(new_rows, 1):
            logger.info(f"Обработка лида {i}/{len(new_rows)}")
            
            # Создаём лид
            result = self.create_lead(row)
            leads_results.append(result)
            
            if not result["success"]:
                # Логируем данные строки для отладки
                logger.debug(f"Данные неудачной строки: {row}")
        
        # Выводим итоговую статистику
        logger.info(f"=== Завершена отправка в Битрикс24 ===")
        logger.info(f"Успешно создано лидов: {self.leads_created}")
        logger.info(f"Ошибок при создании: {self.leads_failed}")
        logger.info(f"Общий процент успеха: {(self.leads_created / len(new_rows) * 100):.1f}%")
        
        return {
            "created": self.leads_created,
            "failed": self.leads_failed,
            "leads": leads_results
        }


def upload_leads_to_bitrix() -> Dict[str, Any]:
    """
    Основная функция для отправки лидов в Битрикс24.
    Интегрируется с существующей системой синхронизации.
    
    Returns:
        Dict[str, Any]: Статистика отправки (created, failed, leads)
        
    Raises:
        Exception: При критических ошибках
    """
    try:
        logger.info("=== ЗАПУСК ОТПРАВКИ ЛИДОВ В БИТРИКС24 ===")
        
        # Получаем новые строки из синхронизации
        logger.info("Получение новых строк из синхронизации...")
        new_rows = sync_and_return_new_rows()
        
        if not new_rows:
            logger.info("Новых лидов для отправки в Битрикс24 нет")
            return {"created": 0, "failed": 0, "leads": []}
        
        logger.info(f"Получено {len(new_rows)} новых лидов для отправки в Битрикс24")
        
        # Создаём экземпляр загрузчика
        uploader = BitrixLeadUploader()
        
        # Обрабатываем новые строки
        result = uploader.process_new_rows(new_rows)
        
        logger.info("=== ОТПРАВКА ЛИДОВ В БИТРИКС24 ЗАВЕРШЕНА ===")
        return result
        
    except Exception as e:
        logger.error(f"Критическая ошибка в upload_leads_to_bitrix(): {e}")
        raise


def main():
    """
    Основная функция для тестирования скрипта
    """
    try:
        start_time = datetime.now()
        logger.info("=== ТЕСТОВЫЙ ЗАПУСК СКРИПТА БИТРИКС24 ===")
        logger.info(f"Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Запускаем отправку лидов
        result = upload_leads_to_bitrix()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"=== ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ===")
        logger.info(f"Время выполнения: {duration:.2f} секунд")
        logger.info(f"Результат: {result}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}")
        raise


if __name__ == "__main__":
    main() 