# Control_leads - Система управления лидами

Автоматизированная система для синхронизации данных между Google-таблицами и отправки уведомлений о новых лидах в Telegram.

## 📋 Описание проекта

Система предназначена для автоматического переноса новых записей лидов из исходной Google-таблицы в целевую таблицу с отправкой уведомлений в Telegram-чат. Система оптимизирована для работы с большими объемами данных и включает механизмы защиты от дублирования.

### 🎯 Основные возможности

- ✅ **Автоматическая синхронизация** данных между Google-таблицами
- ✅ **Уведомления в Telegram** о новых лидах в реальном времени
- ✅ **Защита от дублирования** на основе уникальности телефонов
- ✅ **Оптимизация производительности** с анализом только недавних данных
- ✅ **Планировщик задач** для автоматического запуска
- ✅ **Обработка ошибок** с автоматическими повторами
- ✅ **Подробное логирование** всех операций

## 🏗️ Архитектура проекта

### Основные компоненты

1. **`sheet_transfer.py`** - Основной модуль синхронизации Google-таблиц
2. **`notifier.py`** - Модуль отправки уведомлений в Telegram
3. **`sync_and_notify.py`** - Объединённый скрипт для синхронизации и уведомлений
4. **`scheduler.py`** - Планировщик для автоматического запуска
5. **`.env`** - Конфигурационный файл с настройками

### Структура данных

Система работает со следующими столбцами Google-таблиц:
- **A** - Дата Лида
- **B** - Номер Лида
- **C** - Имя Лида
- **D** - Телефон Лида (ключ уникальности)
- **E** - Комментарий лида
- **F** - Дополнительный комментарий
- **G** - Ссылка на аудио

## 🚀 Установка и настройка

### Требования

- Python 3.8+
- Google Sheets API
- Telegram Bot API

### Установка зависимостей

```bash
# Создание виртуальной среды
python -m venv venv

# Активация виртуальной среды
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt
```

### Настройка Google Sheets API

1. **Создание сервисного аккаунта:**
   - Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
   - Создайте новый проект или выберите существующий
   - Включите Google Sheets API
   - Создайте сервисный аккаунт
   - Скачайте JSON-файл с ключами

2. **Настройка доступа к таблицам:**
   - Разместите JSON-файл в папке `credentials/`
   - Предоставьте доступ к Google-таблицам для email сервисного аккаунта

### Настройка Telegram-бота

1. **Создание бота:**
   - Обратитесь к [@BotFather](https://t.me/BotFather) в Telegram
   - Создайте нового бота командой `/newbot`
   - Получите токен бота

2. **Получение Chat ID:**
   - Добавьте бота в нужный чат/канал
   - Используйте [@userinfobot](https://t.me/userinfobot) для получения Chat ID
   - Для каналов Chat ID будет отрицательным числом

## ⚙️ Конфигурация

### Файл .env

Создайте файл `.env` в корне проекта:

```env
# Google Sheets credentials
GOOGLE_CREDENTIALS_FILE=credentials/your-service-account.json

# Google Sheets IDs
SRC_ID=your_source_spreadsheet_id
DST_ID=your_destination_spreadsheet_id

# Google Sheets Sheet names
SRC_SHEET=Исходный лист
DST_SHEET=Целевой лист

# Интервал синхронизации в секундах (по умолчанию 300 = 5 минут)
SYNC_INTERVAL_SECONDS=300

# Оптимизация: количество дней для анализа данных
ANALYSIS_DAYS_DEPTH=3

# Оптимизация источника (true/false)
OPTIMIZE_SOURCE=true

# Telegram Bot
TELEGRAM_BOT_TOKEN_ASSISTANT=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### Параметры конфигурации

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `SYNC_INTERVAL_SECONDS` | Интервал синхронизации в секундах | 300 (5 минут) |
| `ANALYSIS_DAYS_DEPTH` | Глубина анализа данных в днях | 3 |
| `OPTIMIZE_SOURCE` | Оптимизация обработки источника | true |

## 📖 Использование

### Ручной запуск синхронизации

```bash
# Запуск только синхронизации
python sheet_transfer.py

# Запуск синхронизации с уведомлениями
python sync_and_notify.py
```

### Автоматический запуск через планировщик

```bash
# Запуск планировщика
python scheduler.py
```

Планировщик будет автоматически запускать синхронизацию через заданные интервалы времени.

### Структура команд

```bash
# Основные команды
python sheet_transfer.py          # Только синхронизация
python sync_and_notify.py        # Синхронизация + уведомления
python scheduler.py              # Планировщик
python notifier.py              # Только уведомления (тест)
```

## 📁 Структура файлов

```
Control_leads/
├── README.md                    # Документация проекта
├── requirements.txt             # Зависимости Python
├── .env                        # Конфигурация (создать самостоятельно)
├── .gitignore                  # Исключения Git
├── credentials/                # Папка с ключами Google API
│   └── your-service-account.json
├── venv/                      # Виртуальная среда Python
├── sheet_transfer.py          # Основной модуль синхронизации
├── notifier.py                # Модуль уведомлений Telegram
├── sync_and_notify.py         # Объединённый скрипт
├── scheduler.py               # Планировщик задач
└── бот аиограм.md             # Документация Telegram-бота
```

## 🔧 Функциональность модулей

### sheet_transfer.py

**Основные функции:**
- `create_sheets_service()` - Создание сервиса Google Sheets
- `get_sheet_data()` - Получение данных из таблицы
- `sync_and_return_new_rows()` - Основная функция синхронизации
- `append_rows_to_sheet()` - Добавление новых строк

**Особенности:**
- Автоматические повторы при ошибках сети
- Оптимизация по датам для больших таблиц
- Защита от дублирования по телефону
- Подробное логирование операций

### notifier.py

**Основные функции:**
- `notify_new_rows()` - Отправка уведомлений о новых лидах
- `escape_html()` - Безопасное экранирование HTML

**Особенности:**
- Красивое форматирование сообщений с эмодзи
- Безопасное экранирование пользовательских данных
- Правильное управление соединениями
- Обработка ошибок отправки

### scheduler.py

**Основные функции:**
- `run_transfer_script()` - Запуск скрипта синхронизации
- `main()` - Основной цикл планировщика

**Особенности:**
- Настраиваемый интервал запуска
- Корректное завершение по сигналам
- Логирование всех операций
- Обработка ошибок выполнения

## 🛠️ Устранение неполадок

### Частые проблемы

1. **Ошибка "ModuleNotFoundError: No module named 'tenacity'"**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ошибка "chat not found" в Telegram**
   - Проверьте правильность TELEGRAM_CHAT_ID
   - Убедитесь, что бот добавлен в чат
   - Для каналов используйте отрицательный Chat ID

3. **Ошибка доступа к Google Sheets**
   - Проверьте правильность файла credentials
   - Убедитесь, что сервисный аккаунт имеет доступ к таблицам

4. **Проблемы с переменными окружения**
   - Проверьте файл .env
   - Используйте `load_dotenv(override=True)` для принудительной загрузки

### Логи и отладка

Система ведёт подробные логи всех операций:
- Время выполнения операций
- Количество обработанных строк
- Ошибки и предупреждения
- Статистика синхронизации

## 🔒 Безопасность

- Все пользовательские данные экранируются перед отправкой в Telegram
- Используется сервисный аккаунт Google для безопасного доступа
- Токены и ключи хранятся в отдельном файле .env
- Логи не содержат чувствительной информации

## 📈 Производительность

### Оптимизации

- **Анализ по датам:** Обрабатываются только недавние данные
- **Кэширование:** Повторное использование соединений
- **Пакетная обработка:** Эффективная работа с большими таблицами
- **Повторы при ошибках:** Автоматическое восстановление после сбоев

### Рекомендации

- Используйте интервал синхронизации не менее 5 минут
- Настройте ANALYSIS_DAYS_DEPTH в зависимости от объема данных
- Мониторьте логи для выявления проблем производительности

## 🤝 Поддержка

При возникновении проблем:

1. Проверьте логи выполнения
2. Убедитесь в правильности конфигурации
3. Проверьте доступность Google Sheets API
4. Убедитесь в работоспособности Telegram-бота

## 📄 Лицензия

Проект разработан для внутреннего использования. Все права защищены.

---

**Версия:** 1.0  
**Последнее обновление:** 2025-07-16  
**Автор:** Система управления лидами 