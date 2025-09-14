# 🎯 DialogsRAG v2 - Система анализа диалогов с клиентами

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.39.0-red.svg)](https://streamlit.io)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o--mini-green.svg)](https://openai.com)

Интеллектуальная система для анализа диалогов с клиентами, использующая LLM для извлечения проблем, идей и сигналов из текстовых транскрипций.

## 🚀 Возможности

- **🤖 AI-анализ диалогов** - автоматическое извлечение проблем клиентов с помощью GPT-4o-mini
- **📊 Интерактивный дашборд** - визуализация результатов с фильтрами и поиском
- **🎯 Категоризация проблем** - группировка по темам и подтемам согласно таксономии
- **📈 Статистический анализ** - метрики частоты, интенсивности и распространенности проблем
- **🔍 Детальное исследование** - поиск по цитатам и анализ confidence
- **📋 Экспорт данных** - результаты в форматах CSV, JSON, JSONL
- **🌐 REST API** - программный доступ к результатам анализа

## 📊 Результаты последнего анализа

- **84 диалога** проанализировано
- **340 упоминаний** извлечено
- **260 проблем** выявлено
- **41 идея** от клиентов
- **39 сигналов** для улучшения

### Топ-проблемы клиентов:
1. **Низкий спрос / мало обращений** - 60.5% диалогов
2. **Продвижение — не окупается** - 49.4% диалогов
3. **Поддержка — не помогло/долго** - 48.1% диалогов
4. **UI/настройки — непонятный интерфейс** - 35.8% диалогов
5. **Доставка — работает выборочно** - 24.7% диалогов

## 🛠 Установка

### Требования
- Python 3.8+
- OpenAI API ключ

### Быстрая установка

```bash
# Клонирование репозитория
git clone https://github.com/denirokp/dialogs-rag_v3.git
cd dialogs-rag_v3

# Установка зависимостей
pip install -r requirements.txt

# Настройка API ключа
export OPENAI_API_KEY="ваш_api_ключ_здесь"
```

### Зависимости

```
pandas>=2.2.2
openpyxl>=3.1.2
pyyaml>=6.0.1
httpx>=0.27.0
streamlit>=1.39.0
plotly>=5.22.0
fastapi>=0.116.1
uvicorn>=0.35.0
tqdm>=4.66.4
```

## 🚀 Использование

### 1. Анализ диалогов

```bash
# Запуск полного анализа
python main.py

# Или пошагово
python analyze_dialogs_advanced.py --model gpt-4o-mini --whole_max 8000 --window_tokens 1800
python consolidate_and_summarize.py
```

### 2. Интерактивный дашборд

```bash
streamlit run simple_dashboard.py
```

Откройте http://localhost:8502 в браузере

### 3. REST API

```bash
python simple_api.py
```

API доступно на http://localhost:8000

## 📁 Структура проекта

```
dialogs-rag-v2/
├── main.py                           # Основной скрипт запуска
├── analyze_dialogs_advanced.py       # LLM анализатор диалогов
├── consolidate_and_summarize.py      # Консолидация и суммирование
├── simple_dashboard.py               # Streamlit дашборд
├── simple_api.py                     # FastAPI сервер
├── data/
│   └── input/
│       └── dialogs14_09.xlsx        # Входные данные диалогов
├── artifacts/                        # Результаты анализа
│   ├── comprehensive_results.json    # Полные результаты
│   ├── statistics.json              # Статистика
│   ├── problems_summary.csv         # Сводка по проблемам
│   ├── problems_mentions.csv        # Все упоминания
│   ├── problems_subthemes.csv       # Детализация по подтемам
│   └── problem_cards.jsonl          # Карточки проблем
├── taxonomy.yaml                     # Таксономия тем и подтем
├── problem_map.yaml                  # Карта консолидации проблем
├── requirements.txt                  # Зависимости Python
└── README.md                         # Документация
```

## 🔧 Конфигурация

### Таксономия проблем

Файл `taxonomy.yaml` определяет структуру тем и подтем для анализа:

```yaml
themes:
  - id: "доставка"
    subthemes: ["не работает выборочно", "не удаётся настроить", "мало ПВЗ"]
  - id: "UI/настройки"
    subthemes: ["непонятный интерфейс", "работает выборочно"]
  # ...
```

### Карта консолидации

Файл `problem_map.yaml` определяет, как темы группируются в проблемы:

```yaml
problems:
  - id: delivery_partial
    title: Доставка — работает выборочно/не на всех товарах
    match:
      - {theme: "доставка", subtheme: "не работает выборочно"}
      - {theme: "UI/настройки", subtheme: "работает выборочно"}
  # ...
```

## 📊 Формат данных

### Входные данные

Excel файл с колонками:
- `ID звонка` - уникальный идентификатор диалога
- `Текст транскрибации` - полный текст диалога

### Выходные данные

#### comprehensive_results.json
```json
{
  "mentions": [
    {
      "dialog_id": "136110475",
      "turn_id": 1,
      "label_type": "problems",
      "theme": "доставка",
      "subtheme": "не работает выборочно",
      "text_quote": "Доставка не работает на некоторых товарах",
      "confidence": 0.85
    }
  ]
}
```

#### problems_summary.csv
```csv
problem_id,problem_title,dialogs,mentions,share_dialogs_pct,freq_per_1k,intensity_mpd
delivery_partial,Доставка — работает выборочно,20,30,24.7,246.9,1.5
```

## 🎛 Параметры анализа

### analyze_dialogs_advanced.py

- `--model` - модель OpenAI (по умолчанию: gpt-4o-mini)
- `--whole_max` - максимальные токены для целого диалога (по умолчанию: 8000)
- `--window_tokens` - размер окна для анализа (по умолчанию: 1800)

### Примеры запуска

```bash
# Быстрый анализ с меньшим окном
python analyze_dialogs_advanced.py --window_tokens 1000

# Анализ с другой моделью
python analyze_dialogs_advanced.py --model gpt-4o

# Анализ с большим окном для длинных диалогов
python analyze_dialogs_advanced.py --whole_max 12000 --window_tokens 2000
```

## 🔍 Дашборд

### Фильтры
- **Типы сущностей** - проблемы, идеи, сигналы
- **Диапазон confidence** - от 0.0 до 1.0
- **Поиск в цитатах** - текстовый поиск
- **Темы и подтемы** - фильтрация по категориям

### Визуализации
- Распределение по типам сущностей
- Топ-проблемы клиентов
- Распределение по темам и подтемам
- Анализ confidence
- Детальная таблица с цитатами

## 🌐 API

### Endpoints

- `GET /` - информация о системе
- `GET /stats` - статистика анализа
- `GET /problems` - список проблем
- `GET /mentions` - все упоминания
- `GET /search?q=query` - поиск по цитатам

### Примеры запросов

```bash
# Получить статистику
curl http://localhost:8000/stats

# Поиск по цитатам
curl "http://localhost:8000/search?q=доставка"

# Получить проблемы
curl http://localhost:8000/problems
```

## 🧪 Тестирование

```bash
# Проверка установки
python -c "import pandas, streamlit, httpx; print('Все зависимости установлены')"

# Тест API
python simple_api.py &
curl http://localhost:8000/stats
```

## 📈 Метрики качества

- **Evidence coverage** - 100% упоминаний содержат цитаты
- **Deduplication** - 0% дубликатов после обработки
- **Ambiguity rate** - 3.2% низкоуверенных упоминаний

## 🔧 Разработка

### Добавление новых тем

1. Обновите `taxonomy.yaml`
2. Обновите `problem_map.yaml` при необходимости
3. Перезапустите анализ

### Кастомизация дашборда

Отредактируйте `simple_dashboard.py` для добавления новых визуализаций или фильтров.

## 🐛 Устранение неполадок

### Ошибка "ENV OPENAI_API_KEY не задан"
```bash
export OPENAI_API_KEY="ваш_ключ_здесь"
```

### Ошибка 401 Unauthorized
Проверьте правильность API ключа OpenAI.

### Ошибка "Нет mentions в comprehensive_results.json"
Запустите сначала `analyze_dialogs_advanced.py`.

## 📝 Лицензия

MIT License

## 👥 Авторы

- Denis K. - разработка системы
- AI Assistant - помощь в реализации

## 📞 Поддержка

При возникновении проблем создайте issue в репозитории или обратитесь к документации в `SYSTEM_STATUS.md`.

---

**Последнее обновление:** 14 сентября 2024