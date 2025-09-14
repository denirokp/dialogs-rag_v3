#!/usr/bin/env python3
"""
Migration Script to Enhanced System
Скрипт миграции на улучшенную систему
"""

import json
import shutil
import logging
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd

logger = logging.getLogger(__name__)

def migrate_config():
    """Миграция конфигурации"""
    print("🔄 Мигрируем конфигурацию...")
    
    # Читаем старую конфигурацию
    old_config_path = Path("config.py")
    if old_config_path.exists():
        print("   ✓ Найдена старая конфигурация")
        
        # Создаем новую конфигурацию на основе старой
        new_config = {
            "openai_api_key": "your-api-key-here",
            "processing": {
                "enable_autocorrection": True,
                "enable_adaptive_prompts": True,
                "enable_continuous_learning": True,
                "enable_monitoring": True,
                "enable_scaling": True,
                "max_dialogs_per_batch": 1000,
                "quality_threshold": 0.7,
                "auto_save_results": True,
                "output_directory": "enhanced_results"
            },
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_db": 0
        }
        
        # Сохраняем новую конфигурацию
        with open("enhanced_config.json", "w", encoding="utf-8") as f:
            json.dump(new_config, f, ensure_ascii=False, indent=2)
        
        print("   ✓ Создана новая конфигурация: enhanced_config.json")
    else:
        print("   ⚠️  Старая конфигурация не найдена, создаем новую")

def migrate_data():
    """Миграция данных"""
    print("🔄 Мигрируем данные...")
    
    # Создаем директорию для мигрированных данных
    migrated_dir = Path("migrated_data")
    migrated_dir.mkdir(exist_ok=True)
    
    # Мигрируем диалоги
    dialogs_path = Path("data/dialogs.xlsx")
    if dialogs_path.exists():
        print("   ✓ Найден файл диалогов")
        
        # Копируем файл
        shutil.copy2(dialogs_path, migrated_dir / "dialogs.xlsx")
        print("   ✓ Диалоги скопированы")
    else:
        print("   ⚠️  Файл диалогов не найден")
    
    # Мигрируем результаты
    artifacts_dir = Path("artifacts")
    if artifacts_dir.exists():
        print("   ✓ Найдены артефакты")
        
        # Копируем важные файлы
        important_files = [
            "aggregate_results.json",
            "stage2_extracted.jsonl",
            "stage3_normalized.jsonl",
            "stage4_clusters.json"
        ]
        
        for file_name in important_files:
            file_path = artifacts_dir / file_name
            if file_path.exists():
                shutil.copy2(file_path, migrated_dir / file_name)
                print(f"   ✓ Скопирован {file_name}")
    
    # Мигрируем отчеты
    reports_dir = Path("reports")
    if reports_dir.exists():
        print("   ✓ Найдены отчеты")
        
        # Копируем отчеты
        for report_file in reports_dir.glob("*.json"):
            shutil.copy2(report_file, migrated_dir / report_file.name)
            print(f"   ✓ Скопирован {report_file.name}")

def migrate_prompts():
    """Миграция промптов"""
    print("🔄 Мигрируем промпты...")
    
    # Создаем директорию для промптов
    prompts_dir = Path("enhanced_prompts")
    prompts_dir.mkdir(exist_ok=True)
    
    # Мигрируем существующие промпты
    old_prompts_dir = Path("prompts")
    if old_prompts_dir.exists():
        print("   ✓ Найдены старые промпты")
        
        for prompt_file in old_prompts_dir.glob("*.txt"):
            shutil.copy2(prompt_file, prompts_dir / prompt_file.name)
            print(f"   ✓ Скопирован {prompt_file.name}")
    
    # Создаем новые улучшенные промпты
    enhanced_prompts = {
        "extract_entities_enhanced.txt": """Извлеки ключевые сущности из диалога о доставке.

Диалог: {dialog}

Контекст: Это диалог между клиентом и менеджером службы доставки.

Задачи:
1. Проблемы доставки - конкретные проблемы, с которыми столкнулся клиент
2. Идеи улучшения - предложения по улучшению сервиса
3. Барьеры - препятствия в процессе доставки
4. Релевантные цитаты - точные высказывания клиента

Требования к цитатам:
- Минимум 10 символов
- Содержат ключевые слова о доставке
- Без междометий и мусора
- Максимально информативные

Формат JSON:
{{
    "problems": ["конкретная проблема с деталями"],
    "ideas": ["конкретная идея улучшения"],
    "barriers": ["конкретный барьер"],
    "quotes": ["информативная цитата клиента"]
}}""",
        
        "cluster_labeling_enhanced.txt": """Создай осмысленные метки для кластеров диалогов.

Кластер: {cluster}

Диалоги в кластере: {dialogs}

Требования к меткам:
- Краткие и понятные (2-4 слова)
- Отражают основную тему кластера
- На русском языке
- Без технических терминов

Примеры хороших меток:
- "Проблемы с доставкой"
- "Жалобы на курьеров"
- "Предложения по улучшению"
- "Вопросы по оплате"

Метка: {label}"""
    }
    
    for file_name, content in enhanced_prompts.items():
        with open(prompts_dir / file_name, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"   ✓ Создан {file_name}")

def create_migration_report():
    """Создание отчета о миграции"""
    print("🔄 Создаем отчет о миграции...")
    
    report = {
        "migration_date": "2024-01-15",
        "migrated_components": [
            "Конфигурация системы",
            "Данные диалогов",
            "Артефакты обработки",
            "Отчеты",
            "Промпты"
        ],
        "new_features": [
            "Автокоррекция качества цитат",
            "Адаптивные промпты с A/B тестированием",
            "Непрерывное обучение на новых данных",
            "Мониторинг качества в реальном времени",
            "Масштабирование для больших объемов"
        ],
        "migration_steps": [
            "1. Скопированы данные в migrated_data/",
            "2. Создана новая конфигурация enhanced_config.json",
            "3. Созданы улучшенные промпты в enhanced_prompts/",
            "4. Настроена структура enhanced/",
            "5. Создан главный файл enhanced_main.py"
        ],
        "next_steps": [
            "1. Установить зависимости: pip install -r requirements_enhanced.txt",
            "2. Настроить API ключ в enhanced_config.json",
            "3. Запустить тестовую обработку: python enhanced_main.py --input migrated_data/dialogs.xlsx --output test_results",
            "4. Проверить результаты в test_results/",
            "5. Настроить мониторинг и алерты"
        ]
    }
    
    with open("migration_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print("   ✓ Отчет создан: migration_report.json")

def create_quick_start_guide():
    """Создание руководства по быстрому старту"""
    print("🔄 Создаем руководство по быстрому старту...")
    
    guide = """# 🚀 Быстрый старт с улучшенной системой

## 1. Установка зависимостей

```bash
pip install -r requirements_enhanced.txt
```

## 2. Настройка конфигурации

```bash
# Создайте конфигурацию
python enhanced_main.py --create-config my_config.json

# Отредактируйте API ключ в my_config.json
```

## 3. Тестовая обработка

```bash
# Обработка с включением всех улучшений
python enhanced_main.py --input migrated_data/dialogs.xlsx --enable-all --show-dashboard

# Обработка большого объема
python enhanced_main.py --input migrated_data/dialogs.xlsx --optimize-for 10000
```

## 4. Проверка результатов

- Откройте `enhanced_results/quality_dashboard.html` в браузере
- Изучите `enhanced_results/processing_report.md`
- Проверьте `enhanced_results/enhanced_results.json`

## 5. Настройка мониторинга

- Настройте Redis для кэширования (опционально)
- Настройте алерты в конфигурации
- Запустите мониторинг в фоне

## 6. Оптимизация

- Для 10,000+ диалогов включите масштабирование
- Настройте A/B тестирование промптов
- Включите непрерывное обучение

## Поддержка

- Документация: ENHANCED_README.md
- Логи: enhanced_system.log
- Конфигурация: enhanced_config.json
"""
    
    with open("QUICK_START_ENHANCED.md", "w", encoding="utf-8") as f:
        f.write(guide)
    
    print("   ✓ Руководство создано: QUICK_START_ENHANCED.md")

def main():
    """Главная функция миграции"""
    print("🚀 Начинаем миграцию на улучшенную систему...")
    print("=" * 50)
    
    try:
        # Выполняем миграцию
        migrate_config()
        migrate_data()
        migrate_prompts()
        create_migration_report()
        create_quick_start_guide()
        
        print("=" * 50)
        print("✅ Миграция завершена успешно!")
        print("\n📋 Следующие шаги:")
        print("1. Установите зависимости: pip install -r requirements_enhanced.txt")
        print("2. Настройте API ключ в enhanced_config.json")
        print("3. Запустите тест: python enhanced_main.py --input migrated_data/dialogs.xlsx --output test_results")
        print("4. Изучите результаты в test_results/")
        print("\n📚 Документация:")
        print("- ENHANCED_README.md - полная документация")
        print("- QUICK_START_ENHANCED.md - быстрый старт")
        print("- migration_report.json - отчет о миграции")
        
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        logger.error(f"Ошибка миграции: {e}")

if __name__ == "__main__":
    main()
