#!/usr/bin/env python3
"""
🚀 ЗАПУСК ФИНАЛЬНОГО ПАЙПЛАЙНА
Скрипт для запуска полной системы анализа диалогов
"""

import asyncio
import sys
import os
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.append(str(Path(__file__).parent))

from final_pipeline import main

def print_banner():
    """Печать баннера системы"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║        🎯 ФИНАЛЬНЫЙ ПАЙПЛАЙН DIALOGS RAG SYSTEM            ║
    ║                                                              ║
    ║  ✅ Автокоррекция качества цитат                            ║
    ║  ✅ Адаптивные промпты с A/B тестированием                 ║
    ║  ✅ Непрерывное обучение на новых данных                    ║
    ║  ✅ Real-time мониторинг качества                           ║
    ║  ✅ Масштабирование для больших объемов                    ║
    ║  ✅ Автоматический анализ и отчеты                          ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_usage():
    """Печать инструкции по использованию"""
    usage = """
    📖 ИСПОЛЬЗОВАНИЕ:
    
    # Базовая обработка
    python run_final_pipeline.py --input data/dialogs.xlsx
    
    # С кастомной конфигурацией
    python run_final_pipeline.py --input data/dialogs.xlsx --config final_pipeline_config.json
    
    # С указанием выходной директории
    python run_final_pipeline.py --input data/dialogs.xlsx --output my_results
    
    # Обработка JSON файла
    python run_final_pipeline.py --input dialogs.json --output results
    
    # Обработка текстового файла
    python run_final_pipeline.py --input dialogs.txt --output results
    
    📁 ПОДДЕРЖИВАЕМЫЕ ФОРМАТЫ:
    • Excel (.xlsx) - диалоги в колонке 'Текст транскрибации' или первой колонке
    • JSON (.json) - массив диалогов или объект с ключом 'dialogs'
    • Текст (.txt) - один диалог на строку
    
    📊 РЕЗУЛЬТАТЫ:
    • final_results.json - основные результаты обработки
    • final_statistics.json - статистика и метрики
    • learning_data.json - данные для обучения системы
    • ab_test_results.json - результаты A/B тестирования
    • final_report.md - подробный отчет
    • quality_dashboard.html - интерактивный дашборд (если включен)
    
    🔧 КОМПОНЕНТЫ СИСТЕМЫ:
    • Автокоррекция - автоматическое исправление ошибок в цитатах
    • A/B тестирование - автоматический выбор лучших промптов
    • Непрерывное обучение - улучшение на основе новых данных
    • Мониторинг - отслеживание качества в реальном времени
    • Масштабирование - обработка больших объемов данных
    • Анализ - автоматическая генерация отчетов и рекомендаций
    """
    print(usage)

def check_dependencies():
    """Проверка зависимостей"""
    try:
        import pandas
        import numpy
        import tqdm
        import asyncio
        print("✅ Основные зависимости найдены")
        return True
    except ImportError as e:
        print(f"❌ Отсутствует зависимость: {e}")
        print("Установите зависимости: pip install -r requirements_enhanced.txt")
        return False

def main_wrapper():
    """Обертка для главной функции"""
    print_banner()
    
    # Проверка аргументов
    if len(sys.argv) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print_usage()
        return
    
    # Проверка зависимостей
    if not check_dependencies():
        return
    
    # Проверка входного файла
    input_file = None
    for i, arg in enumerate(sys.argv):
        if arg in ["--input", "-i"] and i + 1 < len(sys.argv):
            input_file = sys.argv[i + 1]
            break
    
    if input_file and not Path(input_file).exists():
        print(f"❌ Файл не найден: {input_file}")
        return
    
    print("🚀 Запуск финального пайплайна...")
    print("=" * 60)
    
    try:
        # Запуск асинхронной функции
        asyncio.run(main())
        print("=" * 60)
        print("🎉 Финальный пайплайн завершен успешно!")
        print("📊 Проверьте результаты в указанной директории")
        
    except KeyboardInterrupt:
        print("\n⏹️  Остановка по запросу пользователя")
    except Exception as e:
        print(f"\n❌ Ошибка выполнения: {e}")
        print("Проверьте логи для подробной информации")

if __name__ == "__main__":
    main_wrapper()
