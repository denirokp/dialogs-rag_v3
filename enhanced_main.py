#!/usr/bin/env python3
"""
Enhanced Dialogs RAG System - Main Entry Point
Главный файл для запуска улучшенной системы анализа диалогов
"""

import asyncio
import json
import logging
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

# Добавляем путь к модулям
sys.path.append(str(Path(__file__).parent))

from enhanced.integrated_system import IntegratedQualitySystem, ProcessingConfig

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_system.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def load_dialogs_from_file(file_path: str) -> List[str]:
    """Загрузка диалогов из файла"""
    file_path = Path(file_path)
    
    if file_path.suffix == '.xlsx':
        # Загрузка из Excel
        df = pd.read_excel(file_path)
        # Предполагаем, что диалоги в колонке 'dialog' или колонке с текстом
        if 'dialog' in df.columns:
            dialogs = df['dialog'].dropna().tolist()
        elif 'Текст транскрибации' in df.columns:
            dialogs = df['Текст транскрибации'].dropna().tolist()
        else:
            # Ищем колонку с текстом (самая длинная строка)
            text_columns = []
            for col in df.columns:
                if df[col].dtype == 'object':  # Строковые колонки
                    avg_length = df[col].astype(str).str.len().mean()
                    text_columns.append((col, avg_length))
            
            if text_columns:
                # Берем колонку с самым длинным текстом
                text_col = max(text_columns, key=lambda x: x[1])[0]
                dialogs = df[text_col].dropna().tolist()
            else:
                dialogs = df.iloc[:, 0].dropna().tolist()
    
    elif file_path.suffix == '.json':
        # Загрузка из JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            dialogs = data
        elif isinstance(data, dict) and 'dialogs' in data:
            dialogs = data['dialogs']
        else:
            raise ValueError("Неверный формат JSON файла")
    
    elif file_path.suffix == '.txt':
        # Загрузка из текстового файла
        with open(file_path, 'r', encoding='utf-8') as f:
            dialogs = [line.strip() for line in f if line.strip()]
    
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {file_path.suffix}")
    
    logger.info(f"Загружено {len(dialogs)} диалогов из {file_path}")
    return dialogs

def create_default_config() -> Dict[str, Any]:
    """Создание конфигурации по умолчанию"""
    return {
        'openai_api_key': 'your-api-key-here',
        'processing': {
            'enable_autocorrection': True,
            'enable_adaptive_prompts': True,
            'enable_continuous_learning': True,
            'enable_monitoring': True,
            'enable_scaling': True,
            'max_dialogs_per_batch': 1000,
            'quality_threshold': 0.7,
            'auto_save_results': True,
            'output_directory': 'enhanced_results'
        },
        'redis_host': 'localhost',
        'redis_port': 6379,
        'redis_db': 0
    }

def save_config(config: Dict[str, Any], config_path: str):
    """Сохранение конфигурации"""
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def load_config(config_path: str) -> Dict[str, Any]:
    """Загрузка конфигурации"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Enhanced Dialogs RAG System')
    parser.add_argument('--input', '-i', help='Путь к файлу с диалогами')
    parser.add_argument('--output', '-o', default='enhanced_results', help='Директория для результатов')
    parser.add_argument('--config', '-c', help='Путь к файлу конфигурации')
    parser.add_argument('--quality-threshold', type=float, default=0.7, help='Порог качества (0.0-1.0)')
    parser.add_argument('--max-batch-size', type=int, default=1000, help='Максимальный размер батча')
    parser.add_argument('--enable-all', action='store_true', help='Включить все улучшения')
    parser.add_argument('--disable-autocorrection', action='store_true', help='Отключить автокоррекцию')
    parser.add_argument('--disable-adaptive-prompts', action='store_true', help='Отключить адаптивные промпты')
    parser.add_argument('--disable-learning', action='store_true', help='Отключить непрерывное обучение')
    parser.add_argument('--disable-monitoring', action='store_true', help='Отключить мониторинг')
    parser.add_argument('--disable-scaling', action='store_true', help='Отключить масштабирование')
    parser.add_argument('--create-config', help='Создать файл конфигурации')
    parser.add_argument('--show-dashboard', action='store_true', help='Показать дашборд качества')
    parser.add_argument('--optimize-for', type=int, help='Оптимизировать для N диалогов')
    
    args = parser.parse_args()
    
    # Создание конфигурации
    if args.create_config:
        config = create_default_config()
        save_config(config, args.create_config)
        print(f"Конфигурация сохранена в {args.create_config}")
        return
    
    # Проверка обязательных параметров для обработки
    if not args.input:
        print("Ошибка: требуется указать --input для обработки диалогов")
        print("Используйте --help для получения справки")
        return
    
    # Загрузка конфигурации
    if args.config and Path(args.config).exists():
        config = load_config(args.config)
    else:
        config = create_default_config()
    
    # Обновление конфигурации из аргументов
    if args.enable_all:
        config['processing'].update({
            'enable_autocorrection': True,
            'enable_adaptive_prompts': True,
            'enable_continuous_learning': True,
            'enable_monitoring': True,
            'enable_scaling': True
        })
    
    if args.disable_autocorrection:
        config['processing']['enable_autocorrection'] = False
    
    if args.disable_adaptive_prompts:
        config['processing']['enable_adaptive_prompts'] = False
    
    if args.disable_learning:
        config['processing']['enable_continuous_learning'] = False
    
    if args.disable_monitoring:
        config['processing']['enable_monitoring'] = False
    
    if args.disable_scaling:
        config['processing']['enable_scaling'] = False
    
    config['processing']['output_directory'] = args.output
    config['processing']['quality_threshold'] = args.quality_threshold
    config['processing']['max_dialogs_per_batch'] = args.max_batch_size
    
    # Загрузка диалогов
    try:
        dialogs = load_dialogs_from_file(args.input)
    except Exception as e:
        logger.error(f"Ошибка загрузки диалогов: {e}")
        return
    
    if not dialogs:
        logger.error("Не найдено диалогов для обработки")
        return
    
    print(f"🚀 Запуск улучшенной системы анализа диалогов")
    print(f"📊 Диалогов для обработки: {len(dialogs)}")
    print(f"⚙️  Конфигурация: {config['processing']}")
    
    # Создание системы
    try:
        system = IntegratedQualitySystem(config)
    except Exception as e:
        logger.error(f"Ошибка инициализации системы: {e}")
        return
    
    # Оптимизация для большого объема
    if args.optimize_for:
        optimization = system.optimize_system_for_volume(args.optimize_for)
        print(f"🔧 Оптимизация для {args.optimize_for} диалогов:")
        for rec in optimization.get('recommendations', []):
            print(f"   • {rec}")
    
    # Обработка диалогов
    try:
        results = await system.process_dialogs_enhanced(dialogs)
        
        print(f"\n✅ Обработка завершена!")
        print(f"📈 Результатов: {len(results)}")
        
        # Статистика качества
        quality_scores = [r.get('quality_score', 0) for r in results if r.get('quality_score', 0) > 0]
        if quality_scores:
            avg_quality = sum(quality_scores) / len(quality_scores)
            print(f"🎯 Среднее качество: {avg_quality:.2f}")
            
            high_quality = sum(1 for q in quality_scores if q >= 0.8)
            print(f"⭐ Высокое качество (≥0.8): {high_quality}/{len(quality_scores)} ({high_quality/len(quality_scores)*100:.1f}%)")
        
        # Статистика по сущностям
        total_problems = sum(len(r.get('extracted_entities', {}).get('problems', [])) for r in results)
        total_ideas = sum(len(r.get('extracted_entities', {}).get('ideas', [])) for r in results)
        total_barriers = sum(len(r.get('extracted_entities', {}).get('barriers', [])) for r in results)
        total_quotes = sum(len(r.get('extracted_entities', {}).get('quotes', [])) for r in results)
        
        print(f"\n📊 Извлеченные сущности:")
        print(f"   • Проблемы: {total_problems}")
        print(f"   • Идеи: {total_ideas}")
        print(f"   • Барьеры: {total_barriers}")
        print(f"   • Цитаты: {total_quotes}")
        
        # Показываем дашборд
        if args.show_dashboard:
            print(f"\n🌐 Открываем дашборд качества...")
            system.monitor.open_dashboard(f"{args.output}/quality_dashboard.html")
        
        # Сохраняем конфигурацию
        config_path = Path(args.output) / "config.json"
        save_config(config, str(config_path))
        
        print(f"\n💾 Результаты сохранены в: {args.output}")
        print(f"📋 Конфигурация сохранена в: {config_path}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки: {e}")
        return

def show_help():
    """Показать справку по использованию"""
    help_text = """
🎯 Enhanced Dialogs RAG System - Система улучшенного анализа диалогов

ОСНОВНОЕ ИСПОЛЬЗОВАНИЕ:
    python enhanced_main.py --input dialogs.xlsx --output results

ПАРАМЕТРЫ:
    --input, -i          Путь к файлу с диалогами (xlsx, json, txt)
    --output, -o         Директория для результатов (по умолчанию: enhanced_results)
    --config, -c         Путь к файлу конфигурации
    --quality-threshold  Порог качества (0.0-1.0, по умолчанию: 0.7)
    --max-batch-size     Максимальный размер батча (по умолчанию: 1000)

УПРАВЛЕНИЕ КОМПОНЕНТАМИ:
    --enable-all                    Включить все улучшения
    --disable-autocorrection        Отключить автокоррекцию качества
    --disable-adaptive-prompts      Отключить адаптивные промпты
    --disable-learning              Отключить непрерывное обучение
    --disable-monitoring            Отключить мониторинг
    --disable-scaling               Отключить масштабирование

ДОПОЛНИТЕЛЬНЫЕ ОПЦИИ:
    --create-config FILE            Создать файл конфигурации
    --show-dashboard                Показать дашборд качества
    --optimize-for N                Оптимизировать для N диалогов

ПРИМЕРЫ:
    # Базовая обработка
    python enhanced_main.py --input dialogs.xlsx --output results

    # Обработка с включением всех улучшений
    python enhanced_main.py --input dialogs.xlsx --enable-all --show-dashboard

    # Обработка большого объема (10,000 диалогов)
    python enhanced_main.py --input large_dialogs.xlsx --optimize-for 10000 --max-batch-size 2000

    # Создание конфигурации
    python enhanced_main.py --create-config my_config.json

    # Обработка с кастомной конфигурацией
    python enhanced_main.py --input dialogs.xlsx --config my_config.json

ФОРМАТЫ ВХОДНЫХ ФАЙЛОВ:
    • Excel (.xlsx) - диалоги в колонке 'dialog' или первой колонке
    • JSON (.json) - массив диалогов или объект с ключом 'dialogs'
    • Текст (.txt) - один диалог на строку

РЕЗУЛЬТАТЫ:
    • enhanced_results.json - основные результаты
    • processing_statistics.json - статистика обработки
    • quality_dashboard.html - интерактивный дашборд
    • processing_report.md - текстовый отчет
    • config.json - использованная конфигурация

КОМПОНЕНТЫ СИСТЕМЫ:
    🔧 Автокоррекция качества - автоматическое исправление ошибок
    🎯 Адаптивные промпты - A/B тестирование и оптимизация
    🧠 Непрерывное обучение - улучшение на основе данных
    📊 Мониторинг качества - отслеживание в реальном времени
    ⚡ Масштабирование - обработка больших объемов

Для получения помощи: python enhanced_main.py --help
    """
    print(help_text)

if __name__ == "__main__":
    if len(sys.argv) == 1 or '--help' in sys.argv or '-h' in sys.argv:
        show_help()
    else:
        asyncio.run(main())
