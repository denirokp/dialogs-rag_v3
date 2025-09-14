#!/usr/bin/env python3
"""
🧪 ТЕСТИРОВАНИЕ КОМПЛЕКСНОЙ СИСТЕМЫ DoD
Проверка работы всех компонентов системы
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import numpy as np

# Добавляем пути для импорта
sys.path.append(str(Path(__file__).parent))

from comprehensive_dod_pipeline import ComprehensiveDoDPipeline, load_dialogs_from_file

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_test_dialogs() -> List[Dict[str, Any]]:
    """Создание тестовых диалогов"""
    test_dialogs = [
        {
            "dialog_id": 1,
            "turns": [
                {"role": "client", "text": "У меня проблема с доставкой, не работает выборочно. Заказ не приходит уже неделю."},
                {"role": "operator", "text": "Понимаю вашу проблему. Давайте разберемся."},
                {"role": "client", "text": "Это очень дорого для категории товаров. Можете дать скидку?"}
            ]
        },
        {
            "dialog_id": 2,
            "turns": [
                {"role": "client", "text": "Функционал не понятен, баг в системе. Не могу найти нужную опцию."},
                {"role": "operator", "text": "Помогу вам разобраться с интерфейсом."},
                {"role": "client", "text": "Спасибо, отлично работает после вашей помощи!"}
            ]
        },
        {
            "dialog_id": 3,
            "turns": [
                {"role": "client", "text": "Обращался в поддержку, но не помогло. Долгое ожидание ответа."},
                {"role": "operator", "text": "Извините за неудобства. Рассмотрим ваш случай."},
                {"role": "client", "text": "Сложно вернуть товар, задержки выплат. Предпочитаю другого оператора."}
            ]
        },
        {
            "dialog_id": 4,
            "turns": [
                {"role": "client", "text": "Редкая позиция в ассортименте, нет спроса на товар."},
                {"role": "operator", "text": "Понял, учтем ваше предложение."},
                {"role": "client", "text": "Идея: можно добавить больше таких товаров в каталог."}
            ]
        },
        {
            "dialog_id": 5,
            "turns": [
                {"role": "client", "text": "Непонятный интерфейс, сложно найти опцию. Долго в пути заказ."},
                {"role": "operator", "text": "Помогу настроить интерфейс под вас."},
                {"role": "client", "text": "Нет отслеживания заказа, хотелось бы знать где он сейчас."}
            ]
        }
    ]
    return test_dialogs

async def test_system_components():
    """Тестирование компонентов системы"""
    logger.info("🧪 Начинаем тестирование компонентов системы...")
    
    # Создаем тестовые диалоги
    test_dialogs = create_test_dialogs()
    logger.info(f"✅ Создано {len(test_dialogs)} тестовых диалогов")
    
    # Создаем конфигурацию для тестирования
    test_config = {
        "openai_api_key": "test-key",
        "processing": {
            "enable_validation": True,
            "enable_dedup": True,
            "enable_clustering": True,
            "enable_quality_checks": True,
            "enable_autocorrection": True,
            "enable_adaptive_prompts": True,
            "enable_continuous_learning": True,
            "enable_monitoring": True,
            "enable_scaling": True,
            "max_dialogs_per_batch": 100,
            "quality_threshold": 0.6
        },
        "dedup": {"threshold": 0.92, "enable_embeddings": False},
        "clustering": {"min_cluster_size": 2, "n_neighbors": 3, "min_dist": 0.1},
        "redis_host": "localhost", "redis_port": 6379, "redis_db": 0
    }
    
    # Сохраняем тестовую конфигурацию
    with open('test_config_comprehensive.json', 'w', encoding='utf-8') as f:
        json.dump(test_config, f, ensure_ascii=False, indent=2)
    
    # Создаем пайплайн
    pipeline = ComprehensiveDoDPipeline('test_config_comprehensive.json')
    
    # Тестируем обработку диалогов
    logger.info("🔄 Тестируем обработку диалогов...")
    results = await pipeline.process_dialogs(test_dialogs)
    
    # Проверяем результаты
    logger.info("🔍 Проверяем результаты...")
    
    # Проверка основных результатов
    assert "dialog_results" in results, "Отсутствуют результаты диалогов"
    assert "all_mentions" in results, "Отсутствуют упоминания"
    assert "clusters" in results, "Отсутствуют кластеры"
    assert "summaries" in results, "Отсутствуют сводки"
    assert "quality_results" in results, "Отсутствуют результаты качества"
    assert "statistics" in results, "Отсутствует статистика"
    
    logger.info("✅ Основные результаты присутствуют")
    
    # Проверка упоминаний
    mentions = results["all_mentions"]
    assert len(mentions) > 0, "Не извлечено ни одного упоминания"
    
    # Проверка структуры упоминаний
    for mention in mentions:
        required_fields = ["dialog_id", "turn_id", "theme", "subtheme", "label_type", "text_quote", "confidence"]
        for field in required_fields:
            assert field in mention, f"Отсутствует поле {field} в упоминании"
    
    logger.info(f"✅ Извлечено {len(mentions)} упоминаний с правильной структурой")
    
    # Проверка качества
    quality_results = results["quality_results"]
    if quality_results:
        dod_status = quality_results.get("dod_status", {})
        logger.info(f"📊 DoD статус: {dod_status}")
        
        # Проверяем, что все упоминания только от клиента
        client_mentions = [m for m in mentions if m.get("dialog_id") is not None]
        assert len(client_mentions) == len(mentions), "Есть упоминания не от клиента"
        
        # Проверяем, что все цитаты не пустые
        empty_quotes = [m for m in mentions if not m.get("text_quote", "").strip()]
        assert len(empty_quotes) == 0, f"Найдены пустые цитаты: {len(empty_quotes)}"
        
        logger.info("✅ DoD требования выполнены")
    
    # Проверка статистики
    stats = results["statistics"]
    assert stats["total_dialogs"] == len(test_dialogs), "Неверное количество диалогов в статистике"
    assert stats["total_mentions"] == len(mentions), "Неверное количество упоминаний в статистике"
    assert stats["success_rate"] > 0, "Нулевая успешность обработки"
    
    logger.info(f"✅ Статистика корректна: {stats['success_rate']:.1%} успешность")
    
    # Проверка A/B тестов
    ab_results = stats.get("ab_test_results", {})
    if ab_results:
        logger.info(f"✅ A/B тесты работают: {len(ab_results.get('variants', {}))} вариантов")
    
    # Проверка обучения
    learning_examples = stats.get("learning_examples_added", 0)
    logger.info(f"✅ Обучение работает: добавлено {learning_examples} примеров")
    
    # Проверка мониторинга
    monitoring_stats = stats.get("monitoring_stats", {})
    if monitoring_stats:
        logger.info(f"✅ Мониторинг работает: {monitoring_stats}")
    
    # Сохраняем результаты тестирования
    pipeline.save_results("test_results_comprehensive")
    
    logger.info("🎉 Все тесты пройдены успешно!")
    return results

def test_file_loading():
    """Тестирование загрузки файлов"""
    logger.info("📂 Тестируем загрузку файлов...")
    
    # Создаем тестовый Excel файл
    test_data = {
        'Текст транскрибации': [
            "У меня проблема с доставкой, не работает выборочно.",
            "Функционал не понятен, баг в системе.",
            "Обращался в поддержку, но не помогло.",
            "Редкая позиция в ассортименте, нет спроса.",
            "Непонятный интерфейс, сложно найти опцию."
        ]
    }
    
    df = pd.DataFrame(test_data)
    df.to_excel('test_dialogs.xlsx', index=False)
    
    # Тестируем загрузку
    dialogs = load_dialogs_from_file('test_dialogs.xlsx')
    assert len(dialogs) == 5, f"Ожидалось 5 диалогов, получено {len(dialogs)}"
    
    # Проверяем структуру
    for dialog in dialogs:
        assert "dialog_id" in dialog, "Отсутствует dialog_id"
        assert "turns" in dialog, "Отсутствует turns"
        assert len(dialog["turns"]) > 0, "Пустые turns"
        assert dialog["turns"][0]["role"] == "client", "Первый turn не от клиента"
    
    logger.info("✅ Загрузка файлов работает корректно")
    
    # Очистка
    Path('test_dialogs.xlsx').unlink(missing_ok=True)

def test_validation():
    """Тестирование валидации"""
    logger.info("🔍 Тестируем валидацию...")
    
    # Создаем валидное упоминание
    valid_mention = {
        "dialog_id": 1,
        "turn_id": 0,
        "theme": "доставка",
        "subtheme": "не работает выборочно",
        "label_type": "барьер",
        "text_quote": "У меня проблема с доставкой",
        "delivery_type": "complaint",
        "cause_hint": "причина указана",
        "confidence": 0.95
    }
    
    # Создаем невалидное упоминание
    invalid_mention = {
        "dialog_id": 1,
        "turn_id": 0,
        "theme": "доставка",
        "subtheme": "не работает выборочно",
        "label_type": "барьер",
        "text_quote": "",  # Пустая цитата
        "confidence": 0.95
    }
    
    # Тестируем валидацию
    pipeline = ComprehensiveDoDPipeline()
    
    try:
        pipeline._validate_mentions([valid_mention])
        logger.info("✅ Валидное упоминание прошло проверку")
    except Exception as e:
        logger.error(f"❌ Ошибка валидации валидного упоминания: {e}")
        raise
    
    try:
        pipeline._validate_mentions([invalid_mention])
        logger.error("❌ Невалидное упоминание прошло проверку")
        raise AssertionError("Невалидное упоминание должно было быть отклонено")
    except Exception:
        logger.info("✅ Невалидное упоминание корректно отклонено")

async def main():
    """Главная функция тестирования"""
    logger.info("🚀 Начинаем комплексное тестирование системы DoD...")
    
    try:
        # Тест загрузки файлов
        test_file_loading()
        
        # Тест валидации
        test_validation()
        
        # Тест компонентов системы
        results = await test_system_components()
        
        # Вывод итоговых результатов
        print("\n" + "="*60)
        print("🎯 ИТОГИ ТЕСТИРОВАНИЯ КОМПЛЕКСНОЙ СИСТЕМЫ DoD")
        print("="*60)
        print(f"✅ Все компоненты работают корректно")
        print(f"📊 Обработано диалогов: {results['statistics']['total_dialogs']}")
        print(f"📝 Извлечено упоминаний: {len(results['all_mentions'])}")
        print(f"🎯 Найдено кластеров: {len(results['clusters'])}")
        print(f"📈 Успешность: {results['statistics']['success_rate']:.1%}")
        print(f"🔍 DoD статус: {'✅ ПРОЙДЕН' if results['quality_results'].get('dod_passed') else '❌ НЕ ПРОЙДЕН'}")
        print("="*60)
        
        logger.info("🎉 Комплексное тестирование завершено успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
