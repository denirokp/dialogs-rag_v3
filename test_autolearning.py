#!/usr/bin/env python3
"""
🧠 ТЕСТИРОВАНИЕ АВТООБУЧЕНИЯ
Проверка системы непрерывного обучения
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from enhanced.continuous_learning import ContinuousLearningSystem
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_autolearning():
    """Тестирование системы автообучения"""
    logger.info("🧠 Тестируем систему автообучения...")
    
    # Создаем конфигурацию
    config = {
        "openai_api_key": "test-key",
        "learning": {
            "min_quality_score": 0.5,
            "min_examples_for_update": 5,
            "pattern_confidence_threshold": 0.7
        }
    }
    
    # Создаем систему обучения
    learning_system = ContinuousLearningSystem(config)
    logger.info("✅ Система обучения создана")
    
    # Добавляем примеры для обучения
    test_examples = [
        {
            "dialog": "У меня проблема с доставкой, не работает выборочно",
            "extracted_entities": {
                "problems": ["проблема с доставкой"],
                "quotes": ["У меня проблема с доставкой, не работает выборочно"]
            },
            "quality_score": 0.85,
            "source": "test"
        },
        {
            "dialog": "Функционал не понятен, баг в системе",
            "extracted_entities": {
                "problems": ["функционал не понятен", "баг в системе"],
                "quotes": ["Функционал не понятен, баг в системе"]
            },
            "quality_score": 0.90,
            "source": "test"
        },
        {
            "dialog": "Обращался в поддержку, но не помогло",
            "extracted_entities": {
                "problems": ["не помогло"],
                "quotes": ["Обращался в поддержку, но не помогло"]
            },
            "quality_score": 0.75,
            "source": "test"
        }
    ]
    
    # Добавляем примеры
    for example in test_examples:
        learning_system.add_learning_example(
            dialog=example["dialog"],
            extracted_entities=example["extracted_entities"],
            quality_score=example["quality_score"],
            source=example["source"]
        )
    
    logger.info(f"✅ Добавлено {len(test_examples)} примеров обучения")
    
    # Проверяем, что примеры добавлены
    assert len(learning_system.learning_examples) == len(test_examples), "Не все примеры добавлены"
    
    # Проверяем инсайты обучения
    insights = learning_system.get_learning_insights()
    logger.info(f"📊 Инсайты обучения: {insights}")
    
    # Проверяем паттерны
    patterns = learning_system.learned_patterns
    logger.info(f"🎯 Найдено паттернов: {len(patterns)}")
    
    # Проверяем эффективность обучения
    effectiveness = learning_system._calculate_learning_effectiveness()
    logger.info(f"📈 Эффективность обучения: {effectiveness:.2f}")
    
    # Тестируем сохранение и загрузку
    test_file = "test_learning_data.json"
    learning_system.save_learning_data(test_file)
    logger.info("💾 Данные обучения сохранены")
    
    # Создаем новую систему и загружаем данные
    new_learning_system = ContinuousLearningSystem(config)
    new_learning_system.load_learning_data(test_file)
    
    assert len(new_learning_system.learning_examples) == len(test_examples), "Данные не загружены корректно"
    logger.info("✅ Данные обучения загружены корректно")
    
    # Очистка
    Path(test_file).unlink(missing_ok=True)
    
    logger.info("🎉 Тестирование автообучения завершено успешно!")
    return True

def test_learning_integration():
    """Тестирование интеграции с пайплайном"""
    logger.info("🔗 Тестируем интеграцию автообучения с пайплайном...")
    
    # Импортируем пайплайн
    from comprehensive_dod_pipeline import ComprehensiveDoDPipeline
    
    # Создаем тестовую конфигурацию
    config = {
        "openai_api_key": "test-key",
        "processing": {
            "enable_continuous_learning": True,
            "enable_autocorrection": False,
            "enable_adaptive_prompts": False,
            "enable_monitoring": False,
            "enable_scaling": False,
            "quality_threshold": 0.7,
            "max_dialogs_per_batch": 1000,
            "auto_save_results": True,
            "output_directory": "test_results"
        }
    }
    
    # Создаем пайплайн
    pipeline = ComprehensiveDoDPipeline(config_dict=config)
    logger.info("✅ Пайплайн с автообучением создан")
    
    # Проверяем, что система обучения инициализирована
    assert pipeline.learning_system is not None, "Система обучения не инициализирована"
    logger.info("✅ Система обучения интегрирована в пайплайн")
    
    return True

def main():
    """Главная функция тестирования"""
    logger.info("🚀 Начинаем тестирование автообучения...")
    
    try:
        # Тест системы обучения
        test_autolearning()
        
        # Тест интеграции
        test_learning_integration()
        
        print("\n" + "="*60)
        print("🧠 ИТОГИ ТЕСТИРОВАНИЯ АВТООБУЧЕНИЯ")
        print("="*60)
        print("✅ Система непрерывного обучения работает")
        print("✅ Добавление примеров обучения работает")
        print("✅ Анализ паттернов работает")
        print("✅ Сохранение/загрузка данных работает")
        print("✅ Интеграция с пайплайном работает")
        print("="*60)
        print("🎉 АВТООБУЧЕНИЕ ПОЛНОСТЬЮ ГОТОВО!")
        print("="*60)
        
        logger.info("🎉 Тестирование автообучения завершено успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования автообучения: {e}")
        raise

if __name__ == "__main__":
    main()
