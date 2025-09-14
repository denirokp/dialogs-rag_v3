#!/usr/bin/env python3
"""
Enhanced Pipeline Manager
Управляет улучшенным pipeline с новыми этапами анализа
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
import json
import pandas as pd

# Добавляем путь к модулям
sys.path.append(str(Path(__file__).parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация этапов
ENHANCED_STAGES = {
    "1": {
        "name": "Детекция доставки",
        "script": "pipeline/stage1_detect_delivery.py",
        "description": "Определение диалогов, связанных с доставкой"
    },
    "1.5": {
        "name": "Фильтрация выборки",
        "script": "pipeline/stage1_5_sample_filter.py", 
        "description": "Фильтрация и валидация выборки диалогов"
    },
    "2": {
        "name": "Извлечение сущностей (базовое)",
        "script": "pipeline/stage2_extract_entities.py",
        "description": "Базовое извлечение барьеров, идей и сигналов"
    },
    "2_enhanced": {
        "name": "Извлечение сущностей (улучшенное)",
        "script": "pipeline/stage2_extract_entities_enhanced.py",
        "description": "Расширенное извлечение с контекстным анализом"
    },
    "2.5": {
        "name": "Контекстный анализ",
        "script": "pipeline/stage2_5_contextual_analysis.py",
        "description": "Анализ контекста, паттернов и эмоциональной динамики"
    },
    "3": {
        "name": "Нормализация",
        "script": "pipeline/stage3_normalize.py",
        "description": "Нормализация формулировок и категоризация"
    },
    "4": {
        "name": "Кластеризация (базовая)",
        "script": "pipeline/stage4_cluster.py",
        "description": "Базовая кластеризация похожих формулировок"
    },
    "4_enhanced": {
        "name": "Кластеризация (улучшенная)",
        "script": "pipeline/stage4_cluster_enhanced.py",
        "description": "Семантическая кластеризация с embeddings"
    },
    "4.5": {
        "name": "Семантическое обогащение",
        "script": "pipeline/stage4_5_semantic_enrichment.py",
        "description": "Обогащение кластеров дополнительной информацией"
    },
    "5": {
        "name": "Агрегация",
        "script": "pipeline/stage5_aggregate.py",
        "description": "Агрегация результатов кластеризации"
    },
    "6": {
        "name": "Генерация отчетов",
        "script": "pipeline/stage6_report.py",
        "description": "Создание Markdown и Excel отчетов"
    },
    "7": {
        "name": "Метрики качества (базовые)",
        "script": "pipeline/stage7_quality.py",
        "description": "Вычисление базовых метрик качества"
    },
    "7_enhanced": {
        "name": "Метрики качества (расширенные)",
        "script": "pipeline/stage7_quality_enhanced.py",
        "description": "Расширенные метрики с семантическими показателями"
    },
    "dashboard": {
        "name": "Интерактивный дашборд",
        "script": "dashboard/interactive_dashboard.py",
        "description": "Создание интерактивного HTML дашборда"
    },
    "ab_test": {
        "name": "A/B тестирование промптов",
        "script": "pipeline/ab_testing_prompts.py",
        "description": "Сравнение эффективности разных промптов"
    }
}

def run_stage(stage_id: str, skip_failed: bool = False) -> bool:
    """Запуск конкретного этапа"""
    
    if stage_id not in ENHANCED_STAGES:
        logger.error(f"❌ Неизвестный этап: {stage_id}")
        return False
    
    stage_info = ENHANCED_STAGES[stage_id]
    script_path = Path(stage_info["script"])
    
    if not script_path.exists():
        logger.error(f"❌ Скрипт этапа не найден: {script_path}")
        return False
    
    logger.info(f"🚀 Запуск этапа {stage_id}: {stage_info['name']}")
    logger.info(f"📝 {stage_info['description']}")
    
    try:
        # Импортируем и запускаем модуль
        import importlib.util
        spec = importlib.util.spec_from_file_location(f"stage_{stage_id}", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Запускаем main функцию
        if hasattr(module, 'main'):
            module.main()
            logger.info(f"✅ Этап {stage_id} завершен успешно")
            return True
        else:
            logger.error(f"❌ В скрипте {script_path} не найдена функция main()")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка в этапе {stage_id}: {e}")
        if not skip_failed:
            raise
        return False

def run_pipeline(stages: List[str], skip_failed: bool = False) -> Dict[str, Any]:
    """Запуск pipeline с указанными этапами"""
    
    logger.info(f"🚀 Запуск Enhanced Pipeline с этапами: {', '.join(stages)}")
    
    results = {
        "started_at": pd.Timestamp.now().isoformat(),
        "stages": {},
        "success": True,
        "errors": []
    }
    
    for stage_id in stages:
        if stage_id not in ENHANCED_STAGES:
            error_msg = f"Неизвестный этап: {stage_id}"
            logger.error(f"❌ {error_msg}")
            results["errors"].append(error_msg)
            if not skip_failed:
                results["success"] = False
                break
            continue
        
        stage_start = pd.Timestamp.now()
        success = run_stage(stage_id, skip_failed)
        stage_end = pd.Timestamp.now()
        
        results["stages"][stage_id] = {
            "name": ENHANCED_STAGES[stage_id]["name"],
            "success": success,
            "started_at": stage_start.isoformat(),
            "completed_at": stage_end.isoformat(),
            "duration_seconds": (stage_end - stage_start).total_seconds()
        }
        
        if not success and not skip_failed:
            results["success"] = False
            break
    
    results["completed_at"] = pd.Timestamp.now().isoformat()
    
    # Сохраняем результаты
    results_file = "reports/pipeline_results.json"
    Path("reports").mkdir(exist_ok=True, parents=True)
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"📊 Результаты pipeline сохранены: {results_file}")
    
    return results

def get_enhanced_pipeline_presets() -> Dict[str, List[str]]:
    """Получение предустановленных конфигураций pipeline"""
    
    return {
        "basic": ["1", "2", "3", "4", "5", "6", "7"],
        "enhanced": ["1", "2_enhanced", "2.5", "3", "4_enhanced", "4.5", "5", "6", "7_enhanced", "dashboard"],
        "full": ["1", "1.5", "2_enhanced", "2.5", "3", "4_enhanced", "4.5", "5", "6", "7_enhanced", "dashboard", "ab_test"],
        "quality": ["2_enhanced", "4_enhanced", "4.5", "7_enhanced", "dashboard"],
        "ab_testing": ["ab_test"],
        "dashboard_only": ["dashboard"]
    }

def main():
    """Основная функция"""
    
    parser = argparse.ArgumentParser(description="Enhanced Pipeline Manager для анализа диалогов")
    
    # Основные опции
    parser.add_argument("--stages", nargs="+", help="Список этапов для запуска")
    parser.add_argument("--preset", choices=list(get_enhanced_pipeline_presets().keys()), 
                       help="Предустановленная конфигурация")
    parser.add_argument("--from", dest="from_stage", help="Запуск с указанного этапа")
    parser.add_argument("--to", help="Запуск до указанного этапа")
    parser.add_argument("--skip-failed", action="store_true", help="Продолжать выполнение при ошибках")
    parser.add_argument("--list-stages", action="store_true", help="Показать список доступных этапов")
    parser.add_argument("--list-presets", action="store_true", help="Показать предустановленные конфигурации")
    
    args = parser.parse_args()
    
    # Показываем список этапов
    if args.list_stages:
        print("\n📋 Доступные этапы:")
        for stage_id, info in ENHANCED_STAGES.items():
            print(f"  {stage_id:8} - {info['name']}")
            print(f"           {info['description']}")
        return
    
    # Показываем предустановленные конфигурации
    if args.list_presets:
        print("\n🎯 Предустановленные конфигурации:")
        presets = get_enhanced_pipeline_presets()
        for preset_name, stages in presets.items():
            print(f"  {preset_name:12} - {', '.join(stages)}")
        return
    
    # Определяем этапы для запуска
    stages_to_run = []
    
    if args.preset:
        presets = get_enhanced_pipeline_presets()
        stages_to_run = presets[args.preset]
        logger.info(f"🎯 Используется предустановка: {args.preset}")
    elif args.stages:
        stages_to_run = args.stages
    elif args.from_stage or args.to:
        all_stage_ids = list(ENHANCED_STAGES.keys())
        start_idx = all_stage_ids.index(args.from_stage) if args.from_stage else 0
        end_idx = all_stage_ids.index(args.to) + 1 if args.to else len(all_stage_ids)
        stages_to_run = all_stage_ids[start_idx:end_idx]
    else:
        # По умолчанию запускаем enhanced pipeline
        stages_to_run = get_enhanced_pipeline_presets()["enhanced"]
        logger.info("🎯 Используется конфигурация по умолчанию: enhanced")
    
    # Запускаем pipeline
    try:
        results = run_pipeline(stages_to_run, args.skip_failed)
        
        if results["success"]:
            logger.info("🎉 Pipeline завершен успешно!")
        else:
            logger.error("❌ Pipeline завершен с ошибками")
            if results["errors"]:
                logger.error("Ошибки:")
                for error in results["errors"]:
                    logger.error(f"  - {error}")
    except KeyboardInterrupt:
        logger.info("⏹️ Pipeline прерван пользователем")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
