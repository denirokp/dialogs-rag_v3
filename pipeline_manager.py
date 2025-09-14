#!/usr/bin/env python3
"""
Pipeline Manager - Центральный менеджер для управления всеми этапами анализа
"""

import sys
import json
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum

# Добавляем папку pipeline в путь
pipeline_path = str(Path(__file__).parent / "pipeline")
if pipeline_path not in sys.path:
    sys.path.append(pipeline_path)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Импорты модулей pipeline
try:
    from stage1_detect_delivery import main as stage1_main
    from stage1_5_sample_filter import main as stage1_5_main
    from stage2_extract_entities import main as stage2_main
    from stage3_normalize import main as stage3_main
    from stage4_cluster import main as stage4_main
    from stage5_aggregate import main as stage5_main
    from stage6_report import main as stage6_main
    from stage7_quality import main as stage7_main
except ImportError as e:
    logger.error(f"❌ Ошибка импорта модулей pipeline: {e}")
    logger.error("💡 Убедитесь, что все файлы pipeline находятся в папке pipeline/")
    sys.exit(1)

class StageStatus(Enum):
    """Статусы этапов"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class StageResult:
    """Результат выполнения этапа"""
    stage_id: str
    stage_name: str
    status: StageStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    output_files: List[str] = None
    metrics: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []
        if self.metrics is None:
            self.metrics = {}

@dataclass
class PipelineConfig:
    """Конфигурация pipeline"""
    input_file: str = "data/dialogs.xlsx"
    output_dir: str = "artifacts"
    reports_dir: str = "reports"
    logs_dir: str = "logs"
    batch_size: int = 100
    max_retries: int = 3
    parallel_execution: bool = False
    skip_failed_stages: bool = False
    cleanup_intermediate: bool = False

class PipelineManager:
    """Центральный менеджер pipeline"""
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.stages = self._initialize_stages()
        self.results: Dict[str, StageResult] = {}
        self.pipeline_start_time: Optional[datetime] = None
        self.pipeline_end_time: Optional[datetime] = None
        
        # Создаем необходимые директории
        self._create_directories()
    
    def _initialize_stages(self) -> Dict[str, Dict[str, Any]]:
        """Инициализация этапов pipeline"""
        return {
            "1": {
                "name": "Детекция доставки",
                "function": stage1_main,
                "dependencies": [],
                "output_files": ["stage1_delivery.jsonl"],
                "description": "Определение диалогов, где обсуждается доставка"
            },
            "1.5": {
                "name": "Фильтрация образцов",
                "function": stage1_5_main,
                "dependencies": ["1"],
                "output_files": ["stage1_5_sampling.jsonl"],
                "description": "Детальная проверка валидности диалогов с обоснованным скорингом"
            },
            "2": {
                "name": "Извлечение сущностей", 
                "function": stage2_main,
                "dependencies": ["1"],
                "output_files": ["stage2_extracted.jsonl"],
                "description": "Извлечение барьеров, идей и сигналов из диалогов"
            },
            "3": {
                "name": "Нормализация формулировок",
                "function": stage3_main,
                "dependencies": ["2"],
                "output_files": ["stage3_normalized.jsonl"],
                "description": "Приведение формулировок к единому виду"
            },
            "4": {
                "name": "Кластеризация",
                "function": stage4_main,
                "dependencies": ["3"],
                "output_files": ["stage4_clusters.json"],
                "description": "Группировка похожих формулировок в кластеры"
            },
            "5": {
                "name": "Агрегация метрик",
                "function": stage5_main,
                "dependencies": ["4"],
                "output_files": ["aggregate_results.json", "barriers.csv", "ideas.csv", "signals.csv"],
                "description": "Расчет метрик и статистики по кластерам"
            },
            "6": {
                "name": "Генерация отчетов",
                "function": stage6_main,
                "dependencies": ["5"],
                "output_files": ["report.md", "report.xlsx", "appendix_ids.md"],
                "description": "Создание финальных отчетов и визуализаций"
            },
            "7": {
                "name": "Метрики качества",
                "function": stage7_main,
                "dependencies": ["6"],
                "output_files": ["quality.json"],
                "description": "Вычисление метрик качества анализа"
            }
        }
    
    def _create_directories(self):
        """Создание необходимых директорий"""
        directories = [
            self.config.output_dir,
            self.config.reports_dir,
            self.config.logs_dir
        ]
        
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
            logger.info(f"📁 Создана папка: {directory}")
    
    def get_stage_status(self, stage_id: str) -> StageStatus:
        """Получение статуса этапа"""
        if stage_id not in self.results:
            return StageStatus.PENDING
        return self.results[stage_id].status
    
    def can_run_stage(self, stage_id: str) -> bool:
        """Проверка возможности запуска этапа"""
        if stage_id not in self.stages:
            return False
        
        # Проверяем зависимости
        for dep in self.stages[stage_id]["dependencies"]:
            if self.get_stage_status(dep) != StageStatus.COMPLETED:
                return False
        
        return True
    
    def run_stage(self, stage_id: str) -> StageResult:
        """Запуск конкретного этапа"""
        if stage_id not in self.stages:
            raise ValueError(f"Неизвестный этап: {stage_id}")
        
        stage_info = self.stages[stage_id]
        stage_name = stage_info["name"]
        
        logger.info(f"🚀 Запуск этапа {stage_id}: {stage_name}")
        
        # Создаем результат
        result = StageResult(
            stage_id=stage_id,
            stage_name=stage_name,
            status=StageStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self.results[stage_id] = result
        
        try:
            # Проверяем зависимости
            if not self.can_run_stage(stage_id):
                missing_deps = [
                    dep for dep in stage_info["dependencies"]
                    if self.get_stage_status(dep) != StageStatus.COMPLETED
                ]
                raise ValueError(f"Не выполнены зависимости: {missing_deps}")
            
            # Запускаем этап
            stage_info["function"]()
            
            # Обновляем результат
            result.status = StageStatus.COMPLETED
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            
            # Проверяем выходные файлы
            self._check_output_files(stage_id, result)
            
            logger.info(f"✅ Этап {stage_id} завершен успешно за {result.duration_seconds:.1f}с")
            
        except Exception as e:
            result.status = StageStatus.FAILED
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            result.error_message = str(e)
            
            logger.error(f"❌ Ошибка в этапе {stage_id}: {e}")
            
            if not self.config.skip_failed_stages:
                raise
        
        return result
    
    def _check_output_files(self, stage_id: str, result: StageResult):
        """Проверка выходных файлов этапа"""
        expected_files = self.stages[stage_id]["output_files"]
        
        for filename in expected_files:
            file_path = Path(self.config.output_dir) / filename
            if file_path.exists():
                result.output_files.append(str(file_path))
            else:
                logger.warning(f"⚠️ Ожидаемый файл не найден: {file_path}")
    
    def run_pipeline(self, stage_ids: List[str] = None) -> Dict[str, StageResult]:
        """Запуск pipeline"""
        if stage_ids is None:
            stage_ids = list(self.stages.keys())
        
        logger.info("🔍 Dialogs RAG - Pipeline Manager")
        logger.info("=" * 60)
        
        self.pipeline_start_time = datetime.now()
        
        # Проверяем зависимости
        if not self._check_dependencies():
            raise RuntimeError("Не все зависимости установлены")
        
        # Запускаем этапы
        for stage_id in stage_ids:
            try:
                self.run_stage(stage_id)
            except Exception as e:
                logger.error(f"❌ Pipeline остановлен на этапе {stage_id}: {e}")
                if not self.config.skip_failed_stages:
                    break
        
        self.pipeline_end_time = datetime.now()
        
        # Итоговая статистика
        self._log_pipeline_summary()
        
        return self.results
    
    def _check_dependencies(self) -> bool:
        """Проверка зависимостей"""
        logger.info("🔍 Проверка зависимостей...")
        
        try:
            import pandas
            import openpyxl
            import tqdm
            import tenacity
            import sklearn
            import numpy
            import openai
            import chromadb
            import sentence_transformers
            logger.info("✅ Все зависимости установлены")
            return True
        except ImportError as e:
            logger.error(f"❌ Отсутствует зависимость: {e}")
            logger.error("💡 Установите зависимости: pip install -r requirements.txt")
            return False
    
    def _log_pipeline_summary(self):
        """Логирование итоговой статистики"""
        total_stages = len(self.results)
        completed_stages = sum(1 for r in self.results.values() if r.status == StageStatus.COMPLETED)
        failed_stages = sum(1 for r in self.results.values() if r.status == StageStatus.FAILED)
        
        total_duration = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"📊 Pipeline завершен: {completed_stages}/{total_stages} этапов успешно")
        logger.info(f"⏱️ Общее время выполнения: {total_duration:.1f} сек")
        
        if failed_stages > 0:
            logger.error(f"❌ Ошибок: {failed_stages}")
            for stage_id, result in self.results.items():
                if result.status == StageStatus.FAILED:
                    logger.error(f"  - Этап {stage_id}: {result.error_message}")
        else:
            logger.info("🎉 Все этапы выполнены успешно!")
            logger.info("📁 Результаты сохранены в папках artifacts/ и reports/")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Получение статуса pipeline"""
        return {
            "pipeline_start_time": self.pipeline_start_time.isoformat() if self.pipeline_start_time else None,
            "pipeline_end_time": self.pipeline_end_time.isoformat() if self.pipeline_end_time else None,
            "total_duration_seconds": (self.pipeline_end_time - self.pipeline_start_time).total_seconds() if self.pipeline_start_time and self.pipeline_end_time else None,
            "stages": {
                stage_id: {
                    "name": result.stage_name,
                    "status": result.status.value,
                    "duration_seconds": result.duration_seconds,
                    "error_message": result.error_message,
                    "output_files": result.output_files
                }
                for stage_id, result in self.results.items()
            }
        }
    
    def save_pipeline_state(self, filepath: str = None):
        """Сохранение состояния pipeline"""
        if filepath is None:
            filepath = Path(self.config.logs_dir) / f"pipeline_state_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        state = {
            "config": asdict(self.config),
            "pipeline_status": self.get_pipeline_status(),
            "timestamp": datetime.now().isoformat()
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Состояние pipeline сохранено в {filepath}")
    
    def load_pipeline_state(self, filepath: str):
        """Загрузка состояния pipeline"""
        with open(filepath, 'r', encoding='utf-8') as f:
            state = json.load(f)
        
        # Восстанавливаем конфигурацию
        self.config = PipelineConfig(**state["config"])
        
        # Восстанавливаем результаты этапов
        for stage_id, stage_data in state["pipeline_status"]["stages"].items():
            result = StageResult(
                stage_id=stage_id,
                stage_name=stage_data["name"],
                status=StageStatus(stage_data["status"]),
                duration_seconds=stage_data["duration_seconds"],
                error_message=stage_data["error_message"],
                output_files=stage_data["output_files"]
            )
            self.results[stage_id] = result
        
        logger.info(f"📂 Состояние pipeline загружено из {filepath}")

def main():
    """Главная функция для CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline Manager для анализа диалогов")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=list("1234567") + ["1.5"] + ["all"],
        default=["all"],
        help="Этапы для запуска (по умолчанию: all)"
    )
    parser.add_argument(
        "--from", 
        type=str,
        choices=list("123456") + ["1.5"],
        help="Начать с указанного этапа"
    )
    parser.add_argument(
        "--to", 
        type=str,
        choices=list("123456") + ["1.5"],
        help="Завершить на указанном этапе"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Путь к файлу конфигурации"
    )
    parser.add_argument(
        "--skip-failed",
        action="store_true",
        help="Продолжить выполнение при ошибках"
    )
    
    args = parser.parse_args()
    
    # Определение этапов для запуска
    all_stages = ["1", "1.5", "2", "3", "4", "5", "6", "7"]
    
    if "all" in args.stages:
        stages = all_stages
    else:
        stages = args.stages
    
    # Фильтрация по диапазону
    if hasattr(args, 'from_') and args.from_:
        from_idx = all_stages.index(args.from_)
        stages = [s for s in stages if all_stages.index(s) >= from_idx]
    
    if hasattr(args, 'to') and args.to:
        to_idx = all_stages.index(args.to)
        stages = [s for s in stages if all_stages.index(s) <= to_idx]
    
    # Создание конфигурации
    config = PipelineConfig()
    if args.skip_failed:
        config.skip_failed_stages = True
    
    # Запуск pipeline
    manager = PipelineManager(config)
    success = manager.run_pipeline(stages)
    
    # Сохранение состояния
    manager.save_pipeline_state()
    
    if all(result.status == StageStatus.COMPLETED for result in success.values()):
        logger.info("🎯 Pipeline выполнен успешно!")
        sys.exit(0)
    else:
        logger.error("💥 Pipeline завершен с ошибками")
        sys.exit(1)

if __name__ == "__main__":
    main()
