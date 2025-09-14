#!/usr/bin/env python3
"""
Unified Pipeline Manager - Единый менеджер пайплайна
Объединяет все существующие менеджеры пайплайнов
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
import subprocess
import os

# Добавляем папку pipeline в путь
pipeline_path = str(Path(__file__).parent)
if pipeline_path not in sys.path:
    sys.path.append(pipeline_path)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/unified_pipeline_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineMode(Enum):
    """Режимы работы пайплайна"""
    LEGACY = "legacy"
    PIPELINE = "pipeline"
    ENHANCED = "enhanced"
    SCALED = "scaled"
    AUTO = "auto"

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
    name: str
    status: StageStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    output_files: List[str] = None
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = None

@dataclass
class PipelineConfig:
    """Конфигурация пайплайна"""
    mode: PipelineMode = PipelineMode.AUTO
    input_file: str = "data/input/dialogs.xlsx"
    batch_id: str = None
    n_dialogs: int = 10000
    max_workers: int = 4
    enable_quality_checks: bool = True
    enable_clustering: bool = True
    enable_enhanced_analysis: bool = False
    cleanup_intermediate: bool = False

class UnifiedPipelineManager:
    """Единый менеджер пайплайна"""
    
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.stages = self._initialize_stages()
        self.results: Dict[str, StageResult] = {}
        self.pipeline_start_time: Optional[datetime] = None
        self.pipeline_end_time: Optional[datetime] = None
        
        # Создаем необходимые директории
        self._create_directories()
        
        # Автоматически определяем режим если не задан
        if self.config.mode == PipelineMode.AUTO:
            self.config.mode = self._detect_mode()
    
    def _detect_mode(self) -> PipelineMode:
        """Автоматическое определение режима"""
        if Path("data/duckdb/mentions.duckdb").exists():
            return PipelineMode.SCALED
        elif Path("artifacts/stage4_5_semantic_enrichment.json").exists():
            return PipelineMode.ENHANCED
        elif Path("artifacts/stage4_clusters.json").exists():
            return PipelineMode.PIPELINE
        else:
            return PipelineMode.LEGACY
    
    def _create_directories(self):
        """Создание необходимых директорий"""
        directories = [
            "data/raw", "data/processed", "data/duckdb", "data/warehouse",
            "artifacts", "reports", "logs"
        ]
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def _initialize_stages(self) -> Dict[str, Dict[str, Any]]:
        """Инициализация этапов пайплайна"""
        return {
            "1": {
                "name": "Ingest & Normalize",
                "function": self._run_ingest_normalize,
                "dependencies": [],
                "output_files": ["data/processed/dialogs.parquet"],
                "description": "Загрузка и нормализация данных"
            },
            "2": {
                "name": "Client Filter & Windows",
                "function": self._run_client_filter_windows,
                "dependencies": ["1"],
                "output_files": ["data/processed/windows.parquet"],
                "description": "Фильтрация клиентских реплик и нарезка окон"
            },
            "3": {
                "name": "Entity Extraction",
                "function": self._run_entity_extraction,
                "dependencies": ["2"],
                "output_files": ["data/duckdb/mentions.duckdb"],
                "description": "Извлечение сущностей (барьеры, идеи, сигналы)"
            },
            "4": {
                "name": "Clustering",
                "function": self._run_clustering,
                "dependencies": ["3"],
                "output_files": ["data/duckdb/clusters.duckdb"],
                "description": "Кластеризация цитат"
            },
            "5": {
                "name": "Aggregation",
                "function": self._run_aggregation,
                "dependencies": ["4"],
                "output_files": ["data/duckdb/summaries.duckdb"],
                "description": "Агрегация метрик и создание витрин"
            },
            "6": {
                "name": "Quality Check",
                "function": self._run_quality_check,
                "dependencies": ["5"],
                "output_files": ["reports/quality_report.json"],
                "description": "Проверка качества и DoD метрик"
            },
            "7": {
                "name": "Report Generation",
                "function": self._run_report_generation,
                "dependencies": ["6"],
                "output_files": ["reports/analysis_report.md"],
                "description": "Генерация финальных отчетов"
            }
        }
    
    async def run_pipeline(self) -> Dict[str, Any]:
        """Запуск пайплайна"""
        self.pipeline_start_time = datetime.now()
        logger.info(f"🚀 Запуск унифицированного пайплайна в режиме: {self.config.mode.value}")
        
        try:
            # Выполняем этапы последовательно
            for stage_id, stage_info in self.stages.items():
                await self._run_stage(stage_id, stage_info)
            
            self.pipeline_end_time = datetime.now()
            duration = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
            
            logger.info(f"✅ Пайплайн завершен за {duration:.2f} секунд")
            
            return {
                "status": "completed",
                "mode": self.config.mode.value,
                "duration": duration,
                "stages": {k: asdict(v) for k, v in self.results.items()},
                "summary": self._generate_summary()
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка пайплайна: {e}")
            return {
                "status": "failed",
                "mode": self.config.mode.value,
                "error": str(e),
                "stages": {k: asdict(v) for k, v in self.results.items()}
            }
    
    async def _run_stage(self, stage_id: str, stage_info: Dict[str, Any]):
        """Выполнение отдельного этапа"""
        logger.info(f"🔄 Запуск этапа {stage_id}: {stage_info['name']}")
        
        # Проверяем зависимости
        for dep in stage_info.get("dependencies", []):
            if dep not in self.results or self.results[dep].status != StageStatus.COMPLETED:
                logger.warning(f"⚠️ Пропуск этапа {stage_id} - не выполнена зависимость {dep}")
                self.results[stage_id] = StageResult(
                    stage_id=stage_id,
                    name=stage_info["name"],
                    status=StageStatus.SKIPPED
                )
                return
        
        # Запускаем этап
        start_time = datetime.now()
        self.results[stage_id] = StageResult(
            stage_id=stage_id,
            name=stage_info["name"],
            status=StageStatus.RUNNING,
            start_time=start_time
        )
        
        try:
            result = await stage_info["function"]()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results[stage_id].status = StageStatus.COMPLETED
            self.results[stage_id].end_time = end_time
            self.results[stage_id].duration = duration
            self.results[stage_id].output_files = result.get("output_files", [])
            self.results[stage_id].metrics = result.get("metrics", {})
            
            logger.info(f"✅ Этап {stage_id} завершен за {duration:.2f} секунд")
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results[stage_id].status = StageStatus.FAILED
            self.results[stage_id].end_time = end_time
            self.results[stage_id].duration = duration
            self.results[stage_id].error_message = str(e)
            
            logger.error(f"❌ Этап {stage_id} завершился с ошибкой: {e}")
            raise
    
    async def _run_ingest_normalize(self) -> Dict[str, Any]:
        """Этап 1: Загрузка и нормализация данных"""
        if self.config.mode == PipelineMode.SCALED:
            return await self._run_scaled_ingest()
        else:
            return await self._run_legacy_ingest()
    
    async def _run_scaled_ingest(self) -> Dict[str, Any]:
        """Scaled ingest с Polars"""
        try:
            import polars as pl
            from app.etl.ingest import read_any
            from app.etl.normalize import to_canonical
            
            # Читаем данные
            df = read_any(self.config.input_file)
            df = to_canonical(df)
            
            # Сохраняем
            df.write_parquet("data/processed/dialogs.parquet")
            
            return {
                "output_files": ["data/processed/dialogs.parquet"],
                "metrics": {"rows": len(df)}
            }
        except ImportError:
            # Fallback на legacy
            return await self._run_legacy_ingest()
    
    async def _run_legacy_ingest(self) -> Dict[str, Any]:
        """Legacy ingest"""
        try:
            # Используем существующий pipeline
            result = subprocess.run([
                "python", "-m", "pipeline.ingest_excel",
                "--file", self.config.input_file,
                "--batch", self.config.batch_id or "unified"
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Ingest failed: {result.stderr}")
            
            return {
                "output_files": [f"data/warehouse/utterances_{self.config.batch_id or 'unified'}.parquet"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Legacy ingest failed: {e}")
    
    async def _run_client_filter_windows(self) -> Dict[str, Any]:
        """Этап 2: Фильтрация клиентских реплик и нарезка окон"""
        if self.config.mode == PipelineMode.SCALED:
            return await self._run_scaled_windows()
        else:
            return await self._run_legacy_extract()
    
    async def _run_scaled_windows(self) -> Dict[str, Any]:
        """Scaled windows с Polars"""
        try:
            import polars as pl
            from app.etl.split_windows import client_only, windows_by_dialog
            
            # Читаем данные
            df = pl.read_parquet("data/processed/dialogs.parquet")
            
            # Фильтруем клиентские реплики
            client_df = client_only(df)
            
            # Нарезаем окна
            windows_df = windows_by_dialog(client_df)
            
            # Сохраняем
            windows_df.write_parquet("data/processed/windows.parquet")
            
            return {
                "output_files": ["data/processed/windows.parquet"],
                "metrics": {"windows": len(windows_df), "client_utterances": len(client_df)}
            }
        except ImportError:
            return await self._run_legacy_extract()
    
    async def _run_legacy_extract(self) -> Dict[str, Any]:
        """Legacy extract"""
        try:
            result = subprocess.run([
                "python", "-m", "pipeline.extract_entities",
                "--batch", self.config.batch_id or "unified"
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Extract failed: {result.stderr}")
            
            return {
                "output_files": [f"data/warehouse/mentions_{self.config.batch_id or 'unified'}.parquet"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Legacy extract failed: {e}")
    
    async def _run_entity_extraction(self) -> Dict[str, Any]:
        """Этап 3: Извлечение сущностей"""
        if self.config.mode == PipelineMode.SCALED:
            return await self._run_scaled_extraction()
        else:
            return await self._run_legacy_normalize()
    
    async def _run_scaled_extraction(self) -> Dict[str, Any]:
        """Scaled extraction с LLM"""
        try:
            import polars as pl
            from app.llm.extract import extract_mentions_for_windows
            from app.llm.llm_client import LLMClient
            from app.utils.io import Duck
            
            # Читаем окна
            windows_df = pl.read_parquet("data/processed/windows.parquet")
            
            # Инициализируем LLM
            llm = LLMClient()
            
            # Извлекаем упоминания
            mentions_df = extract_mentions_for_windows(llm, windows_df, "taxonomy.yaml")
            
            # Сохраняем в DuckDB
            duck = Duck("data/duckdb/mentions.duckdb")
            duck.write_df(mentions_df, "mentions")
            
            return {
                "output_files": ["data/duckdb/mentions.duckdb"],
                "metrics": {"mentions": len(mentions_df)}
            }
        except ImportError:
            return await self._run_legacy_normalize()
    
    async def _run_legacy_normalize(self) -> Dict[str, Any]:
        """Legacy normalize"""
        try:
            result = subprocess.run([
                "python", "-m", "pipeline.normalize",
                "--batch", self.config.batch_id or "unified"
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Normalize failed: {result.stderr}")
            
            return {
                "output_files": [f"data/warehouse/mentions_norm_{self.config.batch_id or 'unified'}.parquet"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Legacy normalize failed: {e}")
    
    async def _run_clustering(self) -> Dict[str, Any]:
        """Этап 4: Кластеризация"""
        if not self.config.enable_clustering:
            return {"output_files": [], "metrics": {"status": "skipped"}}
        
        if self.config.mode == PipelineMode.SCALED:
            return await self._run_scaled_clustering()
        else:
            return await self._run_legacy_clustering()
    
    async def _run_scaled_clustering(self) -> Dict[str, Any]:
        """Scaled clustering"""
        try:
            import polars as pl
            from app.clustering.cluster_quotes import cluster_quotes
            from app.clustering.embed import Embedder
            from app.utils.io import Duck
            
            # Читаем упоминания
            duck = Duck("data/duckdb/mentions.duckdb")
            mentions_df = duck.query("SELECT * FROM mentions")
            
            # Кластеризуем
            embedder = Embedder()
            clusters_df = cluster_quotes(mentions_df, embedder)
            
            # Сохраняем
            duck.write_df(clusters_df, "clusters")
            
            return {
                "output_files": ["data/duckdb/clusters.duckdb"],
                "metrics": {"clusters": clusters_df["cluster"].n_unique()}
            }
        except ImportError:
            return await self._run_legacy_clustering()
    
    async def _run_legacy_clustering(self) -> Dict[str, Any]:
        """Legacy clustering"""
        try:
            result = subprocess.run([
                "python", "-m", "pipeline.cluster_enrich",
                "--batch", self.config.batch_id or "unified"
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Clustering failed: {result.stderr}")
            
            return {
                "output_files": ["artifacts/clusters.json"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Legacy clustering failed: {e}")
    
    async def _run_aggregation(self) -> Dict[str, Any]:
        """Этап 5: Агрегация"""
        if self.config.mode == PipelineMode.SCALED:
            return await self._run_scaled_aggregation()
        else:
            return await self._run_legacy_aggregation()
    
    async def _run_scaled_aggregation(self) -> Dict[str, Any]:
        """Scaled aggregation"""
        try:
            from app.utils.io import Duck
            
            duck = Duck("data/duckdb/mentions.duckdb")
            
            # Выполняем SQL агрегации
            sql_files = ["app/sql/summary.sql", "app/sql/subthemes.sql", "app/sql/cooccurrence.sql"]
            for sql_file in sql_files:
                if Path(sql_file).exists():
                    sql = Path(sql_file).read_text(encoding="utf-8")
                    duck.query(sql)
            
            return {
                "output_files": ["data/duckdb/summaries.duckdb"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            return await self._run_legacy_aggregation()
    
    async def _run_legacy_aggregation(self) -> Dict[str, Any]:
        """Legacy aggregation"""
        try:
            result = subprocess.run([
                "python", "-m", "pipeline.aggregate",
                "--batch", self.config.batch_id or "unified",
                "--n_dialogs", str(self.config.n_dialogs)
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Aggregation failed: {result.stderr}")
            
            return {
                "output_files": ["artifacts/aggregate_results.json"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Legacy aggregation failed: {e}")
    
    async def _run_quality_check(self) -> Dict[str, Any]:
        """Этап 6: Проверка качества"""
        if not self.config.enable_quality_checks:
            return {"output_files": [], "metrics": {"status": "skipped"}}
        
        try:
            from quality.unified_quality import quality_checker
            
            # Проверяем качество
            report = quality_checker.get_quality_report(self.config.mode)
            
            # Сохраняем отчет
            with open("reports/quality_report.json", "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            return {
                "output_files": ["reports/quality_report.json"],
                "metrics": {"passed": report["passed"]}
            }
        except Exception as e:
            raise Exception(f"Quality check failed: {e}")
    
    async def _run_report_generation(self) -> Dict[str, Any]:
        """Этап 7: Генерация отчетов"""
        try:
            # Создаем простой отчет
            report_content = f"""
# Unified Pipeline Report

**Mode:** {self.config.mode.value}
**Start Time:** {self.pipeline_start_time}
**End Time:** {self.pipeline_end_time}
**Duration:** {(self.pipeline_end_time - self.pipeline_start_time).total_seconds():.2f} seconds

## Stages Summary

"""
            
            for stage_id, result in self.results.items():
                report_content += f"### Stage {stage_id}: {result.name}\n"
                report_content += f"- **Status:** {result.status.value}\n"
                report_content += f"- **Duration:** {result.duration:.2f}s\n"
                if result.error_message:
                    report_content += f"- **Error:** {result.error_message}\n"
                report_content += "\n"
            
            # Сохраняем отчет
            with open("reports/analysis_report.md", "w", encoding="utf-8") as f:
                f.write(report_content)
            
            return {
                "output_files": ["reports/analysis_report.md"],
                "metrics": {"status": "completed"}
            }
        except Exception as e:
            raise Exception(f"Report generation failed: {e}")
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Генерация сводки пайплайна"""
        total_stages = len(self.stages)
        completed_stages = sum(1 for r in self.results.values() if r.status == StageStatus.COMPLETED)
        failed_stages = sum(1 for r in self.results.values() if r.status == StageStatus.FAILED)
        skipped_stages = sum(1 for r in self.results.values() if r.status == StageStatus.SKIPPED)
        
        return {
            "total_stages": total_stages,
            "completed_stages": completed_stages,
            "failed_stages": failed_stages,
            "skipped_stages": skipped_stages,
            "success_rate": completed_stages / total_stages if total_stages > 0 else 0
        }

# CLI интерфейс
async def main():
    """Главная функция для CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified Pipeline Manager")
    parser.add_argument("--mode", choices=["legacy", "pipeline", "enhanced", "scaled", "auto"], default="auto")
    parser.add_argument("--input", default="data/input/dialogs.xlsx")
    parser.add_argument("--batch", default=None)
    parser.add_argument("--n-dialogs", type=int, default=10000)
    parser.add_argument("--no-quality", action="store_true")
    parser.add_argument("--no-clustering", action="store_true")
    
    args = parser.parse_args()
    
    config = PipelineConfig(
        mode=PipelineMode(args.mode),
        input_file=args.input,
        batch_id=args.batch,
        n_dialogs=args.n_dialogs,
        enable_quality_checks=not args.no_quality,
        enable_clustering=not args.no_clustering
    )
    
    manager = UnifiedPipelineManager(config)
    result = await manager.run_pipeline()
    
    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
