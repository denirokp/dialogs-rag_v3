#!/usr/bin/env python3
"""
Pipeline Core - Центральные компоненты для работы с pipeline
"""

import logging
import json
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd

from pipeline_manager import PipelineManager, PipelineConfig, StageStatus

logger = logging.getLogger(__name__)

@dataclass
class AnalysisRequest:
    """Запрос на анализ"""
    input_file: str
    stages: List[str] = None
    config: PipelineConfig = None
    callback_url: Optional[str] = None
    user_id: Optional[str] = None

@dataclass
class AnalysisResponse:
    """Ответ анализа"""
    request_id: str
    status: str
    message: str
    results: Dict[str, Any] = None
    error: Optional[str] = None
    created_at: datetime = None
    completed_at: Optional[datetime] = None

class PipelineCore:
    """Центральный класс для работы с pipeline"""
    
    def __init__(self, default_config: PipelineConfig = None):
        self.default_config = default_config or PipelineConfig()
        self.active_analyses: Dict[str, AnalysisResponse] = {}
        self.analysis_history: List[AnalysisResponse] = []
    
    def create_analysis_request(
        self, 
        input_file: str,
        stages: List[str] = None,
        config: PipelineConfig = None,
        user_id: str = None
    ) -> AnalysisRequest:
        """Создание запроса на анализ"""
        return AnalysisRequest(
            input_file=input_file,
            stages=stages or list("123456"),
            config=config or self.default_config,
            user_id=user_id
        )
    
    def run_analysis(self, request: AnalysisRequest) -> AnalysisResponse:
        """Синхронный запуск анализа"""
        request_id = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        response = AnalysisResponse(
            request_id=request_id,
            status="running",
            message="Анализ запущен",
            created_at=datetime.now()
        )
        
        self.active_analyses[request_id] = response
        
        try:
            # Создаем менеджер pipeline
            manager = PipelineManager(request.config)
            
            # Запускаем pipeline
            results = manager.run_pipeline(request.stages)
            
            # Обновляем ответ
            response.status = "completed"
            response.message = "Анализ завершен успешно"
            response.completed_at = datetime.now()
            response.results = {
                "pipeline_status": manager.get_pipeline_status(),
                "output_files": self._collect_output_files(results),
                "metrics": self._calculate_metrics(results)
            }
            
            logger.info(f"✅ Анализ {request_id} завершен успешно")
            
        except Exception as e:
            response.status = "failed"
            response.message = f"Ошибка анализа: {str(e)}"
            response.error = str(e)
            response.completed_at = datetime.now()
            
            logger.error(f"❌ Анализ {request_id} завершен с ошибкой: {e}")
        
        # Перемещаем в историю
        self.active_analyses.pop(request_id, None)
        self.analysis_history.append(response)
        
        return response
    
    async def run_analysis_async(self, request: AnalysisRequest) -> AnalysisResponse:
        """Асинхронный запуск анализа"""
        # Запускаем синхронный анализ в отдельном потоке
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_analysis, request)
    
    def get_analysis_status(self, request_id: str) -> Optional[AnalysisResponse]:
        """Получение статуса анализа"""
        if request_id in self.active_analyses:
            return self.active_analyses[request_id]
        
        # Ищем в истории
        for response in self.analysis_history:
            if response.request_id == request_id:
                return response
        
        return None
    
    def get_analysis_results(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Получение результатов анализа"""
        response = self.get_analysis_status(request_id)
        if response and response.status == "completed":
            return response.results
        return None
    
    def list_analyses(self, user_id: str = None) -> List[AnalysisResponse]:
        """Список анализов"""
        analyses = list(self.active_analyses.values()) + self.analysis_history
        
        if user_id:
            analyses = [a for a in analyses if getattr(a, 'user_id', None) == user_id]
        
        return sorted(analyses, key=lambda x: x.created_at, reverse=True)
    
    def _collect_output_files(self, results: Dict[str, Any]) -> Dict[str, List[str]]:
        """Сбор выходных файлов из результатов"""
        output_files = {}
        
        for stage_id, result in results.items():
            if hasattr(result, 'output_files'):
                output_files[stage_id] = result.output_files
        
        return output_files
    
    def _calculate_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Расчет метрик анализа"""
        total_stages = len(results)
        completed_stages = sum(1 for r in results.values() if r.status == StageStatus.COMPLETED)
        failed_stages = sum(1 for r in results.values() if r.status == StageStatus.FAILED)
        
        total_duration = sum(
            r.duration_seconds for r in results.values() 
            if r.duration_seconds is not None
        )
        
        return {
            "total_stages": total_stages,
            "completed_stages": completed_stages,
            "failed_stages": failed_stages,
            "success_rate": completed_stages / total_stages if total_stages > 0 else 0,
            "total_duration_seconds": total_duration,
            "average_stage_duration": total_duration / completed_stages if completed_stages > 0 else 0
        }
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """Получение информации о pipeline"""
        return {
            "available_stages": {
                "1": "Детекция доставки",
                "2": "Извлечение сущностей", 
                "3": "Нормализация формулировок",
                "4": "Кластеризация",
                "5": "Агрегация метрик",
                "6": "Генерация отчетов"
            },
            "active_analyses": len(self.active_analyses),
            "total_analyses": len(self.analysis_history),
            "default_config": asdict(self.default_config)
        }

class PipelineDataService:
    """Сервис для работы с данными pipeline"""
    
    def __init__(self, artifacts_dir: str = "artifacts", reports_dir: str = "reports"):
        self.artifacts_dir = Path(artifacts_dir)
        self.reports_dir = Path(reports_dir)
        self.artifacts_dir.mkdir(exist_ok=True)
        self.reports_dir.mkdir(exist_ok=True)
    
    def load_stage_results(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Загрузка результатов этапа"""
        stage_files = {
            "1": "stage1_delivery.jsonl",
            "2": "stage2_extracted.jsonl", 
            "3": "stage3_normalized.jsonl",
            "4": "stage4_clusters.json",
            "5": "aggregate_results.json",
            "6": "report.md"
        }
        
        if stage_id not in stage_files:
            return None
        
        file_path = self.artifacts_dir / stage_files[stage_id]
        if not file_path.exists():
            return None
        
        try:
            if file_path.suffix == '.jsonl':
                # Загружаем JSONL файл
                results = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        results.append(json.loads(line.strip()))
                return {"data": results, "count": len(results)}
            
            elif file_path.suffix == '.json':
                # Загружаем JSON файл
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            elif file_path.suffix == '.md':
                # Загружаем Markdown файл
                with open(file_path, 'r', encoding='utf-8') as f:
                    return {"content": f.read()}
            
            else:
                return {"file_path": str(file_path), "type": "unknown"}
                
        except Exception as e:
            logger.error(f"Ошибка загрузки результатов этапа {stage_id}: {e}")
            return None
    
    def get_available_reports(self) -> List[Dict[str, Any]]:
        """Получение списка доступных отчетов"""
        reports = []
        
        for file_path in self.reports_dir.glob("*"):
            if file_path.is_file():
                reports.append({
                    "name": file_path.name,
                    "path": str(file_path),
                    "size": file_path.stat().st_size,
                    "modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                    "type": file_path.suffix
                })
        
        return sorted(reports, key=lambda x: x["modified"], reverse=True)
    
    def get_artifacts_summary(self) -> Dict[str, Any]:
        """Получение сводки по артефактам"""
        summary = {
            "total_files": 0,
            "total_size": 0,
            "by_stage": {},
            "by_type": {}
        }
        
        for file_path in self.artifacts_dir.glob("*"):
            if file_path.is_file():
                summary["total_files"] += 1
                summary["total_size"] += file_path.stat().st_size
                
                # Группировка по этапам
                stage = file_path.stem.split('_')[0] if '_' in file_path.stem else "other"
                if stage not in summary["by_stage"]:
                    summary["by_stage"][stage] = 0
                summary["by_stage"][stage] += 1
                
                # Группировка по типам
                file_type = file_path.suffix
                if file_type not in summary["by_type"]:
                    summary["by_type"][file_type] = 0
                summary["by_type"][file_type] += 1
        
        return summary

# Глобальные экземпляры
pipeline_core = PipelineCore()
data_service = PipelineDataService()
