#!/usr/bin/env python3
"""
Pipeline API - REST API для управления pipeline анализа диалогов
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Query, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import asyncio
import time
import os
import json
import uuid
from typing import List, Optional, Dict, Any
from pathlib import Path
import logging
from datetime import datetime

# Импорты из pipeline core
import sys
sys.path.append(str(Path(__file__).parent.parent))
from core.pipeline_core import pipeline_core, data_service, AnalysisRequest, AnalysisResponse
from pipeline_manager import PipelineConfig

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="Dialogs RAG Pipeline API",
    description="API для управления pipeline анализа диалогов",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware для логирования запросов
@app.middleware("http")
async def log_requests(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")
    return response

# Зависимости
def get_current_user():
    """Получение текущего пользователя (заглушка)"""
    return {"user_id": "default", "role": "user"}

# ==================== ОСНОВНЫЕ ЭНДПОИНТЫ ====================

@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "message": "Dialogs RAG Pipeline API",
        "version": "2.0.0",
        "status": "running",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Проверка здоровья системы"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "pipeline_info": pipeline_core.get_pipeline_info()
    }

# ==================== УПРАВЛЕНИЕ PIPELINE ====================

@app.post("/pipeline/run")
async def run_pipeline(
    input_file: str,
    stages: List[str] = Query(default=None, description="Этапы для запуска (1-6)"),
    config: Optional[Dict[str, Any]] = None,
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(get_current_user)
):
    """Запуск pipeline анализа"""
    try:
        # Создаем конфигурацию
        pipeline_config = PipelineConfig()
        if config:
            for key, value in config.items():
                if hasattr(pipeline_config, key):
                    setattr(pipeline_config, key, value)
        
        # Создаем запрос
        request = pipeline_core.create_analysis_request(
            input_file=input_file,
            stages=stages or list("123456"),
            config=pipeline_config,
            user_id=user["user_id"]
        )
        
        # Запускаем анализ
        if background_tasks:
            # Асинхронный запуск
            response = await pipeline_core.run_analysis_async(request)
        else:
            # Синхронный запуск
            response = pipeline_core.run_analysis(request)
        
        return {
            "request_id": response.request_id,
            "status": response.status,
            "message": response.message,
            "created_at": response.created_at.isoformat(),
            "completed_at": response.completed_at.isoformat() if response.completed_at else None
        }
        
    except Exception as e:
        logger.error(f"Ошибка запуска pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline/status/{request_id}")
async def get_pipeline_status(request_id: str):
    """Получение статуса pipeline"""
    response = pipeline_core.get_analysis_status(request_id)
    if not response:
        raise HTTPException(status_code=404, detail="Анализ не найден")
    
    return {
        "request_id": response.request_id,
        "status": response.status,
        "message": response.message,
        "created_at": response.created_at.isoformat(),
        "completed_at": response.completed_at.isoformat() if response.completed_at else None,
        "error": response.error
    }

@app.get("/pipeline/results/{request_id}")
async def get_pipeline_results(request_id: str):
    """Получение результатов pipeline"""
    results = pipeline_core.get_analysis_results(request_id)
    if not results:
        raise HTTPException(status_code=404, detail="Результаты не найдены")
    
    return results

@app.get("/pipeline/analyses")
async def list_analyses(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100)
):
    """Список анализов"""
    analyses = pipeline_core.list_analyses(user_id)
    return {
        "analyses": analyses[:limit],
        "total": len(analyses)
    }

# ==================== ДАННЫЕ И РЕЗУЛЬТАТЫ ====================

@app.get("/data/stage/{stage_id}")
async def get_stage_data(stage_id: str):
    """Получение данных этапа"""
    data = data_service.load_stage_results(stage_id)
    if not data:
        raise HTTPException(status_code=404, detail="Данные этапа не найдены")
    
    return data

@app.get("/data/artifacts")
async def get_artifacts_summary():
    """Сводка по артефактам"""
    return data_service.get_artifacts_summary()

@app.get("/data/reports")
async def get_available_reports():
    """Список доступных отчетов"""
    return data_service.get_available_reports()

@app.get("/data/download/{file_type}")
async def download_file(file_type: str):
    """Скачивание файла"""
    if file_type == "report_md":
        file_path = Path("reports/report.md")
    elif file_type == "report_xlsx":
        file_path = Path("reports/report.xlsx")
    elif file_type == "appendix":
        file_path = Path("reports/appendix_ids.md")
    else:
        raise HTTPException(status_code=400, detail="Неизвестный тип файла")
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")
    
    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type='application/octet-stream'
    )

# ==================== УПРАВЛЕНИЕ ФАЙЛАМИ ====================

@app.post("/files/upload")
async def upload_file(file: UploadFile = File(...)):
    """Загрузка файла"""
    try:
        # Создаем папку для загруженных файлов
        upload_dir = Path("uploads")
        upload_dir.mkdir(exist_ok=True)
        
        # Сохраняем файл
        file_path = upload_dir / file.filename
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return {
            "filename": file.filename,
            "path": str(file_path),
            "size": len(content),
            "uploaded_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/list")
async def list_files():
    """Список файлов"""
    files = []
    
    # Сканируем папки с файлами
    for directory in ["data", "uploads", "artifacts", "reports"]:
        dir_path = Path(directory)
        if dir_path.exists():
            for file_path in dir_path.glob("*"):
                if file_path.is_file():
                    files.append({
                        "name": file_path.name,
                        "path": str(file_path),
                        "directory": directory,
                        "size": file_path.stat().st_size,
                        "modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                    })
    
    return {"files": files}

# ==================== СИСТЕМНАЯ ИНФОРМАЦИЯ ====================

@app.get("/system/info")
async def get_system_info():
    """Информация о системе"""
    return {
        "pipeline_info": pipeline_core.get_pipeline_info(),
        "artifacts_summary": data_service.get_artifacts_summary(),
        "available_reports": len(data_service.get_available_reports()),
        "system_time": datetime.now().isoformat()
    }

@app.get("/system/logs")
async def get_system_logs(limit: int = Query(default=100, ge=1, le=1000)):
    """Получение системных логов"""
    log_file = Path("logs/pipeline_manager.log")
    if not log_file.exists():
        return {"logs": [], "message": "Лог файл не найден"}
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Возвращаем последние N строк
        recent_lines = lines[-limit:] if len(lines) > limit else lines
        
        return {
            "logs": [line.strip() for line in recent_lines],
            "total_lines": len(lines),
            "returned_lines": len(recent_lines)
        }
    except Exception as e:
        logger.error(f"Ошибка чтения логов: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ОБРАБОТКА ОШИБОК ====================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"detail": "Ресурс не найден", "path": str(request.url.path)}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Внутренняя ошибка: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Внутренняя ошибка сервера"}
    )

# ==================== ЗАПУСК СЕРВЕРА ====================

def main():
    """Запуск API сервера"""
    uvicorn.run(
        "pipeline_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()
