#!/usr/bin/env python3
"""
Simple API для демонстрации результатов анализа диалогов
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание приложения
app = FastAPI(
    title="Dialogs Analysis API",
    description="API для анализа диалогов",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Загрузка данных
def load_analysis_data():
    """Загрузка данных анализа"""
    try:
        # Загружаем comprehensive results
        with open("artifacts/comprehensive_results.json", "r", encoding="utf-8") as f:
            comprehensive_data = json.load(f)
        
        # Загружаем aggregate results
        with open("artifacts/aggregate_results.json", "r", encoding="utf-8") as f:
            aggregate_data = json.load(f)
        
        # Загружаем statistics
        with open("artifacts/statistics.json", "r", encoding="utf-8") as f:
            statistics_data = json.load(f)
        
        return {
            "comprehensive": comprehensive_data,
            "aggregate": aggregate_data,
            "statistics": statistics_data
        }
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")
        return None

# Кэшируем данные
analysis_data = load_analysis_data()

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "message": "Dialogs Analysis API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/api/health")
async def health_check():
    """Проверка здоровья API"""
    return {
        "status": "healthy",
        "data_loaded": analysis_data is not None,
        "timestamp": "2025-09-14T16:58:00Z"
    }

@app.get("/api/statistics")
async def get_statistics():
    """Получение общей статистики"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    stats = analysis_data["statistics"]
    return {
        "total_dialogs": stats.get("total_dialogs", 0),
        "total_entities": stats.get("total_entities", 0),
        "success_rate": stats.get("success_rate", 0),
        "quality_score": stats.get("quality_score", 0),
        "business_relevance": stats.get("business_relevance", 0),
        "actionability": stats.get("actionability", 0)
    }

@app.get("/api/problems")
async def get_problems():
    """Получение списка проблем"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    aggregate = analysis_data["aggregate"]
    problems = aggregate.get("barriers", [])
    
    return {
        "total_problems": len(problems),
        "problems": problems[:50]  # Первые 50 проблем
    }

@app.get("/api/ideas")
async def get_ideas():
    """Получение списка идей"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    aggregate = analysis_data["aggregate"]
    ideas = aggregate.get("ideas", [])
    
    return {
        "total_ideas": len(ideas),
        "ideas": ideas[:50]  # Первые 50 идей
    }

@app.get("/api/signals")
async def get_signals():
    """Получение списка сигналов"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    aggregate = analysis_data["aggregate"]
    signals = aggregate.get("signals", [])
    
    return {
        "total_signals": len(signals),
        "signals": signals[:50]  # Первые 50 сигналов
    }

@app.get("/api/delivery_analysis")
async def get_delivery_analysis():
    """Анализ доставки"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    aggregate = analysis_data["aggregate"]
    delivery_data = aggregate.get("delivery_analysis", {})
    
    return {
        "delivery_mentioned": delivery_data.get("delivery_mentioned", 0),
        "delivery_issues": delivery_data.get("delivery_issues", []),
        "delivery_services": delivery_data.get("delivery_services", [])
    }

@app.get("/api/dialog/{dialog_id}")
async def get_dialog(dialog_id: int):
    """Получение конкретного диалога"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    comprehensive = analysis_data["comprehensive"]
    dialog_results = comprehensive.get("dialog_results", [])
    
    for dialog in dialog_results:
        if dialog.get("dialog_id") == dialog_id:
            return dialog
    
    raise HTTPException(status_code=404, detail="Диалог не найден")

@app.get("/api/search")
async def search_entities(query: str, entity_type: str = "all"):
    """Поиск по сущностям"""
    if not analysis_data:
        raise HTTPException(status_code=500, detail="Данные не загружены")
    
    aggregate = analysis_data["aggregate"]
    results = []
    
    if entity_type in ["all", "problems"]:
        problems = aggregate.get("barriers", [])
        for problem in problems:
            if query.lower() in problem.get("text", "").lower():
                results.append({"type": "problem", "data": problem})
    
    if entity_type in ["all", "ideas"]:
        ideas = aggregate.get("ideas", [])
        for idea in ideas:
            if query.lower() in idea.get("text", "").lower():
                results.append({"type": "idea", "data": idea})
    
    if entity_type in ["all", "signals"]:
        signals = aggregate.get("signals", [])
        for signal in signals:
            if query.lower() in signal.get("text", "").lower():
                results.append({"type": "signal", "data": signal})
    
    return {
        "query": query,
        "entity_type": entity_type,
        "total_results": len(results),
        "results": results[:20]  # Первые 20 результатов
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
