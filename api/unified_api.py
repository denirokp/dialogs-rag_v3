#!/usr/bin/env python3
"""
Unified API - Единый API для всех режимов анализа диалогов
Объединяет legacy, pipeline, enhanced и scaled системы
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import duckdb
import polars as pl
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

# Импорты существующих API
from api.main import con as legacy_con, ensure_passed as legacy_ensure_passed
from api.pipeline_api import app as pipeline_app

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание основного приложения
app = FastAPI(
    title="Unified Dialogs RAG API",
    description="Единый API для всех режимов анализа диалогов (Legacy, Pipeline, Enhanced, Scaled)",
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

# Конфигурация
DB_LEGACY = os.getenv("DUCKDB_PATH", "data/rag.duckdb")
DB_SCALED = "data/duckdb/mentions.duckdb"
REQUIRE_PASS = os.getenv("REQUIRE_QUALITY_PASS", "true").lower() == "true"

class DataMode:
    """Режимы работы с данными"""
    LEGACY = "legacy"
    PIPELINE = "pipeline" 
    ENHANCED = "enhanced"
    SCALED = "scaled"
    AUTO = "auto"

def detect_data_mode() -> str:
    """Автоматическое определение режима данных"""
    if Path("data/duckdb/mentions.duckdb").exists():
        return DataMode.SCALED
    elif Path("artifacts/stage4_5_semantic_enrichment.json").exists():
        return DataMode.ENHANCED
    elif Path("artifacts/stage4_clusters.json").exists():
        return DataMode.PIPELINE
    elif Path("data/warehouse/mentions_final.parquet").exists():
        return DataMode.LEGACY
    else:
        return DataMode.SCALED

def get_db_connection(mode: str = None):
    """Получение подключения к БД в зависимости от режима"""
    if mode is None:
        mode = detect_data_mode()
    
    if mode == DataMode.SCALED:
        return duckdb.connect(DB_SCALED, read_only=True)
    else:
        return legacy_con()

# ==================== УНИФИЦИРОВАННЫЕ ENDPOINTS ====================

@app.get("/api/health")
async def health_check():
    """Проверка здоровья системы"""
    try:
        mode = detect_data_mode()
        con = get_db_connection(mode)
        
        # Проверяем доступность данных
        if mode == DataMode.SCALED:
            mentions_count = con.execute("SELECT COUNT(*) FROM mentions").fetchone()[0]
        else:
            mentions_count = con.execute("SELECT COUNT(*) FROM mentions_final").fetchone()[0]
        
        return {
            "status": "healthy",
            "mode": mode,
            "mentions_count": mentions_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/quality")
async def unified_quality(mode: str = Query(DataMode.AUTO, description="Режим данных")):
    """Унифицированная проверка качества"""
    try:
        if mode == DataMode.AUTO:
            mode = detect_data_mode()
        
        con = get_db_connection(mode)
        
        if mode == DataMode.SCALED:
            # Scaled система - используем новую логику
            evidence = con.execute("SELECT SUM(CASE WHEN text_quote IS NULL OR LENGTH(text_quote)=0 THEN 1 ELSE 0 END)=0 FROM mentions").fetchone()[0]
            dedup = con.execute("WITH t AS (SELECT dialog_id, theme, text_quote, COUNT(*) c FROM mentions GROUP BY 1,2,3), agg AS (SELECT SUM(c) total, SUM(CASE WHEN c>1 THEN c-1 ELSE 0 END) dups FROM t) SELECT CAST(dups AS DOUBLE)/NULLIF(total,0) FROM agg").fetchone()[0]
            coverage_other = con.execute("SELECT 100.0*COUNT(DISTINCT dialog_id)/(SELECT COUNT(DISTINCT dialog_id) FROM mentions) FROM mentions WHERE theme='прочее'").fetchone()[0]
            
            return {
                "evidence_100": bool(evidence),
                "dedup_rate": float(dedup or 0.0),
                "coverage_other_pct": float(coverage_other or 0.0),
                "mode": mode,
                "passed": bool(evidence) and (dedup or 0.0) <= 0.01 and (coverage_other or 0.0) <= 2.0
            }
        else:
            # Legacy/Pipeline/Enhanced системы
            q = con.execute("SELECT * FROM quality ORDER BY ROW_NUMBER() OVER () DESC LIMIT 1").fetch_df()
            if q.empty:
                raise HTTPException(404, "Quality data not found")
            
            result = q.iloc[0].to_dict()
            result["mode"] = mode
            return result
            
    except Exception as e:
        logger.error(f"Quality check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary_themes")
async def unified_summary_themes(mode: str = Query(DataMode.AUTO, description="Режим данных")):
    """Унифицированная сводка по темам"""
    try:
        if mode == DataMode.AUTO:
            mode = detect_data_mode()
        
        con = get_db_connection(mode)
        
        if mode == DataMode.SCALED:
            # Scaled система - используем SQL из app/sql/summary.sql
            sql_path = Path("app/sql/summary.sql")
            if sql_path.exists():
                sql = sql_path.read_text(encoding="utf-8")
                themes_df = con.execute(sql).df()
            else:
                # Fallback SQL
                themes_df = con.execute("""
                    SELECT theme, COUNT(DISTINCT dialog_id) as dialogov, 
                           COUNT(*) as upominanii,
                           ROUND(100.0 * COUNT(DISTINCT dialog_id) / (SELECT COUNT(DISTINCT dialog_id) FROM mentions), 1) AS share_dialogs_pct
                    FROM mentions 
                    WHERE theme IS NOT NULL AND theme <> ''
                    GROUP BY theme 
                    ORDER BY dialogov DESC
                """).df()
            
            total_dialogs = con.execute("SELECT COUNT(DISTINCT dialog_id) FROM mentions").fetchone()[0]
            total_mentions = themes_df["upominanii"].sum() if not themes_df.empty else 0
            
            return {
                "mode": mode,
                "totals": {"dialogs": int(total_dialogs), "mentions": int(total_mentions)},
                "themes": themes_df.to_dict("records")
            }
        else:
            # Legacy/Pipeline/Enhanced системы
            tot = con.execute("SELECT MAX(totalDialogs) FROM quality").fetchone()[0] or 0
            rows = con.execute("SELECT theme, dialogs, mentions, share FROM summary_themes ORDER BY dialogs DESC").df()
            
            return {
                "mode": mode,
                "totals": {"dialogs": int(tot), "mentions": int(rows["mentions"].sum() if not rows.empty else 0)},
                "themes": rows.to_dict("records")
            }
            
    except Exception as e:
        logger.error(f"Summary themes failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/summary_subthemes")
async def unified_summary_subthemes(
    theme: Optional[str] = Query(None, description="Фильтр по теме"),
    mode: str = Query(DataMode.AUTO, description="Режим данных")
):
    """Унифицированная сводка по подтемам"""
    try:
        if mode == DataMode.AUTO:
            mode = detect_data_mode()
        
        con = get_db_connection(mode)
        
        if mode == DataMode.SCALED:
            # Scaled система
            sql_path = Path("app/sql/subthemes.sql")
            if sql_path.exists():
                sql = sql_path.read_text(encoding="utf-8")
            else:
                sql = """
                    SELECT theme, subtheme,
                           COUNT(DISTINCT dialog_id) AS dialogov,
                           COUNT(*) AS upominanii,
                           ROUND(100.0 * COUNT(DISTINCT dialog_id) / (SELECT COUNT(DISTINCT dialog_id) FROM mentions), 1) AS share_dialogs_pct
                    FROM mentions
                    WHERE theme IS NOT NULL AND theme <> ''
                    GROUP BY theme, subtheme
                    ORDER BY dialogov DESC
                """
            
            if theme:
                sql = sql.replace("WHERE theme IS NOT NULL AND theme <> ''", f"WHERE theme = '{theme}' AND theme IS NOT NULL AND theme <> ''")
            
            subs_df = con.execute(sql).df()
            return {"mode": mode, "items": subs_df.to_dict("records")}
        else:
            # Legacy/Pipeline/Enhanced системы
            sql = "SELECT theme, subtheme, dialogs, mentions, share FROM summary_subthemes"
            args = []
            if theme:
                sql += " WHERE theme = ?"
                args.append(theme)
            sql += " ORDER BY dialogs DESC LIMIT 200"
            
            rows = con.execute(sql, args).df()
            return {"mode": mode, "items": rows.to_dict("records")}
            
    except Exception as e:
        logger.error(f"Summary subthemes failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/index_quotes")
async def unified_index_quotes(
    theme: Optional[str] = Query(None, description="Фильтр по теме"),
    subtheme: Optional[str] = Query(None, description="Фильтр по подтеме"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(50, ge=1, le=1000, description="Размер страницы"),
    mode: str = Query(DataMode.AUTO, description="Режим данных")
):
    """Унифицированный индекс цитат"""
    try:
        if mode == DataMode.AUTO:
            mode = detect_data_mode()
        
        con = get_db_connection(mode)
        
        where_conditions = []
        args = []
        
        if theme:
            where_conditions.append("theme = ?")
            args.append(theme)
        if subtheme:
            where_conditions.append("subtheme = ?")
            args.append(subtheme)
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        if mode == DataMode.SCALED:
            sql = f"SELECT dialog_id, turn_id, theme, subtheme, text_quote, confidence FROM mentions{where_clause} ORDER BY dialog_id, turn_id LIMIT ? OFFSET ?"
        else:
            sql = f"SELECT dialog_id, turn_id, theme, subtheme, label_type, text_quote, confidence FROM index_quotes{where_clause} ORDER BY dialog_id, turn_id LIMIT ? OFFSET ?"
        
        args.extend([page_size, (page-1)*page_size])
        
        rows = con.execute(sql, args).df()
        next_page = None if rows.empty or len(rows) < page_size else page + 1
        
        return {
            "mode": mode,
            "items": rows.to_dict("records"),
            "next_page": next_page
        }
        
    except Exception as e:
        logger.error(f"Index quotes failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cooccurrence")
async def unified_cooccurrence(
    top: int = Query(50, ge=1, le=1000, description="Количество топ пар"),
    mode: str = Query(DataMode.AUTO, description="Режим данных")
):
    """Унифицированная совстречаемость тем"""
    try:
        if mode == DataMode.AUTO:
            mode = detect_data_mode()
        
        con = get_db_connection(mode)
        
        if mode == DataMode.SCALED:
            # Scaled система
            sql_path = Path("app/sql/cooccurrence.sql")
            if sql_path.exists():
                sql = sql_path.read_text(encoding="utf-8")
            else:
                sql = """
                    WITH per_dialog AS (
                      SELECT dialog_id, LIST(DISTINCT theme) AS themes
                      FROM mentions
                      WHERE theme IS NOT NULL AND theme <> ''
                      GROUP BY 1
                    )
                    SELECT a.theme AS theme_a, b.theme AS theme_b, COUNT(*) AS cnt
                    FROM per_dialog, UNNEST(themes) a(theme), UNNEST(themes) b(theme)
                    WHERE a.theme < b.theme
                    GROUP BY 1,2
                    ORDER BY cnt DESC
                """
            
            co_df = con.execute(sql).df()
            return {"mode": mode, "items": co_df.head(top).to_dict("records")}
        else:
            # Legacy/Pipeline/Enhanced системы
            rows = con.execute("SELECT themeA, themeB, weight FROM cooccur ORDER BY weight DESC LIMIT ?", [top]).df()
            return {"mode": mode, "items": rows.to_dict("records")}
            
    except Exception as e:
        logger.error(f"Cooccurrence failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/system_info")
async def system_info():
    """Информация о системе и доступных режимах"""
    try:
        mode = detect_data_mode()
        con = get_db_connection(mode)
        
        # Получаем статистику
        if mode == DataMode.SCALED:
            total_dialogs = con.execute("SELECT COUNT(DISTINCT dialog_id) FROM mentions").fetchone()[0]
            total_mentions = con.execute("SELECT COUNT(*) FROM mentions").fetchone()[0]
        else:
            total_dialogs = con.execute("SELECT MAX(totalDialogs) FROM quality").fetchone()[0] or 0
            total_mentions = con.execute("SELECT COUNT(*) FROM mentions_final").fetchone()[0]
        
        return {
            "current_mode": mode,
            "available_modes": [DataMode.LEGACY, DataMode.PIPELINE, DataMode.ENHANCED, DataMode.SCALED],
            "statistics": {
                "total_dialogs": int(total_dialogs),
                "total_mentions": int(total_mentions)
            },
            "features": {
                "scaled_processing": mode == DataMode.SCALED,
                "enhanced_analysis": mode in [DataMode.ENHANCED, DataMode.SCALED],
                "pipeline_stages": mode in [DataMode.PIPELINE, DataMode.ENHANCED, DataMode.SCALED],
                "legacy_compatibility": True
            }
        }
        
    except Exception as e:
        logger.error(f"System info failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== LEGACY COMPATIBILITY ====================

# Включаем все существующие endpoints из legacy API
app.include_router(pipeline_app.router, prefix="/pipeline", tags=["Pipeline System"])

# Legacy endpoints для обратной совместимости
@app.get("/api/quality_legacy")
async def quality_legacy():
    """Legacy endpoint для обратной совместимости"""
    return await unified_quality(mode=DataMode.LEGACY)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
