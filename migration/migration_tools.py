#!/usr/bin/env python3
"""
Migration Tools - Инструменты миграции данных между режимами
Обеспечивает плавный переход между legacy, pipeline, enhanced и scaled системами
"""

import os
import sys
import json
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime
import subprocess

# Добавляем путь к модулям
sys.path.append(str(Path(__file__).parent.parent))

from adapters.data_adapter import UnifiedDataAdapter
from config.config_loader import get_config

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MigrationManager:
    """Менеджер миграции данных"""
    
    def __init__(self):
        self.config = get_config()
        self.data_adapter = UnifiedDataAdapter()
        self.migration_log = []
    
    def detect_current_state(self) -> Dict[str, Any]:
        """Определение текущего состояния системы"""
        state = {
            "timestamp": datetime.now().isoformat(),
            "data_sources": {},
            "available_modes": [],
            "migration_needed": False
        }
        
        # Проверяем наличие данных в разных форматах
        data_sources = {
            "legacy": {
                "warehouse": Path("data/warehouse/mentions_final.parquet").exists(),
                "duckdb": Path("data/rag.duckdb").exists(),
                "utterances": Path("data/warehouse/utterances_*.parquet").exists()
            },
            "pipeline": {
                "artifacts": Path("artifacts/stage4_clusters.json").exists(),
                "extractions": Path("artifacts/stage2_extracted.jsonl").exists(),
                "normalized": Path("artifacts/stage3_normalized.jsonl").exists()
            },
            "enhanced": {
                "enhanced_clusters": Path("artifacts/stage4_5_semantic_enrichment.json").exists(),
                "enhanced_extractions": Path("artifacts/stage2_extracted_enhanced.jsonl").exists(),
                "contextual": Path("artifacts/stage2_5_contextual_analysis.json").exists()
            },
            "scaled": {
                "mentions_db": Path("data/duckdb/mentions.duckdb").exists(),
                "processed_data": Path("data/processed/dialogs.parquet").exists(),
                "windows": Path("data/processed/windows.parquet").exists()
            }
        }
        
        state["data_sources"] = data_sources
        
        # Определяем доступные режимы
        for mode, sources in data_sources.items():
            if any(sources.values()):
                state["available_modes"].append(mode)
        
        # Определяем нужна ли миграция
        if "scaled" not in state["available_modes"] and len(state["available_modes"]) > 0:
            state["migration_needed"] = True
        
        return state
    
    def plan_migration(self, target_mode: str = "scaled") -> Dict[str, Any]:
        """Планирование миграции"""
        current_state = self.detect_current_state()
        
        plan = {
            "target_mode": target_mode,
            "current_state": current_state,
            "migration_steps": [],
            "estimated_time": 0,
            "risks": [],
            "backup_required": True
        }
        
        if target_mode == "scaled":
            # Планируем миграцию в scaled режим
            if "legacy" in current_state["available_modes"]:
                plan["migration_steps"].append({
                    "step": "migrate_legacy_to_scaled",
                    "description": "Миграция данных из legacy в scaled формат",
                    "estimated_time": 300,  # 5 минут
                    "risk_level": "low"
                })
            
            if "pipeline" in current_state["available_modes"]:
                plan["migration_steps"].append({
                    "step": "migrate_pipeline_to_scaled",
                    "description": "Миграция данных из pipeline в scaled формат",
                    "estimated_time": 180,  # 3 минуты
                    "risk_level": "low"
                })
            
            if "enhanced" in current_state["available_modes"]:
                plan["migration_steps"].append({
                    "step": "migrate_enhanced_to_scaled",
                    "description": "Миграция данных из enhanced в scaled формат",
                    "estimated_time": 240,  # 4 минуты
                    "risk_level": "medium"
                })
            
            # Создание структуры scaled системы
            plan["migration_steps"].append({
                "step": "create_scaled_structure",
                "description": "Создание структуры директорий и файлов для scaled режима",
                "estimated_time": 60,  # 1 минута
                "risk_level": "low"
            })
        
        # Подсчитываем общее время
        plan["estimated_time"] = sum(step["estimated_time"] for step in plan["migration_steps"])
        
        # Определяем риски
        if any(step["risk_level"] == "high" for step in plan["migration_steps"]):
            plan["risks"].append("high_risk_operations")
        
        if plan["estimated_time"] > 1800:  # 30 минут
            plan["risks"].append("long_downtime")
        
        return plan
    
    def create_backup(self) -> bool:
        """Создание резервной копии данных"""
        try:
            backup_dir = Path(f"backups/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Копируем важные директории
            dirs_to_backup = [
                "data/warehouse",
                "data/duckdb", 
                "artifacts",
                "reports"
            ]
            
            for dir_path in dirs_to_backup:
                src = Path(dir_path)
                if src.exists():
                    dst = backup_dir / dir_path
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(src, dst, dirs_exist_ok=True)
            
            # Сохраняем информацию о бэкапе
            backup_info = {
                "timestamp": datetime.now().isoformat(),
                "backup_dir": str(backup_dir),
                "directories": dirs_to_backup
            }
            
            with open(backup_dir / "backup_info.json", "w", encoding="utf-8") as f:
                json.dump(backup_info, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Резервная копия создана: {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания резервной копии: {e}")
            return False
    
    def migrate_to_scaled(self, create_backup: bool = True) -> bool:
        """Миграция в scaled режим"""
        try:
            logger.info("Начинаем миграцию в scaled режим")
            
            # Создаем резервную копию если нужно
            if create_backup:
                if not self.create_backup():
                    logger.warning("Не удалось создать резервную копию, продолжаем миграцию")
            
            # Создаем структуру директорий
            self._create_scaled_structure()
            
            # Определяем источник данных
            source = self.data_adapter.detect_data_source()
            logger.info(f"Обнаружен источник данных: {source}")
            
            # Мигрируем данные
            success = self.data_adapter.migrate_to_scaled(source)
            
            if success:
                logger.info("Миграция в scaled режим завершена успешно")
                self.migration_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "operation": "migrate_to_scaled",
                    "source": source,
                    "status": "success"
                })
                return True
            else:
                logger.error("Миграция в scaled режим не удалась")
                self.migration_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "operation": "migrate_to_scaled",
                    "source": source,
                    "status": "failed"
                })
                return False
                
        except Exception as e:
            logger.error(f"Ошибка миграции: {e}")
            self.migration_log.append({
                "timestamp": datetime.now().isoformat(),
                "operation": "migrate_to_scaled",
                "error": str(e),
                "status": "error"
            })
            return False
    
    def _create_scaled_structure(self):
        """Создание структуры директорий для scaled режима"""
        directories = [
            "data/raw",
            "data/processed", 
            "data/duckdb",
            "app/api",
            "app/etl",
            "app/llm",
            "app/clustering",
            "app/sql",
            "app/dashboard",
            "app/utils",
            "logs"
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        logger.info("Структура директорий для scaled режима создана")
    
    def validate_migration(self, mode: str = "scaled") -> Dict[str, Any]:
        """Валидация миграции"""
        validation_result = {
            "mode": mode,
            "timestamp": datetime.now().isoformat(),
            "checks": {},
            "overall_status": "unknown"
        }
        
        if mode == "scaled":
            # Проверяем наличие scaled данных
            checks = {
                "mentions_db_exists": Path("data/duckdb/mentions.duckdb").exists(),
                "processed_data_exists": Path("data/processed/dialogs.parquet").exists(),
                "app_structure_exists": Path("app").exists(),
                "sql_files_exist": Path("app/sql/summary.sql").exists()
            }
            
            validation_result["checks"] = checks
            
            # Проверяем качество данных
            try:
                from quality.unified_quality import quality_checker
                quality_report = quality_checker.get_quality_report()
                validation_result["quality_report"] = quality_report
            except Exception as e:
                validation_result["quality_error"] = str(e)
            
            # Определяем общий статус
            if all(checks.values()):
                validation_result["overall_status"] = "success"
            else:
                validation_result["overall_status"] = "partial"
        
        return validation_result
    
    def rollback_migration(self, backup_dir: str) -> bool:
        """Откат миграции к предыдущему состоянию"""
        try:
            backup_path = Path(backup_dir)
            if not backup_path.exists():
                logger.error(f"Директория резервной копии не найдена: {backup_dir}")
                return False
            
            # Восстанавливаем данные из резервной копии
            dirs_to_restore = [
                "data/warehouse",
                "data/duckdb",
                "artifacts", 
                "reports"
            ]
            
            for dir_path in dirs_to_restore:
                src = backup_path / dir_path
                dst = Path(dir_path)
                
                if src.exists():
                    if dst.exists():
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
            
            logger.info(f"Откат миграции выполнен из {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отката миграции: {e}")
            return False
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Получение статуса миграции"""
        current_state = self.detect_current_state()
        
        return {
            "current_state": current_state,
            "migration_log": self.migration_log,
            "last_migration": self.migration_log[-1] if self.migration_log else None,
            "recommendations": self._get_migration_recommendations(current_state)
        }
    
    def _get_migration_recommendations(self, current_state: Dict[str, Any]) -> List[str]:
        """Получение рекомендаций по миграции"""
        recommendations = []
        
        if current_state["migration_needed"]:
            recommendations.append("Рекомендуется миграция в scaled режим для лучшей производительности")
        
        if "legacy" in current_state["available_modes"] and "scaled" not in current_state["available_modes"]:
            recommendations.append("Legacy данные можно мигрировать в scaled формат")
        
        if len(current_state["available_modes"]) > 1:
            recommendations.append("Обнаружены данные в нескольких форматах, рекомендуется консолидация")
        
        return recommendations

# CLI интерфейс для миграции
def main():
    """Главная функция для CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migration Tools")
    parser.add_argument("--action", choices=["detect", "plan", "migrate", "validate", "rollback", "status"], 
                       required=True, help="Действие для выполнения")
    parser.add_argument("--target-mode", default="scaled", help="Целевой режим для миграции")
    parser.add_argument("--backup-dir", help="Директория резервной копии для отката")
    parser.add_argument("--no-backup", action="store_true", help="Не создавать резервную копию")
    
    args = parser.parse_args()
    
    manager = MigrationManager()
    
    if args.action == "detect":
        state = manager.detect_current_state()
        print(json.dumps(state, indent=2, ensure_ascii=False))
    
    elif args.action == "plan":
        plan = manager.plan_migration(args.target_mode)
        print(json.dumps(plan, indent=2, ensure_ascii=False))
    
    elif args.action == "migrate":
        success = manager.migrate_to_scaled(not args.no_backup)
        print(f"Миграция {'успешна' if success else 'неудачна'}")
    
    elif args.action == "validate":
        result = manager.validate_migration(args.target_mode)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    elif args.action == "rollback":
        if not args.backup_dir:
            print("Ошибка: необходимо указать --backup-dir для отката")
            return
        success = manager.rollback_migration(args.backup_dir)
        print(f"Откат {'успешен' if success else 'неудачен'}")
    
    elif args.action == "status":
        status = manager.get_migration_status()
        print(json.dumps(status, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
