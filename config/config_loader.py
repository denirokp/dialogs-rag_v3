#!/usr/bin/env python3
"""
Configuration Loader - Загрузчик конфигурации для унифицированной системы
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigLoader:
    """Загрузчик конфигурации"""
    
    def __init__(self, config_path: str = "config/unified_config.yaml"):
        self.config_path = Path(config_path)
        self._config: Optional[Dict[str, Any]] = None
        self._load_config()
    
    def _load_config(self):
        """Загрузка конфигурации из файла"""
        try:
            if not self.config_path.exists():
                logger.warning(f"Конфигурационный файл {self.config_path} не найден, используем значения по умолчанию")
                self._config = self._get_default_config()
                return
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            
            # Заменяем переменные окружения
            self._substitute_env_vars()
            
            logger.info(f"Конфигурация загружена из {self.config_path}")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            self._config = self._get_default_config()
    
    def _substitute_env_vars(self):
        """Замена переменных окружения в конфигурации"""
        def replace_env_vars(obj):
            if isinstance(obj, dict):
                return {k: replace_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_env_vars(item) for item in obj]
            elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
                env_var = obj[2:-1]
                return os.getenv(env_var, obj)
            else:
                return obj
        
        self._config = replace_env_vars(self._config)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Конфигурация по умолчанию"""
        return {
            "general": {
                "version": "2.0.0",
                "default_mode": "auto",
                "log_level": "INFO",
                "debug": False
            },
            "data": {
                "input_dir": "data/input",
                "processed_dir": "data/processed",
                "warehouse_dir": "data/warehouse",
                "duckdb_dir": "data/duckdb",
                "artifacts_dir": "artifacts",
                "reports_dir": "reports"
            },
            "processing": {
                "windows": {
                    "max_tokens": 1800,
                    "whole_dialog_max": 8000
                },
                "batch_size": 100,
                "max_workers": 4
            },
            "quality": {
                "thresholds": {
                    "dedup_max": 0.01,
                    "coverage_other_max": 2.0,
                    "ambiguity_max": 40.0
                }
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8000
            },
            "dashboard": {
                "host": "0.0.0.0",
                "port": 8501
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Получение значения конфигурации по ключу"""
        if self._config is None:
            return default
        
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_mode_config(self, mode: str) -> Dict[str, Any]:
        """Получение конфигурации для конкретного режима"""
        mode_config = self.get(f"modes.{mode}", {})
        if not mode_config:
            logger.warning(f"Конфигурация для режима {mode} не найдена")
            return {}
        
        return mode_config
    
    def is_mode_enabled(self, mode: str) -> bool:
        """Проверка включен ли режим"""
        return self.get(f"modes.{mode}.enabled", False)
    
    def get_available_modes(self) -> list[str]:
        """Получение списка доступных режимов"""
        modes = self.get("modes", {})
        return [mode for mode, config in modes.items() if config.get("enabled", False)]
    
    def get_database_config(self, mode: str = "scaled") -> Dict[str, Any]:
        """Получение конфигурации базы данных для режима"""
        db_config = self.get("database.duckdb", {})
        
        if mode == "scaled":
            return {
                "path": db_config.get("scaled_path", "data/duckdb/mentions.duckdb"),
                "memory_limit": db_config.get("memory_limit", "1GB"),
                "threads": db_config.get("threads", 4)
            }
        else:
            return {
                "path": db_config.get("legacy_path", "data/rag.duckdb"),
                "memory_limit": db_config.get("memory_limit", "1GB"),
                "threads": db_config.get("threads", 4)
            }
    
    def get_llm_config(self, provider: str = "openai") -> Dict[str, Any]:
        """Получение конфигурации LLM"""
        return self.get(f"llm.{provider}", {})
    
    def get_quality_thresholds(self) -> Dict[str, Any]:
        """Получение порогов качества"""
        return self.get("quality.thresholds", {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Получение конфигурации обработки"""
        return self.get("processing", {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Получение конфигурации API"""
        return self.get("api", {})
    
    def get_dashboard_config(self) -> Dict[str, Any]:
        """Получение конфигурации дашборда"""
        return self.get("dashboard", {})
    
    def reload(self):
        """Перезагрузка конфигурации"""
        self._load_config()
    
    def save(self, config_path: str = None):
        """Сохранение конфигурации"""
        if config_path is None:
            config_path = self.config_path
        else:
            config_path = Path(config_path)
        
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True)
            logger.info(f"Конфигурация сохранена в {config_path}")
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации: {e}")

# Глобальный экземпляр для использования в других модулях
config = ConfigLoader()

def get_config() -> ConfigLoader:
    """Получение глобального экземпляра конфигурации"""
    return config

if __name__ == "__main__":
    # Тестирование загрузчика конфигурации
    loader = ConfigLoader()
    
    print("=== Конфигурация ===")
    print(f"Версия: {loader.get('general.version')}")
    print(f"Режим по умолчанию: {loader.get('general.default_mode')}")
    print(f"Доступные режимы: {loader.get_available_modes()}")
    
    print("\n=== Настройки обработки ===")
    processing = loader.get_processing_config()
    print(f"Максимум токенов в окне: {processing.get('windows', {}).get('max_tokens')}")
    print(f"Размер батча: {processing.get('batch_size')}")
    
    print("\n=== Настройки качества ===")
    quality = loader.get_quality_thresholds()
    print(f"Максимальный dedup: {quality.get('dedup_max')}")
    print(f"Максимальное покрытие 'прочее': {quality.get('coverage_other_max')}")
