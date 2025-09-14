#!/usr/bin/env python3
"""
Data Adapters - Адаптеры для конвертации между разными форматами данных
Обеспечивает совместимость между legacy, pipeline, enhanced и scaled системами
"""

import pandas as pd
import polars as pl
import duckdb
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAdapter:
    """Базовый класс для адаптеров данных"""
    
    def __init__(self):
        self.supported_formats = ["pandas", "polars", "duckdb", "json", "parquet"]
    
    def convert(self, data: Any, from_format: str, to_format: str) -> Any:
        """Универсальная конвертация данных"""
        if from_format == to_format:
            return data
        
        converter_name = f"_{from_format}_to_{to_format}"
        if hasattr(self, converter_name):
            return getattr(self, converter_name)(data)
        else:
            raise ValueError(f"Конвертация из {from_format} в {to_format} не поддерживается")

class LegacyToScaledAdapter(DataAdapter):
    """Адаптер для конвертации из legacy в scaled формат"""
    
    def __init__(self):
        super().__init__()
        self.legacy_db_path = "data/rag.duckdb"
        self.scaled_db_path = "data/duckdb/mentions.duckdb"
    
    def migrate_utterances_to_mentions(self) -> bool:
        """Миграция utterances в mentions для scaled системы"""
        try:
            # Подключаемся к legacy БД
            legacy_con = duckdb.connect(self.legacy_db_path, read_only=True)
            
            # Читаем utterances
            utterances_df = legacy_con.execute("SELECT * FROM utterances").df()
            legacy_con.close()
            
            if utterances_df.empty:
                logger.warning("Нет данных utterances для миграции")
                return False
            
            # Конвертируем в Polars
            df = pl.from_pandas(utterances_df)
            
            # Приводим к каноническому формату scaled системы
            df = df.rename({
                "batch_id": "dialog_id",  # Используем batch_id как dialog_id
            })
            
            # Фильтруем только client реплики
            df = df.filter(pl.col("role") == "client")
            
            # Создаем mentions с базовыми полями
            mentions_df = df.select([
                pl.col("dialog_id"),
                pl.col("turn_id"),
                pl.lit("прочее").alias("theme"),
                pl.lit("").alias("subtheme"),
                pl.col("text").alias("text_quote"),
                pl.lit(0.5).alias("confidence")
            ])
            
            # Создаем scaled БД
            Path(self.scaled_db_path).parent.mkdir(parents=True, exist_ok=True)
            scaled_con = duckdb.connect(self.scaled_db_path)
            
            # Сохраняем mentions
            scaled_con.register("mentions_df", mentions_df.to_pandas())
            scaled_con.execute("CREATE TABLE mentions AS SELECT * FROM mentions_df")
            scaled_con.unregister("mentions_df")
            scaled_con.close()
            
            logger.info(f"Мигрировано {len(mentions_df)} записей из legacy в scaled")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции utterances: {e}")
            return False
    
    def migrate_mentions_to_scaled(self) -> bool:
        """Миграция mentions из legacy в scaled формат"""
        try:
            # Подключаемся к legacy БД
            legacy_con = duckdb.connect(self.legacy_db_path, read_only=True)
            
            # Читаем mentions_final
            mentions_df = legacy_con.execute("SELECT * FROM mentions_final").df()
            legacy_con.close()
            
            if mentions_df.empty:
                logger.warning("Нет данных mentions_final для миграции")
                return False
            
            # Конвертируем в Polars
            df = pl.from_pandas(mentions_df)
            
            # Приводим к формату scaled системы
            scaled_df = df.select([
                pl.col("dialog_id"),
                pl.col("turn_id"),
                pl.col("theme"),
                pl.col("subtheme"),
                pl.col("text_quote"),
                pl.col("confidence")
            ])
            
            # Создаем scaled БД
            Path(self.scaled_db_path).parent.mkdir(parents=True, exist_ok=True)
            scaled_con = duckdb.connect(self.scaled_db_path)
            
            # Сохраняем mentions
            scaled_con.register("mentions_df", scaled_df.to_pandas())
            scaled_con.execute("CREATE TABLE mentions AS SELECT * FROM mentions_df")
            scaled_con.unregister("mentions_df")
            scaled_con.close()
            
            logger.info(f"Мигрировано {len(scaled_df)} записей из legacy в scaled")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции mentions: {e}")
            return False

class PipelineToScaledAdapter(DataAdapter):
    """Адаптер для конвертации из pipeline в scaled формат"""
    
    def __init__(self):
        super().__init__()
        self.artifacts_dir = Path("artifacts")
        self.scaled_db_path = "data/duckdb/mentions.duckdb"
    
    def migrate_extractions_to_mentions(self) -> bool:
        """Миграция извлечений из pipeline в scaled формат"""
        try:
            # Читаем stage2_extracted.jsonl
            extractions_path = self.artifacts_dir / "stage2_extracted.jsonl"
            if not extractions_path.exists():
                logger.warning("Файл stage2_extracted.jsonl не найден")
                return False
            
            mentions = []
            with open(extractions_path, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    # Конвертируем в формат scaled
                    mention = {
                        "dialog_id": data.get("dialog_id", ""),
                        "turn_id": data.get("turn_id", 0),
                        "theme": data.get("theme", "прочее"),
                        "subtheme": data.get("subtheme", ""),
                        "text_quote": data.get("text_quote", ""),
                        "confidence": data.get("confidence", 0.5)
                    }
                    mentions.append(mention)
            
            if not mentions:
                logger.warning("Нет данных для миграции")
                return False
            
            # Конвертируем в Polars DataFrame
            df = pl.DataFrame(mentions)
            
            # Создаем scaled БД
            Path(self.scaled_db_path).parent.mkdir(parents=True, exist_ok=True)
            scaled_con = duckdb.connect(self.scaled_db_path)
            
            # Сохраняем mentions
            scaled_con.register("mentions_df", df.to_pandas())
            scaled_con.execute("CREATE TABLE mentions AS SELECT * FROM mentions_df")
            scaled_con.unregister("mentions_df")
            scaled_con.close()
            
            logger.info(f"Мигрировано {len(mentions)} записей из pipeline в scaled")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции извлечений: {e}")
            return False

class EnhancedToScaledAdapter(DataAdapter):
    """Адаптер для конвертации из enhanced в scaled формат"""
    
    def __init__(self):
        super().__init__()
        self.artifacts_dir = Path("artifacts")
        self.scaled_db_path = "data/duckdb/mentions.duckdb"
    
    def migrate_enhanced_to_mentions(self) -> bool:
        """Миграция enhanced данных в scaled формат"""
        try:
            # Читаем enhanced извлечения
            enhanced_path = self.artifacts_dir / "stage2_extracted_enhanced.jsonl"
            if not enhanced_path.exists():
                logger.warning("Файл stage2_extracted_enhanced.jsonl не найден")
                return False
            
            mentions = []
            with open(enhanced_path, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    # Конвертируем в формат scaled
                    mention = {
                        "dialog_id": data.get("dialog_id", ""),
                        "turn_id": data.get("turn_id", 0),
                        "theme": data.get("theme", "прочее"),
                        "subtheme": data.get("subtheme", ""),
                        "text_quote": data.get("text_quote", ""),
                        "confidence": data.get("confidence", 0.5)
                    }
                    mentions.append(mention)
            
            if not mentions:
                logger.warning("Нет данных для миграции")
                return False
            
            # Конвертируем в Polars DataFrame
            df = pl.DataFrame(mentions)
            
            # Создаем scaled БД
            Path(self.scaled_db_path).parent.mkdir(parents=True, exist_ok=True)
            scaled_con = duckdb.connect(self.scaled_db_path)
            
            # Сохраняем mentions
            scaled_con.register("mentions_df", df.to_pandas())
            scaled_con.execute("CREATE TABLE mentions AS SELECT * FROM mentions_df")
            scaled_con.unregister("mentions_df")
            scaled_con.close()
            
            logger.info(f"Мигрировано {len(mentions)} записей из enhanced в scaled")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции enhanced данных: {e}")
            return False

class FormatConverter(DataAdapter):
    """Конвертер между разными форматами данных"""
    
    def pandas_to_polars(self, df: pd.DataFrame) -> pl.DataFrame:
        """Конвертация из pandas в polars"""
        return pl.from_pandas(df)
    
    def polars_to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        """Конвертация из polars в pandas"""
        return df.to_pandas()
    
    def pandas_to_duckdb(self, df: pd.DataFrame, table_name: str, db_path: str) -> bool:
        """Сохранение pandas DataFrame в DuckDB"""
        try:
            con = duckdb.connect(db_path)
            con.register("df", df)
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            con.unregister("df")
            con.close()
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения в DuckDB: {e}")
            return False
    
    def polars_to_duckdb(self, df: pl.DataFrame, table_name: str, db_path: str) -> bool:
        """Сохранение polars DataFrame в DuckDB"""
        return self.pandas_to_duckdb(df.to_pandas(), table_name, db_path)
    
    def duckdb_to_polars(self, query: str, db_path: str) -> pl.DataFrame:
        """Чтение из DuckDB в polars"""
        try:
            con = duckdb.connect(db_path, read_only=True)
            df = con.execute(query).df()
            con.close()
            return pl.from_pandas(df)
        except Exception as e:
            logger.error(f"Ошибка чтения из DuckDB: {e}")
            return pl.DataFrame()
    
    def json_to_polars(self, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Конвертация JSON в polars"""
        return pl.DataFrame(data)
    
    def polars_to_json(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Конвертация polars в JSON"""
        return df.to_dicts()

class UnifiedDataAdapter:
    """Единый адаптер данных для всех систем"""
    
    def __init__(self):
        self.legacy_adapter = LegacyToScaledAdapter()
        self.pipeline_adapter = PipelineToScaledAdapter()
        self.enhanced_adapter = EnhancedToScaledAdapter()
        self.converter = FormatConverter()
    
    def detect_data_source(self) -> str:
        """Автоматическое определение источника данных"""
        if Path("data/duckdb/mentions.duckdb").exists():
            return "scaled"
        elif Path("artifacts/stage4_5_semantic_enrichment.json").exists():
            return "enhanced"
        elif Path("artifacts/stage4_clusters.json").exists():
            return "pipeline"
        elif Path("data/warehouse/mentions_final.parquet").exists():
            return "legacy"
        else:
            return "none"
    
    def migrate_to_scaled(self, source: str = "auto") -> bool:
        """Миграция данных в scaled формат"""
        if source == "auto":
            source = self.detect_data_source()
        
        logger.info(f"Миграция данных из {source} в scaled формат")
        
        if source == "legacy":
            return self.legacy_adapter.migrate_mentions_to_scaled()
        elif source == "pipeline":
            return self.pipeline_adapter.migrate_extractions_to_mentions()
        elif source == "enhanced":
            return self.enhanced_adapter.migrate_enhanced_to_mentions()
        elif source == "scaled":
            logger.info("Данные уже в scaled формате")
            return True
        else:
            logger.error(f"Неизвестный источник данных: {source}")
            return False
    
    def convert_format(self, data: Any, from_format: str, to_format: str) -> Any:
        """Универсальная конвертация форматов"""
        return self.converter.convert(data, from_format, to_format)
    
    def get_unified_data(self, mode: str = "auto") -> pl.DataFrame:
        """Получение унифицированных данных"""
        if mode == "auto":
            mode = self.detect_data_source()
        
        if mode == "scaled":
            return self.converter.duckdb_to_polars("SELECT * FROM mentions", "data/duckdb/mentions.duckdb")
        elif mode == "legacy":
            return self.converter.duckdb_to_polars("SELECT * FROM mentions_final", "data/rag.duckdb")
        else:
            # Для pipeline и enhanced нужно сначала мигрировать
            if self.migrate_to_scaled(mode):
                return self.converter.duckdb_to_polars("SELECT * FROM mentions", "data/duckdb/mentions.duckdb")
            else:
                return pl.DataFrame()

# Глобальный экземпляр для использования в других модулях
data_adapter = UnifiedDataAdapter()

if __name__ == "__main__":
    # Тестирование адаптеров
    adapter = UnifiedDataAdapter()
    
    # Определяем источник данных
    source = adapter.detect_data_source()
    print(f"Обнаружен источник данных: {source}")
    
    # Мигрируем в scaled формат
    success = adapter.migrate_to_scaled(source)
    print(f"Миграция {'успешна' if success else 'неудачна'}")
    
    # Получаем унифицированные данные
    data = adapter.get_unified_data()
    print(f"Получено {len(data)} записей")
