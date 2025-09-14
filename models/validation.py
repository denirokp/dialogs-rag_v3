#!/usr/bin/env python3
"""
Модели валидации данных для Dialogs RAG
Обеспечивает типизацию и валидацию всех структур данных
"""
from pydantic import BaseModel, validator, Field
from typing import List, Optional, Dict, Any
from enum import Enum
import re

class SentimentType(str, Enum):
    """Типы тональности"""
    IRRITATION = "раздражение"
    NEUTRAL = "нейтрально"
    DOUBT = "сомнение"
    POSITIVE = "позитив"

class ClientType(str, Enum):
    """Типы клиентов"""
    PRIVATE = "частный"
    CORPORATE = "корпоративный"
    WHOLESALE = "оптовый"

class DeliveryType(str, Enum):
    """Типы доставки"""
    PICKUP = "самовывоз"
    SELLER = "продавцом"
    PVZ = "ПВЗ"
    COURIER = "курьерская"
    LARGE = "крупногабаритная"
    POSTAL = "почтовая"
    MARKETPLACE = "маркетплейс"
    EXPRESS = "экспресс"
    INTERREGIONAL = "межрегиональная"
    PAYMENT = "оплата"
    RETURN = "возврат"
    LOGISTICS = "логистика"
    PACKAGING = "упаковка"
    ADDRESS = "адрес"
    PROBLEMS = "проблемы"

class DeliveryDetection(BaseModel):
    """Результат детекции доставки"""
    dialog_id: str = Field(..., min_length=1, max_length=100)
    delivery_discussed: bool
    p_deliv: float = Field(default=0.0, ge=0.0, le=1.0)
    
    @validator('dialog_id')
    def validate_dialog_id(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('ID диалога может содержать только буквы, цифры, _ и -')
        return v


class Citation(BaseModel):
    """Цитата из диалога"""
    quote: str = Field(..., min_length=1, max_length=1000)
    speaker: str = Field(default="Клиент", regex="^(Клиент|Оператор)$")
    
    @validator('quote')
    def validate_quote(cls, v):
        if not v.strip():
            raise ValueError('Цитата не может быть пустой')
        return v.strip()


class DialogExtraction(BaseModel):
    """Извлечение сущностей из диалога"""
    dialog_id: str = Field(..., min_length=1, max_length=100)
    delivery_discussed: bool
    delivery_types: List[str] = Field(default_factory=list)
    barriers: List[str] = Field(default_factory=list)
    ideas: List[str] = Field(default_factory=list)
    signals: List[str] = Field(default_factory=list)
    citations: List[Citation] = Field(default_factory=list)
    region: str = Field(default="")
    segment: str = Field(default="")
    product_category: str = Field(default="")
    sentiment: str = Field(default="")
    extras: Dict[str, Any] = Field(default_factory=dict)


class ClusterVariant(BaseModel):
    """Вариант формулировки в кластере"""
    text: str
    count: int


class ClusterSlices(BaseModel):
    """Срезы данных кластера"""
    regions: Dict[str, int] = Field(default_factory=dict)
    segments: Dict[str, int] = Field(default_factory=dict)
    product_categories: Dict[str, int] = Field(default_factory=dict)
    delivery_types: Dict[str, int] = Field(default_factory=dict)
    sentiment: Dict[str, int] = Field(default_factory=dict)


class Cluster(BaseModel):
    """Кластер похожих формулировок"""
    name: str
    variants: List[ClusterVariant]
    dialog_ids: List[str]
    slices: ClusterSlices


class ClustersData(BaseModel):
    """Данные кластеризации"""
    barriers: List[Cluster] = Field(default_factory=list)
    ideas: List[Cluster] = Field(default_factory=list)
    signals: List[Cluster] = Field(default_factory=list)


class AggregateResults(BaseModel):
    """Результаты агрегации"""
    total_dialogs: int
    delivery_dialogs: int
    delivery_percentage: float
    clusters: ClustersData
    quality_metrics: Dict[str, Any] = Field(default_factory=dict)


class DialogAnalysisResult(BaseModel):
    """Результат анализа диалога"""
    dialog_id: str = Field(..., min_length=1, max_length=100)
    delivery_discussed: bool
    delivery_types: List[str] = Field(default_factory=list)
    barriers: List[str] = Field(default_factory=list)
    ideas: List[str] = Field(default_factory=list)
    signals: List[str] = Field(default_factory=list)
    region: Optional[str] = Field(None, max_length=100)
    segment: Optional[str] = Field(None, max_length=50)
    product_category: Optional[str] = Field(None, max_length=100)
    sentiment: Optional[SentimentType] = None
    client_type: Optional[ClientType] = None
    payment_method: Optional[str] = Field(None, max_length=50)
    return_issue: Optional[str] = Field(None, max_length=200)
    self_check: str = Field(default="", max_length=500)
    citations: List[Citation] = Field(default_factory=list)
    
    @validator('dialog_id')
    def validate_dialog_id(cls, v):
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('ID диалога может содержать только буквы, цифры, _ и -')
        return v
    
    @validator('delivery_types')
    def validate_delivery_types(cls, v):
        valid_types = [dt.value for dt in DeliveryType]
        for dt in v:
            if not dt.startswith('другое:') and dt not in valid_types:
                raise ValueError(f'Неверный тип доставки: {dt}. Допустимые: {valid_types}')
        return v
    
    @validator('barriers', 'ideas', 'signals')
    def validate_text_lists(cls, v):
        for item in v:
            if not item.strip():
                raise ValueError('Элементы списка не могут быть пустыми')
            if len(item) > 200:
                raise ValueError('Элементы списка не могут быть длиннее 200 символов')
        return [item.strip() for item in v]
    
    @validator('region', 'segment', 'product_category', 'payment_method', 'return_issue')
    def validate_optional_strings(cls, v):
        if v is not None and not v.strip():
            return None
        return v
    
    @validator('self_check')
    def validate_self_check(cls, v):
        if len(v) > 500:
            raise ValueError('Self-check не может быть длиннее 500 символов')
        return v
    
    @validator('citations')
    def validate_citations(cls, v):
        if len(v) > 10:
            raise ValueError('Не может быть больше 10 цитат')
        return v

class DialogTurn(BaseModel):
    """Реплика в диалоге"""
    role: str = Field(..., regex="^(клиент|оператор)$")
    text: str = Field(..., min_length=1, max_length=10000)
    
    @validator('text')
    def validate_text(cls, v):
        if not v.strip():
            raise ValueError('Текст реплики не может быть пустым')
        return v.strip()

class DialogWindow(BaseModel):
    """Окно контекста диалога"""
    id: str = Field(..., min_length=1, max_length=200)
    dialog_id: str = Field(..., min_length=1, max_length=100)
    turn_L: int = Field(..., ge=0)
    turn_R: int = Field(..., ge=0)
    context_full: str = Field(..., min_length=1, max_length=50000)
    context_client_only: str = Field(..., max_length=50000)
    
    @validator('turn_R')
    def validate_turn_range(cls, v, values):
        if 'turn_L' in values and v < values['turn_L']:
            raise ValueError('turn_R должен быть >= turn_L')
        return v
    
    @validator('context_full', 'context_client_only')
    def validate_context(cls, v):
        if not v.strip():
            raise ValueError('Контекст не может быть пустым')
        return v.strip()

class BatchAnalysisRequest(BaseModel):
    """Запрос на пакетный анализ"""
    dialog_ids: List[str] = Field(..., min_items=1, max_items=10000)
    query: Optional[str] = Field(None, max_length=500)
    top_k: int = Field(default=5, ge=1, le=20)
    use_openai: bool = Field(default=True)
    
    @validator('dialog_ids')
    def validate_dialog_ids(cls, v):
        if len(set(v)) != len(v):
            raise ValueError('ID диалогов должны быть уникальными')
        for dialog_id in v:
            if not re.match(r'^[a-zA-Z0-9_-]+$', dialog_id):
                raise ValueError(f'Неверный формат ID диалога: {dialog_id}')
        return v

class BatchAnalysisResponse(BaseModel):
    """Ответ на пакетный анализ"""
    task_id: str = Field(..., min_length=1)
    status: str = Field(..., regex="^(pending|processing|completed|failed)$")
    total_dialogs: int = Field(..., ge=0)
    processed: int = Field(default=0, ge=0)
    errors: int = Field(default=0, ge=0)
    progress_percent: float = Field(default=0.0, ge=0.0, le=100.0)
    results_file: Optional[str] = None
    error_message: Optional[str] = None
    created_at: float = Field(default_factory=lambda: __import__('time').time())
    
    @validator('processed')
    def validate_processed(cls, v, values):
        if 'total_dialogs' in values and v > values['total_dialogs']:
            raise ValueError('Обработано не может быть больше общего количества')
        return v
    
    @validator('errors')
    def validate_errors(cls, v, values):
        if 'processed' in values and v > values['processed']:
            raise ValueError('Ошибок не может быть больше обработанных')
        return v

class SystemHealth(BaseModel):
    """Состояние системы"""
    status: str = Field(..., regex="^(healthy|degraded|unhealthy)$")
    timestamp: float = Field(default_factory=lambda: __import__('time').time())
    cpu_percent: float = Field(..., ge=0.0, le=100.0)
    memory_percent: float = Field(..., ge=0.0, le=100.0)
    disk_usage_percent: float = Field(..., ge=0.0, le=100.0)
    active_connections: int = Field(..., ge=0)
    queue_size: int = Field(..., ge=0)
    
    @validator('status')
    def validate_status(cls, v):
        if v == "unhealthy" and any([
            # Добавить условия для unhealthy
        ]):
            pass
        return v

class AnalysisMetrics(BaseModel):
    """Метрики анализа"""
    dialog_id: str = Field(..., min_length=1)
    processing_time: float = Field(..., ge=0.0)
    success: bool
    error_message: Optional[str] = None
    tokens_used: Optional[int] = Field(None, ge=0)
    model_used: Optional[str] = None
    timestamp: float = Field(default_factory=lambda: __import__('time').time())
    
    @validator('processing_time')
    def validate_processing_time(cls, v):
        if v > 300:  # 5 минут
            raise ValueError('Время обработки не может превышать 5 минут')
        return v

def validate_dialog_text(text: str) -> bool:
    """Валидация текста диалога"""
    if not text or not text.strip():
        return False
    
    # Проверка на минимальную длину
    if len(text.strip()) < 20:  # Увеличиваем минимум
        return False
    
    # Проверка на наличие ролей
    if not re.search(r'(клиент|оператор)', text.lower()):
        return False
    
    return True

def validate_excel_file(file_path: str, required_columns: List[str]) -> Dict[str, Any]:
    """Валидация Excel файла"""
    import pandas as pd
    
    try:
        df = pd.read_excel(file_path)
        
        # Проверка наличия обязательных колонок
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return {
                "valid": False,
                "error": f"Отсутствуют обязательные колонки: {missing_columns}"
            }
        
        # Проверка на пустые строки
        empty_rows = df.isnull().all(axis=1).sum()
        
        # Проверка на дубликаты ID
        if 'ID звонка' in df.columns:
            duplicate_ids = df['ID звонка'].duplicated().sum()
        else:
            duplicate_ids = 0
        
        return {
            "valid": True,
            "total_rows": len(df),
            "empty_rows": empty_rows,
            "duplicate_ids": duplicate_ids,
            "columns": list(df.columns)
        }
        
    except Exception as e:
        return {
            "valid": False,
            "error": f"Ошибка чтения файла: {str(e)}"
        }
