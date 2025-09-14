from dataclasses import dataclass
from typing import Optional, List
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    # Данные
    xlsx_path: str = "data/dialogs.xlsx"
    sheet_names: Optional[List[str]] = None  # None = все листы

    # Колонки в Excel
    col_id: str = "ID звонка"
    col_text: str = "Текст транскрибации"

    # Метки ролей (внутри текста)
    client_label: str = "клиент"
    operator_label: str = "оператор"

    # Окна контекста вокруг клиентской реплики (оптимизировано для больших данных)
    prev_turns: int = 1  # уменьшено для ускорения
    next_turns: int = 1  # уменьшено для ускорения

    # Векторный индекс (Chroma)
    chroma_path: str = "chroma_db"
    collection: str = "dialogs_windows"

    # Эмбеддинги (локально) - быстрая модель для больших данных
    embed_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    # Ретривер (оптимизировано для 2000+ диалогов)
    top_k: int = 5  # уменьшено для ускорения обработки

    # LLM (OpenAI) — можно отключить
    use_openai: bool = True
    openai_model: str = "gpt-4o-mini"
    temperature: float = 0.1
    
    # OpenAI API
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    
    # Переключатель качества моделей
    quality: str = os.getenv("RAG_QUALITY", "cheap")  # По умолчанию cheap для стабильности
    
    @property
    def model_extract(self) -> str:
        if self.quality == "cheap":
            return "gpt-4o-mini"
        elif self.quality == "max":
            return "o3-mini"
        else:  # balanced
            return "gpt-4o-mini"  # Fallback на стабильную модель
    
    @property
    def model_label(self) -> str:
        if self.quality == "cheap":
            return "gpt-4o-mini"
        elif self.quality == "max":
            return "o3-mini"
        else:  # balanced
            return "gpt-4o-mini"
    
    @property
    def model_summary(self) -> str:
        if self.quality == "cheap":
            return "gpt-4o-mini"
        elif self.quality == "max":
            return "gpt-4o"
        else:  # balanced
            return "gpt-4o-mini"
    
    # Stage 1: детекция доставки
    delivery_conf_threshold: float = 0.5  # p_deliv порог серой зоны
    
    # Батчи/параллель
    batch_size: int = 100
    max_concurrency: int = 8
    retry_backoff_sec: float = 2.0
    max_retries: int = 3
    
    # Отчёты
    output_dir: str = "reports"
    show_ids_in_card_limit: int = 20  # сколько ID показывать в карточке, остальное в приложение
    
    # Логирование
    log_level: str = "INFO"
    log_file: str = "logs/analysis.log"
    
    # Кэширование
    cache_enabled: bool = True
    cache_ttl: int = 3600  # 1 час
    
    # Веб-интерфейс
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    dashboard_port: int = 8501
    
    # Новые параметры
    # Выборка/валидатор
    min_call_secs: int = 180
    platform_share_threshold: float = 0.6  # кластер считается "платформенным", если <60% доставочных диалогов
    
    # Отчёт
    warn_small_D_threshold: int = 50
    rare_threshold: int = 3
    require_client_role_for_ideas: bool = True
    
    # Текст/язык
    sentiment_labels: tuple = ("раздражение", "нейтрально", "сомнение", "позитив")

settings = Settings()
