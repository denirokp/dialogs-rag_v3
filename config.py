from dataclasses import dataclass
from typing import Optional, List

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

settings = Settings()
