from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Settings:
    # Данные
    xlsx_path: str = "data/dialogs.xlsx"
    sheet_names: Optional[List[str]] = None  # None = все листы

    # Колонки в Excel
    col_id: str = "Id звонка"
    col_text: str = "Текст транскрибации"

    # Метки ролей (внутри текста)
    client_label: str = "клиент"
    operator_label: str = "оператор"

    # Окна контекста вокруг клиентской реплики
    prev_turns: int = 2
    next_turns: int = 2

    # Векторный индекс (Chroma)
    chroma_path: str = "chroma_db"
    collection: str = "dialogs_windows"

    # Эмбеддинги (локально)
    embed_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    # Ретривер
    top_k: int = 8  # сколько окон переправляем в LLM

    # LLM (OpenAI) — можно отключить
    use_openai: bool = True
    openai_model: str = "gpt-4o-mini"
    temperature: float = 0.1

settings = Settings()
