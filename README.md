# dialogs-rag

RAG-пайплайн для анализа диалогов (2 колонки в Excel: `Id звонка`, `Текст транскрибации`, роли в тексте: `Клиент:` / `Оператор:`).

## Быстрый старт
```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .\.venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env          # вставь OPENAI_API_KEY (опц.)
# положи свой dialogs.xlsx в data/

python ingest_excel.py
python query.py "Определи: обсуждалась ли доставка, типы, барьеры, идеи, сигналы."
python eval_batch.py
