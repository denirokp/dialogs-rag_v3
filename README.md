# Dialogs RAG v2 — Quickstart

## 1) Установка
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
export OPENAI_API_KEY=...  # Windows: set OPENAI_API_KEY=...
```

## 2) Анализ и консолидация
```bash
python main.py
```
Артефакты появятся в `artifacts/`.

## 3) API (порт 8000)
```bash
uvicorn simple_api:app --port 8000 --reload
```

## 4) Дашборд (порт 8501)
```bash
streamlit run simple_dashboard.py --server.port 8501
```

## Примечания
- LLM возвращает **строго** `{ "mentions": [...] }`.
- Таксономия доменная (стынкуется с `problem_map.yaml`).
- DoD-метрики: `evidence_100`, `dedup_removed_pct`, `ambiguity_pct` в `artifacts/statistics.json`.
- Консолидация и карточки проблем: запускаются `consolidate_and_summarize.py` или из `main.py`.
