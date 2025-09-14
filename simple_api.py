# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
import pandas as pd

app = FastAPI(title="DialogsRAG API", version="2.0")

# CORS на всякий случай
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ART = Path("artifacts")
RES_PATH = ART / "comprehensive_results.json"
STATS_PATH = ART / "statistics.json"

# ---------- helpers ----------
def read_mentions():
    if not RES_PATH.exists():
        return []
    js = json.loads(RES_PATH.read_text(encoding="utf-8"))
    return js.get("mentions", [])

# ---------- base endpoints ----------
@app.get("/api/statistics")
def statistics():
    if not STATS_PATH.exists():
        return {}
    return json.loads(STATS_PATH.read_text(encoding="utf-8"))

@app.get("/api/mentions")
def mentions(limit: int = 1000, offset: int = 0, label_type: str | None = None):
    arr = read_mentions()
    if label_type:
        arr = [m for m in arr if m.get("label_type") == label_type]
    return {
        "total": len(arr),
        "items": arr[offset: offset + limit]
    }

@app.get("/api/summary_themes")
def summary_themes():
    df = pd.DataFrame(read_mentions())
    if df.empty:
        return {"by_label": []}
    grp = (
        df.groupby(["label_type", "theme", "subtheme"])
          .agg(mentions=("text_quote", "count"), dialogs=("dialog_id", "nunique"))
          .reset_index()
    )
    return {"by_label": grp.to_dict(orient="records")}

@app.get("/api/problems")
def problems():
    df = pd.DataFrame(read_mentions())
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "problems"].to_dict(orient="records")}

@app.get("/api/ideas")
def ideas():
    df = pd.DataFrame(read_mentions())
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "ideas"].to_dict(orient="records")}

@app.get("/api/signals")
def signals():
    df = pd.DataFrame(read_mentions())
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "signals"].to_dict(orient="records")}

@app.post("/api/reload")
def reload_data():
    # Источник — файлы, поэтому reload по сути no-op, но оставим для совместимости
    return {"status": "ok"}

# ---------- consolidation endpoints ----------
@app.get("/api/problems_consolidated")
def problems_consolidated():
    ps = ART / "problems_summary.csv"
    sub = ART / "problems_subthemes.csv"
    if not ps.exists():
        return {"summary": [], "subthemes": []}
    ps_df = pd.read_csv(ps)
    sub_df = pd.read_csv(sub) if sub.exists() else pd.DataFrame()
    return {"summary": ps_df.to_dict(orient="records"),
            "subthemes": sub_df.to_dict(orient="records")}

@app.get("/api/problem_cards")
def problem_cards():
    p = ART / "problem_cards.jsonl"
    if not p.exists():
        return {"cards": []}
    cards = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {"cards": cards}

# uvicorn simple_api:app --port 8000