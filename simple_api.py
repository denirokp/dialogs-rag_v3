# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from typing import Any, Callable
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

_CACHE: dict[tuple[Path, str | None], tuple[float, Any]] = {}


def _get_cached(path: Path, loader: Callable[[], Any], key: str | None = None, default: Any = None) -> Any:
    """Return cached value for *path* using *loader* if file changed."""
    cache_key = (path, key)
    if not path.exists():
        _CACHE.pop(cache_key, None)
        return default
    mtime = path.stat().st_mtime
    entry = _CACHE.get(cache_key)
    if entry is None or entry[0] < mtime:
        _CACHE[cache_key] = (mtime, loader())
    return _CACHE[cache_key][1]


def clear_cache() -> None:
    _CACHE.clear()


# ---------- helpers ----------
def read_mentions():
    def loader():
        js = json.loads(RES_PATH.read_text(encoding="utf-8"))
        return js.get("mentions", [])

    return _get_cached(RES_PATH, loader, default=[])


def read_mentions_df() -> pd.DataFrame:
    return _get_cached(RES_PATH, lambda: pd.DataFrame(read_mentions()), key="df", default=pd.DataFrame())

# ---------- base endpoints ----------
@app.get("/api/statistics")
def statistics():
    return _get_cached(
        STATS_PATH,
        lambda: json.loads(STATS_PATH.read_text(encoding="utf-8")),
        default={},
    )

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
    df = read_mentions_df()
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
    df = read_mentions_df()
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "problems"].to_dict(orient="records")}

@app.get("/api/ideas")
def ideas():
    df = read_mentions_df()
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "ideas"].to_dict(orient="records")}

@app.get("/api/signals")
def signals():
    df = read_mentions_df()
    if df.empty:
        return {"items": []}
    return {"items": df[df["label_type"] == "signals"].to_dict(orient="records")}

@app.post("/api/reload")
def reload_data():
    clear_cache()
    return {"status": "ok"}

# ---------- consolidation endpoints ----------
@app.get("/api/problems_consolidated")
def problems_consolidated():
    ps = ART / "problems_summary.csv"
    sub = ART / "problems_subthemes.csv"
    ps_df = _get_cached(ps, lambda: pd.read_csv(ps), key="df", default=pd.DataFrame())
    if ps_df.empty:
        return {"summary": [], "subthemes": []}
    sub_df = _get_cached(sub, lambda: pd.read_csv(sub), key="df", default=pd.DataFrame())
    return {
        "summary": ps_df.to_dict(orient="records"),
        "subthemes": sub_df.to_dict(orient="records"),
    }

@app.get("/api/problem_cards")
def problem_cards():
    p = ART / "problem_cards.jsonl"
    cards = _get_cached(
        p,
        lambda: [
            json.loads(line)
            for line in p.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ],
        default=[],
    )
    return {"cards": cards}

# uvicorn simple_api:app --port 8000