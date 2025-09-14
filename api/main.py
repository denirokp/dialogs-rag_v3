import os, duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

DB = os.getenv("DUCKDB_PATH", "data/rag.duckdb")
REQUIRE_PASS = os.getenv("REQUIRE_QUALITY_PASS", "true").lower() == "true"

app = FastAPI(title="Dialogs RAG API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

def con():
    return duckdb.connect(DB, read_only=True)

# helper: проверить quality.passed
async def ensure_passed():
    if not REQUIRE_PASS: return
    with con() as c:
        q = c.execute("SELECT * FROM quality ORDER BY ROW_NUMBER() OVER () DESC LIMIT 1").fetch_df()
    if q.empty or not bool(q.iloc[0]["passed"]):
        raise HTTPException(status_code=409, detail="Quality gate not passed; data not published")

@app.get("/api/quality")
async def quality():
    with con() as c:
        q = c.execute("SELECT * FROM quality ORDER BY ROW_NUMBER() OVER () DESC LIMIT 1").fetch_df()
    if q.empty: raise HTTPException(404, "quality not found")
    return q.iloc[0].to_dict()

@app.get("/api/summary_themes")
async def summary_themes():
    await ensure_passed()
    with con() as c:
        tot = c.execute("SELECT MAX(totalDialogs) FROM quality").fetchone()[0] or 0
        rows = c.execute("SELECT theme, dialogs, mentions, share FROM summary_themes ORDER BY dialogs DESC").fetch_df()
    return {"totals": {"dialogs": int(tot), "mentions": int(rows["mentions"].sum() if not rows.empty else 0)}, "themes": rows.to_dict("records")}

@app.get("/api/summary_subthemes")
async def summary_subthemes(theme: str | None = None):
    await ensure_passed()
    sql = "SELECT theme, subtheme, dialogs, mentions, share FROM summary_subthemes"
    args = []
    if theme:
        sql += " WHERE theme = ?"; args.append(theme)
    sql += " ORDER BY dialogs DESC LIMIT 200"
    with con() as c:
        rows = c.execute(sql, args).fetch_df()
    return {"items": rows.to_dict("records")}

@app.get("/api/index_quotes")
async def index_quotes(theme: str | None = None, subtheme: str | None = None, page: int = 1, page_size: int = 50):
    await ensure_passed()
    where, args = [], []
    if theme: where.append("theme = ?"); args.append(theme)
    if subtheme: where.append("subtheme = ?"); args.append(subtheme)
    sql = "SELECT dialog_id, turn_id, theme, subtheme, label_type, text_quote, confidence FROM index_quotes"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY dialog_id, turn_id LIMIT ? OFFSET ?"; args += [page_size, (page-1)*page_size]
    with con() as c:
        rows = c.execute(sql, args).fetch_df()
    next_page = None if rows.empty or len(rows) < page_size else page + 1
    return {"items": rows.to_dict("records"), "next_page": next_page}

@app.get("/api/cooccurrence")
async def cooccurrence(top: int = 50):
    await ensure_passed()
    with con() as c:
        rows = c.execute("SELECT themeA, themeB, weight FROM cooccur ORDER BY weight DESC LIMIT ?", [top]).fetch_df()
    return {"items": rows.to_dict("records")}

