import json, re
import polars as pl
from app.llm.prompts import EXTRACTION_SYSTEM, EXTRACTION_USER_TMPL
from app.utils.taxonomy import load_taxonomy
from app.api.schemas import Mention

def format_window(turns: list[dict]) -> str:
    # В окно кладём только client-реплики: "[turn_id] text"
    lines = [f"[{t['turn_id']}] {t['text']}" for t in turns if t["role"] == "client"]
    return "\n".join(lines)

def extract_mentions_for_windows(llm, windows_df: pl.DataFrame, taxonomy_path: str) -> pl.DataFrame:
    tax = load_taxonomy(taxonomy_path)
    tax_str = json.dumps(tax, ensure_ascii=False)
    out = []
    for row in windows_df.iter_rows(named=True):
        dialog_id = row["dialog_id"]
        window = format_window(row["turns"])
        if not window.strip():
            continue
        user_prompt = EXTRACTION_USER_TMPL.format(taxonomy=tax_str, window=window)
        payload = llm.generate_json(EXTRACTION_SYSTEM, user_prompt)
        for m in payload:
            try:
                item = Mention(**m).model_dump()
                out.append(item)
            except Exception:
                # пропускаем невалидные
                continue
    return pl.DataFrame(out) if out else pl.DataFrame(schema={
        "dialog_id": pl.Utf8, "turn_id": pl.Int64, "theme": pl.Utf8,
        "subtheme": pl.Utf8, "text_quote": pl.Utf8, "confidence": pl.Float64
    })
