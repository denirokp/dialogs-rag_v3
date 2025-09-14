# -*- coding: utf-8 -*-
"""
Консолидация проблем + авто-саммори (интеграция с файловой моделью артефактов).
Берёт: artifacts/comprehensive_results.json и problem_map.yaml
Пишет:
- artifacts/problems_summary.csv
- artifacts/problems_subthemes.csv
- artifacts/problems_mentions.csv
- artifacts/problem_cards.jsonl (+ .csv)
"""

import os, json, yaml
import pandas as pd, httpx
from pathlib import Path

ART_DIR = Path("artifacts")
RES_PATH = ART_DIR / "comprehensive_results.json"
MAP_PATH = "problem_map.yaml"

def load_mentions() -> pd.DataFrame:
    js = json.loads(RES_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(js.get("mentions", []))
    # нормализация для join
    if not df.empty:
        df["theme"] = df["theme"].fillna("")
        df["subtheme"] = df["subtheme"].fillna("")
    return df

def build_map() -> pd.DataFrame:
    mp = yaml.safe_load(open(MAP_PATH, "r", encoding="utf-8"))
    rows = []
    for p in mp["problems"]:
        for m in p["match"]:
            rows.append((p["id"], p["title"], m["theme"], m.get("subtheme", "")))
    return pd.DataFrame(rows, columns=["problem_id", "problem_title", "theme", "subtheme"])

def consolidate():
    m = load_mentions()
    if m.empty:
        raise SystemExit("Нет mentions в comprehensive_results.json")
    mp = build_map()
    merged = m.merge(mp, how="left", on=["theme", "subtheme"])
    merged["problem_id"] = merged["problem_id"].fillna("other_unmapped")
    merged["problem_title"] = merged["problem_title"].fillna("Прочее/не сконсолидировано")
    total_dialogs = merged["dialog_id"].nunique()

    agg = (
        merged.groupby(["problem_id", "problem_title"])
        .agg(dialogs=("dialog_id", "nunique"), mentions=("text_quote", "count"))
        .reset_index()
    )
    agg["share_dialogs_pct"] = (100 * agg["dialogs"] / max(1, total_dialogs)).round(1)
    agg["freq_per_1k"] = (1000 * agg["dialogs"] / max(1, total_dialogs)).round(1)
    agg["intensity_mpd"] = (agg["mentions"] / agg["dialogs"].clip(lower=1)).round(2)

    sub = (
        merged.groupby(["problem_id", "problem_title", "theme", "subtheme"])
        .agg(dialogs=("dialog_id", "nunique"), mentions=("text_quote", "count"))
        .reset_index()
        .sort_values(["problem_id", "dialogs"], ascending=[True, False])
    )

    ART_DIR.mkdir(parents=True, exist_ok=True)
    agg.to_csv(ART_DIR / "problems_summary.csv", index=False)
    sub.to_csv(ART_DIR / "problems_subthemes.csv", index=False)
    merged.to_csv(ART_DIR / "problems_mentions.csv", index=False)
    return merged, agg, sub, total_dialogs

# ---------- авто-саммори карточек ----------
SYSTEM = (
    "Ты пишешь краткий отчёт о ПРОБЛЕМЕ на основе статистики и цитат. "
    "Никаких советов и рекомендаций. Русский язык. Формат строго JSON."
)
USER_TMPL = """
ПРОБЛЕМА:
id: {problem_id}
title: {title}

СТАТИСТИКА:
dialogs: {dialogs}
mentions: {mentions}
share_dialogs_pct: {share:.1f}
intensity_mpd: {intensity:.2f}

ТОП-ПОДТЕМ:
{top_sub}

ПРИМЕРЫ ЦИТАТ (до 6):
{quotes}

СФОРМИРУЙ СТРОГИЙ JSON:
{{
  "problem_id": "{problem_id}",
  "title": "{title}",
  "definition": "1–2 строки — что именно жалуются клиенты (без советов)",
  "why_it_matters": "1–2 строки — чем это плохо для клиента/бизнеса (без решений)",
  "common_motifs": ["краткий мотив 1","краткий мотив 2","краткий мотив 3"],
  "co_occurs_with": [],
  "evidence_examples": [
    "{{dialog_id}}: {{короткая цитата…}}"
  ]
}}
"""

def summarize(merged: pd.DataFrame, agg: pd.DataFrame, sub: pd.DataFrame, model="gpt-4o-mini"):
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        print("[warn] OPENAI_API_KEY не задан — карточки проблем не будут сгенерированы.")
        return pd.DataFrame()
    client = httpx.Client(timeout=60)

    out = []
    for _, row in agg[agg["problem_id"] != "other_unmapped"].sort_values("dialogs", ascending=False).iterrows():
        pid, title = row["problem_id"], row["problem_title"]
        dfp = merged[merged["problem_id"] == pid]
        subp = sub[sub["problem_id"] == pid].head(5)
        top_sub = "\n".join([f"- {r.theme} / {r.subtheme} — dlg={r.dialogs} / m={r.mentions}" for r in subp.itertuples()])
        sample = dfp.sample(n=min(6, len(dfp)), random_state=42)
        quotes = "\n".join([
            f"- {r.dialog_id}: «{(r.text_quote[:217]+'…') if len(r.text_quote)>220 else r.text_quote}»"
            for r in sample.itertuples()
        ])

        user = USER_TMPL.format(
            problem_id=pid,
            title=title,
            dialogs=int(row["dialogs"]),
            mentions=int(row["mentions"]),
            share=float(row["share_dialogs_pct"]),
            intensity=float(row["intensity_mpd"]),
            top_sub=top_sub or "-",
            quotes=quotes or "-",
        )
        payload = {
            "model": model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user},
            ],
        }
        try:
            r = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
            )
            r.raise_for_status()
        except httpx.RequestError as e:
            print(f"[warn] OpenAI request failed for {pid}: {e}")
            continue
        try:
            content = r.json()["choices"][0]["message"]["content"]
            js = json.loads(content)
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            print(f"[warn] Failed to parse response for {pid}: {e}")
            continue
        out.append(js)

    if out:
        Path(ART_DIR / "problem_cards.jsonl").write_text(
            "\n".join([json.dumps(x, ensure_ascii=False) for x in out]), encoding="utf-8"
        )
        pd.DataFrame(out).to_csv(ART_DIR / "problem_cards.csv", index=False)
    return pd.DataFrame(out)

if __name__ == "__main__":
    merged, agg, sub, _ = consolidate()
    summarize(merged, agg, sub)
    print("[ok] artifacts/problems_*.{csv,jsonl} готовы")
