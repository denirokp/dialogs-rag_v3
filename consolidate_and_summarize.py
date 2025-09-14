# -*- coding: utf-8 -*-
"""
Универсальная консолидация для problems/ideas/signals + авто-карточки.

Берёт:
  artifacts/comprehensive_results.json   ({"mentions":[...]})
  problem_map.yaml / idea_map.yaml / signal_map.yaml

Пишет (для каждого типа):
  artifacts/<kind>_summary.csv
  artifacts/<kind>_subthemes.csv
  artifacts/<kind>_mentions.csv
  artifacts/<singular>_cards.jsonl (и .csv) — если задан OPENAI_API_KEY

Где kind ∈ {"problems","ideas","signals"}, singular ∈ {"problem","idea","signal"}.

Колонки метрик (англ.) — UI их локализует:
  dialogs, mentions, share_dialogs_pct, freq_per_1k, intensity_mpd
"""

import os, json, yaml, httpx
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple

# ---- пути
ART = Path("artifacts")
ART.mkdir(parents=True, exist_ok=True)
RES_PATH = ART / "comprehensive_results.json"

MAPS = {
    "problems": "problem_map.yaml",
    "ideas":    "idea_map.yaml",
    "signals":  "signal_map.yaml",
}

# ---- utils
def _load_mentions() -> pd.DataFrame:
    if not RES_PATH.exists():
        raise SystemExit(f"Нет {RES_PATH} — сначала прогоняй анализатор диалогов.")
    js = json.loads(RES_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(js.get("mentions", []))
    if df.empty:
        raise SystemExit("В comprehensive_results.json нет mentions.")
    # нормализация
    for c in ("theme","subtheme","label_type","dialog_id","turn_id","text_quote"):
        if c not in df.columns:
            df[c] = ""
    df["theme"] = df["theme"].fillna("").astype(str)
    df["subtheme"] = df["subtheme"].fillna("").astype(str)
    df["label_type"] = df["label_type"].fillna("problems").astype(str)
    df["dialog_id"] = df["dialog_id"].astype(str)
    return df

def _load_map(path: str, top_key: str, id_key: str) -> pd.DataFrame:
    """Ожидается структура:
    <top_key>:
      - id: "<id>"
        title: "<человеческое имя>"
        match:
          - theme: "<таксономия theme>"
            subtheme: "<или пусто>"
          - ...
    """
    mp = yaml.safe_load(open(path, "r", encoding="utf-8"))
    rows = []
    for item in mp.get(top_key, []):
        _id = item["id"]
        _title = item["title"]
        for m in item.get("match", []):
            rows.append((_id, _title, m.get("theme",""), m.get("subtheme","")))
    df = pd.DataFrame(rows, columns=[id_key, f"{id_key[:-3]}title", "theme", "subtheme"])
    # пример: id_key="problem_id" -> title-колонка "problem_title"
    return df.rename(columns={f"{id_key[:-3]}title": f"{id_key[:-3]}_title"})

def _consolidate_one(
    m_all: pd.DataFrame, kind: str, map_path: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Возвращает (merged, summary, subthemes) и пишет CSV."""
    singular = {"problems":"problem","ideas":"idea","signals":"signal"}[kind]
    id_col = f"{singular}_id"
    title_col = f"{singular}_title"

    m = m_all[m_all["label_type"] == kind].copy()
    if m.empty:
        # создадим пустые файлы, чтобы UI не жаловался
        (ART / f"{kind}_summary.csv").write_text("", encoding="utf-8")
        (ART / f"{kind}_subthemes.csv").write_text("", encoding="utf-8")
        (ART / f"{kind}_mentions.csv").write_text("", encoding="utf-8")
        return m, m, m

    # карта соответствий
    if not Path(map_path).exists():
        raise SystemExit(f"Нет {map_path} — создай карту соответствий для {kind}.")
    mp = _load_map(map_path, top_key=kind, id_key=id_col)

    merged = m.merge(mp, how="left", on=["theme","subtheme"])
    merged[id_col] = merged[id_col].fillna("other_unmapped")
    merged[title_col] = merged[title_col].fillna("Прочее/не сконсолидировано")

    total_dialogs = merged["dialog_id"].nunique()

    # агрегаты по объектам
    agg = (
        merged.groupby([id_col, title_col])
        .agg(dialogs=("dialog_id","nunique"), mentions=("text_quote","count"))
        .reset_index()
    )
    agg["share_dialogs_pct"] = (100 * agg["dialogs"] / max(1, total_dialogs)).round(1)
    agg["freq_per_1k"] = (1000 * agg["dialogs"] / max(1, total_dialogs)).round(1)
    agg["intensity_mpd"] = (agg["mentions"] / agg["dialogs"].clip(lower=1)).round(2)

    # подтемы
    sub = (
        merged.groupby([id_col, title_col, "theme","subtheme"])
        .agg(dialogs=("dialog_id","nunique"), mentions=("text_quote","count"))
        .reset_index()
        .sort_values([id_col, "dialogs"], ascending=[True, False])
    )

    # write
    agg.to_csv(ART / f"{kind}_summary.csv", index=False)
    sub.to_csv(ART / f"{kind}_subthemes.csv", index=False)
    merged.to_csv(ART / f"{kind}_mentions.csv", index=False)

    return merged, agg, sub

# ---- авто-карточки (по желанию)
SYS_TMPL = (
    "Ты пишешь краткую карточку на русском для {label_ru} на основе статистики и цитат. "
    "Без советов. Ответ строго JSON."
)
USER_TMPL = """
{label_ru}:
id: {obj_id}
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
  "{id_col}": "{obj_id}",
  "title": "{title}",
  "definition": "1–2 строки — что именно говорят клиенты",
  "why_it_matters": "1–2 строки — почему это важно клиенту/бизнесу",
  "common_motifs": ["мотив 1","мотив 2","мотив 3"],
  "co_occurs_with": [],
  "evidence_examples": [
    "{{dialog_id}}: {{короткая цитата…}}"
  ]
}}
"""

def _summarize_cards(kind: str, merged: pd.DataFrame, agg: pd.DataFrame, sub: pd.DataFrame, model="gpt-4o-mini"):
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        print(f"[warn] OPENAI_API_KEY не задан — пропускаю карточки для {kind}.")
        return

    singular = {"problems":"problem","ideas":"idea","signals":"signal"}[kind]
    id_col = f"{singular}_id"
    title_col = f"{singular}_title"
    label_ru = {"problems":"ПРОБЛЕМА","ideas":"ИДЕЯ","signals":"СИГНАЛ"}[kind]
    out_path_jsonl = ART / f"{singular}_cards.jsonl"
    out_path_csv   = ART / f"{singular}_cards.csv"

    client = httpx.Client(timeout=60)
    out = []

    data_iter = agg[agg[id_col] != "other_unmapped"].sort_values("dialogs", ascending=False)
    for _, row in data_iter.iterrows():
        oid, title = row[id_col], row[title_col]
        dfp = merged[merged[id_col] == oid]
        subp = sub[sub[id_col] == oid].head(5)
        top_sub = "\n".join([f"- {r.theme} / {r.subtheme} — dlg={r.dialogs} / m={r.mentions}" for r in subp.itertuples()])
        sample = dfp.sample(n=min(6, len(dfp)), random_state=42)
        quotes = "\n".join([
            f"- {r.dialog_id}: «{(r.text_quote[:217]+'…') if len(r.text_quote)>220 else r.text_quote}»"
            for r in sample.itertuples()
        ])

        sys = SYS_TMPL.format(label_ru=label_ru)
        user = USER_TMPL.format(
            label_ru=label_ru,
            obj_id=oid,
            title=title.replace('"', "'"),
            dialogs=int(row["dialogs"]),
            mentions=int(row["mentions"]),
            share=float(row["share_dialogs_pct"]),
            intensity=float(row["intensity_mpd"]),
            top_sub=top_sub or "-",
            quotes=quotes or "-",
            id_col=id_col,
        )

        payload = {
            "model": model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": sys},
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
        out_path_jsonl.write_text("\n".join([json.dumps(x, ensure_ascii=False) for x in out]), encoding="utf-8")
        pd.DataFrame(out).to_csv(out_path_csv, index=False)
        print(f"[ok] карточки {kind} -> {out_path_jsonl}, {out_path_csv}")

def main():
    m_all = _load_mentions()
    # прогон по всем типам
    for kind, map_path in MAPS.items():
        merged, agg, sub = _consolidate_one(m_all, kind, map_path)
        print(f"[ok] {kind}: dialogs={agg['dialogs'].sum() if not agg.empty else 0}, rows={len(agg)}")
        if not agg.empty:
            _summarize_cards(kind, merged, agg, sub)

    print("[ok] artifacts/* для problems/ideas/signals готовы")

if __name__ == "__main__":
    main()