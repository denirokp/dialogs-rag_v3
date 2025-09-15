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

import os, json, yaml, httpx, re
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

# --- НОРМАЛИЗАЦИЯ ДЛЯ JOIN ---
def _norm(s: str) -> str:
    if s is None: 
        return ""
    s = str(s)
    # унифицируем тире, пробелы, регистр
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s+", " ", s.strip())
    return s.lower()

# --- ЗАГРУЗКА МАПЫ ДЛЯ КОНКРЕТНОГО ТИПА ---
def build_map_for(kind: str) -> pd.DataFrame:
    # kind ∈ {"problems", "ideas", "signals"}
    file_by_kind = {
        "problems": "problem_map.yaml",
        "ideas":    "idea_map.yaml",
        "signals":  "signal_map.yaml",
    }
    root_key = {
        "problems": "problems",
        "ideas":    "ideas",
        "signals":  "signals",
    }[kind]
    singular = {"problems":"problem", "ideas":"idea", "signals":"signal"}[kind]

    path = file_by_kind[kind]
    if not Path(path).exists():
        # вернём пустую мапу — всё пойдёт в other_unmapped
        return pd.DataFrame(columns=[f"{singular}_id", f"{singular}_title", "theme_norm", "subtheme_norm"])

    mp = yaml.safe_load(open(path, "r", encoding="utf-8"))
    rows = []
    for item in mp.get(root_key, []):
        pid   = item.get("id", "").strip()
        title = item.get("title", "").strip()
        for m in item.get("match", []):
            rows.append((
                pid, title,
                _norm(m.get("theme", "")),
                _norm(m.get("subtheme", "")),
            ))
    df = pd.DataFrame(rows, columns=[f"{singular}_id", f"{singular}_title", "theme_norm", "subtheme_norm"])
    return df

# --- КОНСОЛИДАЦИЯ ПО ТИПУ ---
def consolidate_one(kind: str):
    """
    kind: 'problems' | 'ideas' | 'signals'
    читает artifacts/comprehensive_results.json и кладёт:
    artifacts/{kind}_summary.csv, ..._subthemes.csv, ..._mentions.csv
    """
    singular = {"problems":"problem", "ideas":"idea", "signals":"signal"}[kind]
    # 1) грузим mentions и фильтруем по типу
    js = json.loads((ART/"comprehensive_results.json").read_text(encoding="utf-8"))
    m = pd.DataFrame(js.get("mentions", []))
    if m.empty:
        raise SystemExit("Нет mentions в comprehensive_results.json")
    m = m[m["label_type"] == kind]  # <— критично
    if m.empty:
        # всё равно выпустим пустышки, чтобы UI не падал
        (ART/f"{kind}_summary.csv").write_text("", encoding="utf-8")
        (ART/f"{kind}_subthemes.csv").write_text("", encoding="utf-8")
        (ART/f"{kind}_mentions.csv").write_text("", encoding="utf-8")
        print(f"[{kind}] предупреждение: нет упоминаний этого типа")
        return m, pd.DataFrame(), pd.DataFrame(), 0

    # 2) нормализуем ключи для join
    m["theme"] = m["theme"].fillna("")
    m["subtheme"] = m["subtheme"].fillna("")
    m["theme_norm"] = m["theme"].map(_norm)
    m["subtheme_norm"] = m["subtheme"].map(_norm)

    # 3) мапа соответствий
    mp = build_map_for(kind)

    # 4) join
    merged = m.merge(mp, how="left", on=["theme_norm", "subtheme_norm"])
    # дефолт «Прочее»
    merged[f"{singular}_id"]    = merged[f"{singular}_id"].fillna("other_unmapped")
    merged[f"{singular}_title"] = merged[f"{singular}_title"].fillna("Прочее/не сконсолидировано")

    # 5) отладочный отчёт в консоль — очень помогает
    total_pairs = m[["theme_norm","subtheme_norm"]].drop_duplicates().shape[0]
    matched_pairs = merged[merged[f"{singular}_id"]!="other_unmapped"][["theme_norm","subtheme_norm"]].drop_duplicates().shape[0]
    print(f"[{kind}] неповторимых пар theme/subtheme: {total_pairs}; заматчено: {matched_pairs}; "
          f"доля мапы: {0 if total_pairs==0 else round(100*matched_pairs/total_pairs,1)}%")

    # 6) агрегации
    total_dialogs = merged["dialog_id"].nunique()
    agg = (
        merged.groupby([f"{singular}_id", f"{singular}_title"])
        .agg(dialogs=("dialog_id","nunique"), mentions=("text_quote","count"))
        .reset_index()
    )
    agg["share_dialogs_pct"] = (100*agg["dialogs"]/max(1,total_dialogs)).round(1)
    agg["freq_per_1k"]       = (1000*agg["dialogs"]/max(1,total_dialogs)).round(1)
    agg["intensity_mpd"]     = (agg["mentions"]/agg["dialogs"].clip(lower=1)).round(2)

    sub = (
        merged.groupby([f"{singular}_id", f"{singular}_title", "theme", "subtheme"])
        .agg(dialogs=("dialog_id","nunique"), mentions=("text_quote","count"))
        .reset_index()
        .sort_values([f"{singular}_id","dialogs"], ascending=[True, False])
    )

    # 7) выгрузка артефактов
    agg.to_csv(ART/f"{kind}_summary.csv", index=False)
    sub.to_csv(ART/f"{kind}_subthemes.csv", index=False)
    merged.to_csv(ART/f"{kind}_mentions.csv", index=False)
    return merged, agg, sub, total_dialogs

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
        r = client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
        )
        r.raise_for_status()
        js = json.loads(r.json()["choices"][0]["message"]["content"])
        out.append(js)

    if out:
        out_path_jsonl.write_text("\n".join([json.dumps(x, ensure_ascii=False) for x in out]), encoding="utf-8")
        pd.DataFrame(out).to_csv(out_path_csv, index=False)
        print(f"[ok] карточки {kind} -> {out_path_jsonl}, {out_path_csv}")

def main():
    # раньше: merged, agg, sub, _ = consolidate()
    # теперь:
    for _kind in ["problems", "ideas", "signals"]:
        consolidate_one(_kind)
    
    # загружаем результаты для карточек
    for kind in ["problems", "ideas", "signals"]:
        try:
            agg = pd.read_csv(ART/f"{kind}_summary.csv")
            if not agg.empty:
                merged = pd.read_csv(ART/f"{kind}_mentions.csv")
                sub = pd.read_csv(ART/f"{kind}_subthemes.csv")
                _summarize_cards(kind, merged, agg, sub)
                print(f"[ok] {kind}: dialogs={agg['dialogs'].sum() if not agg.empty else 0}, rows={len(agg)}")
        except Exception as e:
            print(f"[warn] {kind}: {e}")

    print("[ok] artifacts/* для problems/ideas/signals готовы")

if __name__ == "__main__":
    main()