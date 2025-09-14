# -*- coding: utf-8 -*-
"""
LLM-based анализатор (интегрирован под текущую систему):
- Читает Excel: data/input/dialogs14_09.xlsx с колонками ["ID звонка","Текст транскрибации"]
- Режет диалоги на реплики, оставляет только client-only
- Гибрид whole-dialog / окна
- LLM извлекает упоминания (problems/ideas/signals) по taxonomy.yaml
- Дедуп и запись артефактов:
  - artifacts/comprehensive_results.json   ({"mentions": [...]})
  - artifacts/statistics.json              (пересчитывается)
Требуется: OPENAI_API_KEY; pip install -r requirements.txt
"""

import os, re, json, math, hashlib, argparse
import httpx, pandas as pd, yaml
from pathlib import Path
from typing import List, Dict, Any

INPUT_XLSX = "data/input/dialogs14_09.xlsx"
ART_DIR = Path("artifacts")
ART_DIR.mkdir(parents=True, exist_ok=True)
RES_PATH = ART_DIR / "comprehensive_results.json"
STATS_PATH = ART_DIR / "statistics.json"
TAX_PATH = "taxonomy.yaml"

# ----------------- чтение, парсинг, client-only -----------------
def read_dialogs(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.rename(columns={"ID звонка":"dialog_id","Текст транскрибации":"full_text"})
    assert {"dialog_id","full_text"} <= set(df.columns)
    df["dialog_id"] = df["dialog_id"].astype(str)
    return df[["dialog_id","full_text"]]

# Поддержка вариантов ролей и разделителей (":" или "-")
ROLE_PATTERNS = {
    "client": re.compile(r"^(?:Клиент|Покупатель)\s*[:\-]\s*(.*)$", re.IGNORECASE),
    "operator": re.compile(r"^(?:Оператор|Менеджер)\s*[:\-]\s*(.*)$", re.IGNORECASE),
}

def split_turns(full_text: str) -> List[Dict[str,Any]]:
    turns = []
    tid = 0
    for raw in (full_text or "").splitlines():
        raw = str(raw).strip()
        if not raw:
            continue
        m_client = ROLE_PATTERNS["client"].match(raw)
        m_oper = ROLE_PATTERNS["operator"].match(raw)
        if m_client:
            role = "client"; text = m_client.group(1).strip()
        elif m_oper:
            role = "operator"; text = m_oper.group(1).strip()
        else:
            continue
        tid += 1
        turns.append({"turn_id": tid, "role": role, "text": text})
    return turns

def client_only_windows(turns: List[Dict[str,Any]], whole_max_tokens=8000, window_tokens=1800):
    # оценка токенов ~ символы/4
    client_turns = [t for t in turns if t["role"] == "client"]
    total_chars = sum(len(t["text"]) for t in client_turns)
    if math.ceil(total_chars/4) <= whole_max_tokens:
        return [{"mode":"whole","window_id":0,"turns":client_turns}]
    # окна
    out, acc, acc_len, widx = [], [], 0, 0
    max_chars = window_tokens*4
    for t in client_turns:
        L = len(t["text"])
        if acc and acc_len + L > max_chars:
            out.append({"mode":"window","window_id":widx,"turns":acc})
            widx, acc, acc_len = widx+1, [], 0
        acc.append(t); acc_len += L
    if acc: out.append({"mode":"window","window_id":widx,"turns":acc})
    return out

def format_for_prompt(window) -> str:
    return "\n".join([f"[{t['turn_id']}] {t['text']}" for t in window["turns"]])

# ----------------- LLM экстракция -----------------
SYSTEM = (
    "Ты извлекаешь только из слов КЛИЕНТА. Верни ОДИН JSON-ОБЪЕКТ вида "
    '{"mentions":[{...}]}. Для каждого упоминания ключи: '
    "dialog_id, turn_id, label_type (problems|ideas|signals), "
    "theme, subtheme, text_quote, confidence (0..1). Без пояснений."
)

USER_TMPL = (
    "Таксономия (themes→subthemes):\n{taxonomy}\n---\n"
    "Окно диалога (только клиент):\n{window}\n---\n"
    "Верни JSON-объект {\"mentions\":[...]} со строгими ключами."
)

class LLM:
    def __init__(self, model="gpt-4o-mini", timeout=60):
        self.model = model
        self.key = os.getenv("OPENAI_API_KEY", "")
        self.client = httpx.Client(timeout=timeout)

    def extract(self, dialog_id: str, window) -> List[Dict[str,Any]]:
        if not self.key:
            raise RuntimeError("ENV OPENAI_API_KEY не задан")
        with open(TAX_PATH, "r", encoding="utf-8") as f:
            taxonomy = yaml.safe_load(f)
        user = USER_TMPL.format(
            taxonomy=json.dumps(taxonomy, ensure_ascii=False),
            window=format_for_prompt(window),
        )
        payload = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user},
            ],
        }
        try:
            r = self.client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            r.raise_for_status()
        except httpx.RequestError as e:
            print(f"[warn] OpenAI request failed for dialog {dialog_id}: {e}")
            return []
        try:
            content = r.json()["choices"][0]["message"]["content"]
            js = json.loads(content)
            arr = js.get("mentions", [])
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            print(f"[warn] Failed to parse response for dialog {dialog_id}: {e}")
            arr = []
        out = []
        for m in arr:
            if not isinstance(m, dict):
                continue
            text_quote = (m.get("text_quote") or "").strip()
            if not text_quote:
                continue
            turn_id = m.get("turn_id")
            try:
                turn_id = int(turn_id)
            except:
                mt = re.search(r"\[(\d+)\]", text_quote)
                turn_id = int(mt.group(1)) if mt else 0
            out.append({
                "dialog_id": dialog_id,
                "turn_id": turn_id,
                "label_type": (m.get("label_type") or "problems"),
                "theme": (m.get("theme") or "").strip(),
                "subtheme": (m.get("subtheme") or "").strip(),
                "text_quote": text_quote,
                "confidence": float(m.get("confidence") or 0.5),
            })
        return out

# ----------------- дедуп -----------------
def norm_quote(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def dedup_mentions(rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    seen = set(); out = []
    for r in rows:
        key = (
            r["dialog_id"],
            r["turn_id"],
            r["label_type"],
            r["theme"],
            r.get("subtheme"),
            hashlib.md5(norm_quote(r["text_quote"]).encode()).hexdigest(),
        )
        if key in seen:
            continue
        seen.add(key); out.append(r)
    return out

# ----------------- основной прогон -----------------
def run(model="gpt-4o-mini", whole_max=8000, window_tokens=1800):
    df = read_dialogs(INPUT_XLSX)
    llm = LLM(model=model)
    all_mentions: List[Dict[str,Any]] = []
    for _, row in df.iterrows():
        dlg_id = row["dialog_id"]; turns = split_turns(row["full_text"])
        windows = client_only_windows(
            turns, whole_max_tokens=whole_max, window_tokens=window_tokens
        )
        for w in windows:
            all_mentions.extend(llm.extract(dlg_id, w))

    # До дедупа
    pre_count = len(all_mentions)
    all_mentions = dedup_mentions(all_mentions)
    dedup_removed_pct = round(100 * (1 - len(all_mentions) / max(1, pre_count)), 1)
    ambiguity_pct = round(
        100 * sum(1 for m in all_mentions if m.get("confidence", 0) < 0.6) / max(1, len(all_mentions)),
        1,
    )

    # запишем артефакты
    RES_PATH.write_text(
        json.dumps({"mentions": all_mentions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # пересчёт статистики
    stats = {
        "dialogs": int(df["dialog_id"].nunique()),
        "mentions": len(all_mentions),
        "problems": sum(1 for m in all_mentions if m["label_type"] == "problems"),
        "ideas": sum(1 for m in all_mentions if m["label_type"] == "ideas"),
        "signals": sum(1 for m in all_mentions if m["label_type"] == "signals"),
        "evidence_100": (len(all_mentions) > 0 and all(m.get("text_quote") for m in all_mentions)),
        "dedup_removed_pct": dedup_removed_pct,
        "ambiguity_pct": ambiguity_pct,
    }
    STATS_PATH.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] mentions={len(all_mentions)}  dialogs={stats['dialogs']}  saved -> {RES_PATH}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--whole_max", type=int, default=8000)
    ap.add_argument("--window_tokens", type=int, default=1800)
    args = ap.parse_args()
    run(model=args.model, whole_max=args.whole_max, window_tokens=args.window_tokens)