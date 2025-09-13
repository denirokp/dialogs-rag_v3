import re, uuid, hashlib
import pandas as pd
from sentence_transformers import SentenceTransformer
import chromadb
from config import settings

ROLE_RE = re.compile(r"^\s*(Клиент|Оператор)\s*[:.]\s*(.*)$", re.I)

def read_excel(xlsx_path, sheets=None):
    if sheets is None:
        x = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")
        frames = []
        for name, df in x.items():
            df["__sheet"] = name
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    else:
        frames = []
        for name in sheets:
            df = pd.read_excel(xlsx_path, sheet_name=name, engine="openpyxl")
            df["__sheet"] = name
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

def split_turns(raw_text: str):
    turns = []
    for raw in str(raw_text).splitlines():
        line = raw.strip()
        if not line:
            continue
        m = ROLE_RE.match(line)
        if m:
            role = m.group(1).lower()
            role = "client" if role.startswith(settings.client_label) else "operator"
            text = m.group(2).strip()
            turns.append({"role": role, "text": text})
        else:
            if turns:
                turns[-1]["text"] += " " + line
    return turns

def build_windows(dialog_id: str, turns, prev:int, next_:int):
    windows = []
    seen = set()
    for i, t in enumerate(turns):
        if t["role"] != "client":
            continue
        L = max(0, i - prev)
        R = min(len(turns)-1, i + next_)
        full = "\n".join(f'{x["role"]}: {x["text"]}' for x in turns[L:R+1])
        client_only = "\n".join(x["text"] for x in turns[L:R+1] if x["role"]=="client")
        h = hashlib.md5(full.encode("utf-8")).hexdigest()
        if h in seen:
            continue
        seen.add(h)
        windows.append({
            "id": str(uuid.uuid4()),
            "dialog_id": str(dialog_id),
            "turn_L": L, "turn_R": R,
            "context_full": full,
            "context_client_only": client_only
        })
    return windows

def main():
    df = read_excel(settings.xlsx_path, settings.sheet_names).fillna("")
    assert settings.col_id in df.columns, f"Нет колонки: {settings.col_id}"
    assert settings.col_text in df.columns, f"Нет колонки: {settings.col_text}"

    records = []
    for _, row in df.iterrows():
        did = row[settings.col_id]
        turns = split_turns(row[settings.col_text])
        if not turns:
            continue
        records.extend(build_windows(str(did), turns, settings.prev_turns, settings.next_turns))

    if not records:
        print("Нечего индексировать.")
        return

    emb_model = SentenceTransformer(settings.embed_model_name)
    vecs = emb_model.encode([r["context_full"] for r in records],
                            normalize_embeddings=True, show_progress_bar=True).tolist()

    client = chromadb.PersistentClient(path=settings.chroma_path)
    col = client.get_or_create_collection(name=settings.collection)

    col.add(
        ids=[r["id"] for r in records],
        documents=[r["context_full"] for r in records],
        metadatas=[{
            "dialog_id": r["dialog_id"],
            "turn_L": r["turn_L"],
            "turn_R": r["turn_R"],
            "client_only": r["context_client_only"][:20000]
        } for r in records],
        embeddings=vecs
    )
    print(f"Готово. Проиндексировано окон: {len(records)}")

if __name__ == "__main__":
    main()
