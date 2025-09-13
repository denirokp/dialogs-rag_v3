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
    print("📊 Загружаем данные...")
    df = read_excel(settings.xlsx_path, settings.sheet_names).fillna("")
    assert settings.col_id in df.columns, f"Нет колонки: {settings.col_id}"
    assert settings.col_text in df.columns, f"Нет колонки: {settings.col_text}"
    
    print(f"📈 Найдено диалогов: {len(df)}")
    print("🔍 Обрабатываем диалоги...")

    records = []
    processed = 0
    for _, row in df.iterrows():
        did = row[settings.col_id]
        turns = split_turns(row[settings.col_text])
        if not turns:
            continue
        records.extend(build_windows(str(did), turns, settings.prev_turns, settings.next_turns))
        processed += 1
        if processed % 100 == 0:
            print(f"  Обработано диалогов: {processed}/{len(df)}")

    if not records:
        print("❌ Нечего индексировать.")
        return

    print(f"📦 Создано окон: {len(records)}")
    print("🤖 Загружаем модель эмбеддингов...")
    emb_model = SentenceTransformer(settings.embed_model_name)
    
    print("🔢 Создаем эмбеддинги...")
    # Обрабатываем батчами для экономии памяти
    batch_size = 100
    all_embeddings = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_texts = [r["context_full"] for r in batch]
        batch_embeddings = emb_model.encode(
            batch_texts,
            normalize_embeddings=True, 
            show_progress_bar=True,
            batch_size=32  # уменьшенный batch_size для стабильности
        )
        all_embeddings.extend(batch_embeddings.tolist())
        print(f"  Обработано батчей: {i//batch_size + 1}/{(len(records)-1)//batch_size + 1}")

    print("💾 Сохраняем в ChromaDB...")
    client = chromadb.Client()
    col = client.create_collection(name=settings.collection)

    # Добавляем батчами для экономии памяти
    add_batch_size = 500
    for i in range(0, len(records), add_batch_size):
        batch = records[i:i + add_batch_size]
        batch_embeddings = all_embeddings[i:i + add_batch_size]
        
        col.add(
            ids=[r["id"] for r in batch],
            documents=[r["context_full"] for r in batch],
            metadatas=[{
                "dialog_id": r["dialog_id"],
                "turn_L": r["turn_L"],
                "turn_R": r["turn_R"],
                "client_only": r["context_client_only"][:20000]
            } for r in batch],
            embeddings=batch_embeddings
        )
        print(f"  Сохранено батчей: {i//add_batch_size + 1}/{(len(records)-1)//add_batch_size + 1}")
    
    print(f"✅ Готово! Проиндексировано окон: {len(records)}")
    print(f"📊 Диалогов обработано: {processed}")
    print(f"🗄️ Размер базы: {len(records)} окон")

if __name__ == "__main__":
    main()
