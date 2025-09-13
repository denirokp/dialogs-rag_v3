#!/usr/bin/env python3
"""
Единый скрипт для индексации и анализа диалогов
"""
import os
import json
import time
import csv
from typing import List, Dict, Any

import pandas as pd
import chromadb
from sentence_transformers import SentenceTransformer
from openai import OpenAI
from dotenv import load_dotenv

from config import settings
from prompts import ANALYST_SYSTEM_PROMPT, DEFAULT_QUERY

load_dotenv()

def read_excel(xlsx_path: str, sheet_names=None):
    """Читает Excel файл"""
    if sheet_names is None:
        return pd.read_excel(xlsx_path)
    else:
        return pd.concat([pd.read_excel(xlsx_path, sheet_name=name) for name in sheet_names], ignore_index=True)

def split_turns(text: str):
    """Разбивает текст на реплики по меткам ролей"""
    if not text or pd.isna(text):
        return []
    
    # Ищем метки ролей
    client_label = settings.client_label
    operator_label = settings.operator_label
    
    turns = []
    lines = text.split('\n')
    current_turn = ""
    current_role = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if client_label in line.lower():
            if current_turn and current_role:
                turns.append({"role": current_role, "text": current_turn.strip()})
            current_turn = line
            current_role = "client"
        elif operator_label in line.lower():
            if current_turn and current_role:
                turns.append({"role": current_role, "text": current_turn.strip()})
            current_turn = line
            current_role = "operator"
        else:
            if current_turn:
                current_turn += " " + line
            else:
                current_turn = line
    
    if current_turn and current_role:
        turns.append({"role": current_role, "text": current_turn.strip()})
    
    return turns

def build_windows(dialog_id: str, turns, prev: int, next_: int):
    """Создает окна контекста вокруг клиентских реплик"""
    windows = []
    
    for i, turn in enumerate(turns):
        if turn["role"] != "client":
            continue
            
        # Границы окна
        start = max(0, i - prev)
        end = min(len(turns), i + next_ + 1)
        
        # Собираем контекст
        context_turns = turns[start:end]
        context_full = " ".join([t["text"] for t in context_turns])
        context_client_only = " ".join([t["text"] for t in context_turns if t["role"] == "client"])
        
        window_id = f"{dialog_id}_{i}"
        
        windows.append({
            "id": window_id,
            "dialog_id": dialog_id,
            "turn_L": start,
            "turn_R": end - 1,
            "context_full": context_full,
            "context_client_only": context_client_only
        })
    
    return windows

def retrieve_for_dialog(dialog_id: str, query: str, topN: int = 15, emb_model=None, collection=None):
    """Извлекает релевантные блоки для диалога"""
    qvec = emb_model.encode([query], normalize_embeddings=True).tolist()[0]
    
    res = collection.query(
        query_embeddings=[qvec],
        n_results=topN,
        where={"dialog_id": dialog_id}
    )
    
    blocks = []
    for i in range(len(res["ids"][0])):
        meta = res["metadatas"][0][i]
        blocks.append({
            "id": res["ids"][0][i],
            "dialog_id": meta["dialog_id"],
            "turn_L": meta["turn_L"],
            "turn_R": meta["turn_R"],
            "context_full": res["documents"][0][i],
            "context_client_only": meta["client_only"],
            "distance": res["distances"][0][i]
        })
    
    return blocks

def rerank_blocks(query: str, blocks: List[Dict], top_k: int):
    """Ранжирует блоки по релевантности"""
    if not blocks:
        return []
    
    # Простое ранжирование по расстоянию
    sorted_blocks = sorted(blocks, key=lambda x: x["distance"])
    return sorted_blocks[:top_k]

def call_llm(system_prompt: str, user_prompt: str):
    """Вызывает LLM"""
    if not settings.use_openai:
        return '{"delivery_types": [], "barriers": [], "ideas": [], "self_check": "LLM отключен"}'
    
    oai = OpenAI()
    response = oai.chat.completions.create(
        model=settings.openai_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.1
    )
    return response.choices[0].message.content

def has_client_citations(payload):
    """Проверяет наличие цитат клиента в результатах"""
    text = json.dumps(payload, ensure_ascii=False)
    return settings.client_label in text.lower()

def main():
    print("🚀 Запускаем полный анализ диалогов...")
    
    # 1. Индексация данных
    print("\n📊 Этап 1: Индексация данных...")
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
    emb = SentenceTransformer(settings.embed_model_name)
    
    print("🔢 Создаем эмбеддинги...")
    # Обрабатываем батчами для экономии памяти
    batch_size = 100
    all_embeddings = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_texts = [r["context_full"] for r in batch]
        batch_embeddings = emb.encode(
            batch_texts,
            normalize_embeddings=True, 
            show_progress_bar=True,
            batch_size=32
        )
        all_embeddings.extend(batch_embeddings.tolist())
        print(f"  Обработано батчей: {i//batch_size + 1}/{(len(records)-1)//batch_size + 1}")

    print("💾 Сохраняем в ChromaDB...")
    ch = chromadb.Client()
    col = ch.create_collection(name=settings.collection)

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
    
    print(f"✅ Индексация завершена! Проиндексировано окон: {len(records)}")
    
    # 2. Анализ диалогов
    print("\n📊 Этап 2: Анализ диалогов...")
    
    # Получаем все ID диалогов
    res = col.get(include=["metadatas"], limit=200000)
    dialog_ids = sorted({m["dialog_id"] for m in res["metadatas"]})
    
    print(f"📊 Диалогов для обработки: {len(dialog_ids)}")
    print(f"⚙️ Настройки: top_k={settings.top_k}, модель={settings.openai_model}")
    print("🚀 Начинаем анализ...")

    with open("batch_results.jsonl", "w", encoding="utf-8") as jf, \
         open("batch_results.csv", "w", encoding="utf-8", newline="") as cf:
        
        csv_w = csv.DictWriter(cf, fieldnames=[
            "dialog_id", "delivery_types", "barriers", "ideas", "self_check"
        ])
        csv_w.writeheader()

        processed = 0
        errors = 0
        
        for i, did in enumerate(dialog_ids, 1):
            try:
                # Оптимизированный поиск
                blocksN = retrieve_for_dialog(did, DEFAULT_QUERY, topN=15, emb_model=emb, collection=col)
                blocks = rerank_blocks(DEFAULT_QUERY, blocksN, settings.top_k)
                
                if not blocks:
                    payload = {
                        "dialog_id": did,
                        "delivery_types": [],
                        "barriers": [],
                        "ideas": [],
                        "self_check": "Нет релевантных окон"
                    }
                else:
                    # Формируем промпт
                    context = "\n\n".join([f"Окно {j+1}:\n{b['context_full']}" for j, b in enumerate(blocks)])
                    prompt = f"Контекст диалога:\n{context}\n\nЗапрос: {DEFAULT_QUERY}"
                    
                    # Вызываем LLM
                    ans_raw = call_llm(ANALYST_SYSTEM_PROMPT, prompt)
                    
                    try:
                        data = json.loads(ans_raw)
                    except json.JSONDecodeError:
                        # Повторная попытка с исправленным промптом
                        ans_raw = call_llm(
                            ANALYST_SYSTEM_PROMPT + "\nОтвечай строго валидным JSON без преамбулы.",
                            prompt
                        )
                        data = json.loads(ans_raw)
                    payload = {"dialog_id": did, **data}

                    # Проверка качества цитат
                    if (payload.get("delivery_types") or payload.get("barriers") or payload.get("ideas")) and not has_client_citations(payload):                                                                       
                        sc = payload.get("self_check","")
                        payload["self_check"] = (sc + " | Нет цитат клиента для части выводов").strip()

                # Сохраняем результаты
                jf.write(json.dumps(payload, ensure_ascii=False) + "\n")
                csv_w.writerow({
                    "dialog_id": payload["dialog_id"],
                    "delivery_types": json.dumps(payload.get("delivery_types", []), ensure_ascii=False),
                    "barriers": json.dumps(payload.get("barriers", []), ensure_ascii=False),
                    "ideas": json.dumps(payload.get("ideas", []), ensure_ascii=False),
                    "self_check": payload.get("self_check","")
                })

                processed += 1
                
                # Прогресс каждые 25 диалогов
                if i % 25 == 0:
                    print(f"📈 Обработано: {i}/{len(dialog_ids)} ({i/len(dialog_ids)*100:.1f}%)")
                    print(f"✅ Успешно: {processed}, ❌ Ошибок: {errors}")
                
                # Уменьшенная задержка для ускорения
                time.sleep(0.02)

            except Exception as e:
                errors += 1
                print(f"❌ [{did}] Ошибка: {e}")
                jf.write(json.dumps({"dialog_id": did, "error": str(e)}, ensure_ascii=False) + "\n")
                continue
        
        print(f"\n🎉 Анализ завершен!")
        print(f"📊 Статистика: {processed} успешно, {errors} ошибок из {len(dialog_ids)} диалогов")
        print(f"📁 Результаты сохранены в batch_results.csv и batch_results.jsonl")

if __name__ == "__main__":
    main()
