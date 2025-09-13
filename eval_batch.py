import json, time, csv
import chromadb
from sentence_transformers import SentenceTransformer
from openai import OpenAI
from dotenv import load_dotenv
from config import settings
from prompts import ANALYST_SYSTEM_PROMPT, DEFAULT_QUERY

load_dotenv()
emb = SentenceTransformer(settings.embed_model_name)
ch = chromadb.PersistentClient(path=settings.chroma_path)
col = ch.get_or_create_collection(name=settings.collection)
oai = OpenAI() if settings.use_openai else None

def get_all_dialog_ids(limit=200000):
    res = col.get(include=["metadatas"], limit=limit)
    return sorted({m["dialog_id"] for m in res["metadatas"]})

def retrieve_for_dialog(dialog_id, query_text, topN=30):
    qvec = emb.encode([query_text], normalize_embeddings=True).tolist()[0]
    res = col.query(query_embeddings=[qvec], n_results=topN, where={"dialog_id": dialog_id})
    blocks = []
    for i in range(len(res["ids"][0])):
        meta = res["metadatas"][0][i]
        blocks.append({
            "dialog_id": meta["dialog_id"],
            "turn_L": meta["turn_L"],
            "turn_R": meta["turn_R"],
            "full": res["documents"][0][i],
            "client_only": meta["client_only"]
        })
    return blocks

USE_RERANKER = False
try:
    from sentence_transformers import CrossEncoder
except Exception:
    CrossEncoder = None
_reranker = None

def rerank_blocks(query_text, blocks, topK):
    global _reranker
    if not USE_RERANKER or CrossEncoder is None or not blocks:
        return blocks[:topK]
    if _reranker is None:
        _reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    pairs = [(query_text, b["full"]) for b in blocks]
    scores = _reranker.predict(pairs)
    ranked = sorted(zip(blocks, scores), key=lambda x: x[1], reverse=True)
    return [b for b, _ in ranked[:topK]]

def build_user_prompt(query_text, blocks):
    parts = [f"Вопрос:\n{query_text}\n"]
    for b in blocks:
        parts.append(
f"""[dialog_id={b['dialog_id']} turn={b['turn_L']}-{b['turn_R']}]
# FULL
{b['full']}

# CLIENT_ONLY
{b['client_only']}
"""
        )
    return "\n---\n".join(parts)

def call_llm(system, user):
    if not settings.use_openai:
        return json.dumps({
            "delivery_discussed": False, "delivery_types": [],
            "barriers": [], "ideas": [], "signals": [],
            "self_check": "LLM disabled (stub)", "citations": []
        }, ensure_ascii=False)
    resp = oai.chat.completions.create(
        model=settings.openai_model, temperature=settings.temperature,
        messages=[{"role":"system","content":system},{"role":"user","content":user}]
    )
    return resp.choices[0].message.content

def has_client_citations(payload):
    cits = payload.get("citations", [])
    return bool(cits) and all(isinstance(c.get("quote",""), str) and len(c["quote"])>0 for c in cits)

def main():
    ids = get_all_dialog_ids()
    print(f"Диалогов для обработки: {len(ids)}")

    with open("batch_results.jsonl", "w", encoding="utf-8") as jf, \
         open("batch_results.csv", "w", encoding="utf-8", newline="") as cf:

        csv_w = csv.DictWriter(cf, fieldnames=[
            "dialog_id","delivery_discussed","delivery_types","barriers","ideas","signals","self_check"
        ])
        csv_w.writeheader()

        for i, did in enumerate(ids, 1):
            try:
                blocksN = retrieve_for_dialog(did, DEFAULT_QUERY, topN=30)
                blocks = rerank_blocks(DEFAULT_QUERY, blocksN, settings.top_k)
                if not blocks:
                    payload = {
                        "dialog_id": did,
                        "delivery_discussed": False,
                        "delivery_types": [],
                        "barriers": [],
                        "ideas": [],
                        "signals": [],
                        "self_check": "Нет клиентских окон/реплик",
                        "citations": []
                    }
                else:
                    prompt = build_user_prompt(DEFAULT_QUERY, blocks)
                    ans_raw = call_llm(ANALYST_SYSTEM_PROMPT, prompt)
                    try:
                        data = json.loads(ans_raw)
                    except json.JSONDecodeError:
                        ans_raw = call_llm(
                            ANALYST_SYSTEM_PROMPT + "\nОтвечай строго валидным JSON без преамбулы.",
                            prompt
                        )
                        data = json.loads(ans_raw)
                    payload = {"dialog_id": did, **data}

                    if (payload.get("delivery_types") or payload.get("barriers") or payload.get("ideas")) and not has_client_citations(payload):
                        sc = payload.get("self_check","")
                        payload["self_check"] = (sc + " | Нет цитат клиента для части выводов — проверьте окна/промпт/реранкер").strip()

                jf.write(json.dumps(payload, ensure_ascii=False) + "\n")
                csv_w.writerow({
                    "dialog_id": did,
                    "delivery_discussed": payload.get("delivery_discussed"),
                    "delivery_types": "|".join(payload.get("delivery_types", [])),
                    "barriers": "|".join(payload.get("barriers", [])),
                    "ideas": "|".join(payload.get("ideas", [])),
                    "signals": "|".join(payload.get("signals", [])),
                    "self_check": payload.get("self_check","")
                })

                if i % 50 == 0:
                    print(f"Готово {i}/{len(ids)}")
                time.sleep(0.05)

            except Exception as e:
                print(f"[{did}] error: {e}")
                jf.write(json.dumps({"dialog_id": did, "error": str(e)}, ensure_ascii=False) + "\n")
                continue

if __name__ == "__main__":
    main()
