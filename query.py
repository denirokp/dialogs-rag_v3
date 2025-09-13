import json
import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from openai import OpenAI
from config import settings
from prompts import ANALYST_SYSTEM_PROMPT, DEFAULT_QUERY

load_dotenv()

def retrieve_blocks(query_text: str, top_k: int):
    emb = SentenceTransformer(settings.embed_model_name)
    qvec = emb.encode([query_text], normalize_embeddings=True).tolist()[0]

    ch = chromadb.Client()
    col = ch.get_collection(name=settings.collection)

    res = col.query(query_embeddings=[qvec], n_results=top_k)
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

def build_user_prompt(query_text: str, blocks):
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

def call_llm(system_prompt: str, user_prompt: str) -> str:
    if not settings.use_openai:
        return json.dumps({
            "delivery_discussed": False,
            "delivery_types": [],
            "barriers": [],
            "ideas": [],
            "signals": [],
            "self_check": "LLM отключён (stub)",
            "citations": []
        }, ensure_ascii=False)
    client = OpenAI()
    resp = client.chat.completions.create(
        model=settings.openai_model,
        temperature=settings.temperature,
        messages=[{"role": "system", "content": system_prompt},
                  {"role": "user", "content": user_prompt}]
    )
    return resp.choices[0].message.content

if __name__ == "__main__":
    import sys
    query_text = " ".join(sys.argv[1:]).strip() or DEFAULT_QUERY
    blocks = retrieve_blocks(query_text, settings.top_k)
    prompt = build_user_prompt(query_text, blocks)
    result = call_llm(ANALYST_SYSTEM_PROMPT, prompt)
    print(result)
