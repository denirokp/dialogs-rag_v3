import duckdb, argparse, asyncio, pandas as pd, os
from pipeline.utils import classify

# Извлекаем только из client-реплик → mentions

async def main_async(batch: str):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    ut = con.execute("SELECT * FROM utterances WHERE batch_id = ? AND role = 'client'", [batch]).fetch_df()
    if ut.empty:
        print("No client utterances"); return

    mentions = []
    for row in ut.itertuples(index=False):
        pred = await classify(str(row.text))
        if not pred: continue
        mentions.append({
            "batch_id": row.batch_id,
            "dialog_id": row.dialog_id,
            "turn_id": row.turn_id,
            "theme": pred["theme"],
            "subtheme": pred["subtheme"],
            "label_type": pred.get("label_type", "барьер"),
            "text_quote": str(row.text).strip(),
            "confidence": float(pred.get("confidence", 0.5)),
            "is_client_only": True,
            "has_evidence": True,
        })

    md = pd.DataFrame(mentions)
    os.makedirs("data/warehouse", exist_ok=True)
    md.to_parquet(f"data/warehouse/mentions_{batch}.parquet", index=False)

    con.execute("CREATE OR REPLACE VIEW mentions AS SELECT * FROM read_parquet('data/warehouse/mentions_*.parquet');")
    print("Mentions:", len(md))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True)
    asyncio.run(main_async(ap.parse_args().batch))

