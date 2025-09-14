import duckdb, argparse, os, pandas as pd, numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# Дедуп внутри (theme, subtheme) по косинусу эмбеддингов

def main(batch: str, threshold: float = 0.92):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    df = con.execute("SELECT * FROM mentions_norm WHERE batch_id = ?", [batch]).fetch_df()
    if df.empty: print("No mentions"); return

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    embs = model.encode(df["text_quote"].tolist(), normalize_embeddings=True)

    keep = np.ones(len(df), dtype=bool)
    for theme in df["theme"].unique():
        sub = df[df["theme"] == theme]
        for subtheme in sub["subtheme"].unique():
            idx = sub.index[sub["subtheme"] == subtheme].tolist()
            if len(idx) < 2: continue
            S = cosine_similarity(embs[idx], embs[idx])
            for i in range(len(idx)):
                if not keep[idx[i]]: continue
                dup = [j for j in range(i+1, len(idx)) if S[i, j] >= threshold]
                for j in dup: keep[idx[j]] = False

    deduped = df[keep]
    deduped.to_parquet(f"data/warehouse/mentions_final_{batch}.parquet", index=False)
    con.execute("CREATE OR REPLACE VIEW mentions_final AS SELECT * FROM read_parquet('data/warehouse/mentions_final_*.parquet');")
    removed = len(df) - len(deduped)
    print(f"Dedup: removed {removed} ({removed/max(1,len(df)):.1%})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True)
    main(ap.parse_args().batch)

