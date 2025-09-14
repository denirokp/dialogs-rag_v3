import duckdb, argparse, os, pandas as pd
import numpy as np
import umap, hdbscan
from collections import Counter
from sentence_transformers import SentenceTransformer

# Кластеризация цитат ПО КАЖДОЙ подтеме → размеры кластеров + ключевые слова (для карточек)

def top_tokens(texts, n=5):
    toks = []
    for t in texts:
        toks += [w for w in str(t).lower().split() if 2 <= len(w) <= 20]
    return [w for w, _ in Counter(toks).most_common(n)]

def main(batch: str, min_cluster_size: int = 25):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    df = con.execute("SELECT * FROM mentions_final WHERE batch_id = ?", [batch]).fetch_df()
    if df.empty: print("No mentions_final"); return

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    clusters = []
    for theme in df["theme"].unique():
        subdf_t = df[df["theme"] == theme]
        for subtheme in subdf_t["subtheme"].unique():
            subdf = subdf_t[subdf_t["subtheme"] == subtheme].reset_index(drop=True)
            if len(subdf) < max(10, min_cluster_size):
                clusters.append({"theme": theme, "subtheme": subtheme, "cluster_label": -1, "size": len(subdf), "keywords": top_tokens(subdf["text_quote"])})
                continue
            embs = model.encode(subdf["text_quote"].tolist(), normalize_embeddings=True)
            emb2 = umap.UMAP(n_neighbors=12, min_dist=0.1, random_state=42).fit_transform(embs)
            labels = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size).fit_predict(emb2)
            subdf["cluster_label"] = labels
            for cl in sorted(subdf["cluster_label"].unique()):
                samples = subdf[subdf["cluster_label"] == cl]
                clusters.append({
                    "theme": theme,
                    "subtheme": subtheme,
                    "cluster_label": int(cl),
                    "size": int(len(samples)),
                    "keywords": top_tokens(samples["text_quote"])[:5],
                })

    out = pd.DataFrame(clusters)
    os.makedirs("data/warehouse", exist_ok=True)
    out.to_parquet(f"data/warehouse/clusters_{batch}.parquet", index=False)
    con.execute("CREATE OR REPLACE VIEW clusters AS SELECT * FROM read_parquet('data/warehouse/clusters_*.parquet');")
    print("Clusters rows:", len(out))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True)
    main(ap.parse_args().batch)

