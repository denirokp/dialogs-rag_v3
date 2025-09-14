import argparse, json
import numpy as np
from umap import UMAP
from hdbscan import HDBSCAN

def load_mentions(path, theme, subtheme):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            m = json.loads(line)
            if m["theme"] == theme and m["subtheme"] == subtheme:
                data.append(m)
    return data

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mentions", required=True)      # jsonl
    p.add_argument("--embeddings", required=True)    # npy, same order as mentions file!
    p.add_argument("--theme", required=True)
    p.add_argument("--subtheme", required=True)
    p.add_argument("--out", required=True)           # json
    args = p.parse_args()

    mentions = load_mentions(args.mentions, args.theme, args.subtheme)
    if not mentions:
        with open(args.out, "w", encoding="utf-8") as f: f.write(json.dumps({"clusters":[]}, ensure_ascii=False))
        return

    emb = np.load(args.embeddings)[:len(mentions)]
    X = UMAP(n_neighbors=12, min_dist=0.1, metric="cosine", random_state=42).fit_transform(emb)
    labels = HDBSCAN(min_cluster_size=25, metric='euclidean').fit_predict(X)

    clusters = {}
    for lbl, m in zip(labels, mentions):
        clusters.setdefault(int(lbl), {"size":0, "quotes":[]})
        clusters[int(lbl)]["size"] += 1
        if len(clusters[int(lbl)]["quotes"]) < 10:
            clusters[int(lbl)]["quotes"].append(m["text_quote"])

    out = []
    for lbl, info in sorted(clusters.items(), key=lambda kv: (-kv[1]["size"], kv[0])):
        name = "шум" if lbl == -1 else f"кластер_{lbl}"
        out.append({"cluster_id": str(lbl), "name": name, "size": info["size"], "sample_quotes": info["quotes"]})

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"theme": args.theme, "subtheme": args.subtheme, "clusters": out}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
