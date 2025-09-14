import argparse, json, math
from collections import defaultdict
import numpy as np

def norm(s: str) -> str:
    return " ".join(s.lower().split())

def cosine(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", dest="out", required=True)
    p.add_argument("--emb", dest="emb", required=False)  # npy with embeddings aligned to mentions
    p.add_argument("--threshold", type=float, default=0.92)
    args = p.parse_args()

    with open(args.inp, "r", encoding="utf-8") as f:
        mentions = [json.loads(line) for line in f]

    quotes_norm = [norm(m["text_quote"]) for m in mentions]
    keep = [True] * len(mentions)

    # Hard dedup per (dialog_id, subtheme, text_quote_norm)
    seen = set()
    for i, m in enumerate(mentions):
        key = (str(m["dialog_id"]), m["subtheme"], quotes_norm[i])
        if key in seen:
            keep[i] = False
        else:
            seen.add(key)

    # Near-dup via embeddings (optional)
    if args.emb:
        emb = np.load(args.emb)
        buckets = defaultdict(list)
        for i, m in enumerate(mentions):
            if not keep[i]: continue
            buckets[(str(m["dialog_id"]), m["subtheme"])].append(i)
        for _, idxs in buckets.items():
            for i in range(len(idxs)):
                if not keep[idxs[i]]: continue
                for j in range(i+1, len(idxs)):
                    if not keep[idxs[j]]: continue
                    if cosine(emb[idxs[i]], emb[idxs[j]]) > args.threshold:
                        keep[idxs[j]] = False

    with open(args.out, "w", encoding="utf-8") as f:
        for i, m in enumerate(mentions):
            if keep[i]:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()
