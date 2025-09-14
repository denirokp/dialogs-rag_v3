import json, argparse
from collections import Counter

def key(m): return (m["theme"], m["subtheme"], m["label_type"])

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(x) for x in f]

def micro_f1(gold, pred):
    gold_keys = Counter(key(m) for m in gold)
    pred_keys = Counter(key(m) for m in pred)
    tp = sum((gold_keys & pred_keys).values())
    fp = sum((pred_keys - gold_keys).values())
    fn = sum((gold_keys - pred_keys).values())
    prec = tp / (tp + fp + 1e-12)
    rec = tp / (tp + fn + 1e-12)
    f1 = 2*prec*rec/(prec+rec+1e-12)
    return prec, rec, f1

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", required=True)
    ap.add_argument("--pred", required=True)
    args = ap.parse_args()
    gold = load_jsonl(args.gold)
    pred = load_jsonl(args.pred)
    p, r, f1 = micro_f1(gold, pred)
    print(f"precision={p:.4f} recall={r:.4f} microF1={f1:.4f}")
    if f1 < 0.90:
        raise SystemExit("❌ micro-F1 < 0.90")
    print("✅ micro-F1 ≥ 0.90")
