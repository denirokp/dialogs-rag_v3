import re, hashlib
import polars as pl

NORMALIZE_RE = re.compile(r"\s+", re.MULTILINE)

def norm_quote(s: str) -> str:
    s = s.strip().lower()
    s = NORMALIZE_RE.sub(" ", s)
    return s

def dedup_mentions(df: pl.DataFrame, threshold: float = 0.01) -> tuple[pl.DataFrame, float]:
    if df.is_empty():
        return df, 0.0
    df = df.with_columns([
        pl.col("text_quote").map_elements(norm_quote).alias("_nq"),
    ])
    df = df.with_columns([
        pl.col("_nq").map_elements(lambda s: hashlib.md5(s.encode()).hexdigest()).alias("_hash")
    ])
    total = df.height
    df = df.unique(subset=["dialog_id", "turn_id", "theme", "subtheme", "_hash"])
    dedup_rate = 1 - df.height / total
    if dedup_rate > threshold:
        print(f"[WARN] Dedup rate {dedup_rate:.3%} exceeds threshold {threshold:.1%}")
    return df.drop(["_nq", "_hash"]), dedup_rate
