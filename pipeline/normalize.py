import duckdb, argparse, os, pandas as pd, re

PHONE = re.compile(r"(?:\+?\d[\s\-]?){7,}")
EMAIL = re.compile(r"[\w\.-]+@[\w\.-]+")

def mask_pii(t: str):
    t = PHONE.sub("[PHONE]", t)
    t = EMAIL.sub("[EMAIL]", t)
    return t

def main(batch: str):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    m = con.execute("SELECT * FROM mentions WHERE batch_id = ?", [batch]).fetch_df()
    if m.empty: print("No mentions"); return
    m["text_quote"] = m["text_quote"].astype(str).apply(mask_pii)
    m.to_parquet(f"data/warehouse/mentions_norm_{batch}.parquet", index=False)
    con.execute("CREATE OR REPLACE VIEW mentions_norm AS SELECT * FROM read_parquet('data/warehouse/mentions_norm_*.parquet');")
    print("Normalized:", len(m))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True)
    main(ap.parse_args().batch)

