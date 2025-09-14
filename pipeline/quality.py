import duckdb, argparse, os, pandas as pd, yaml

# Evidence-100 / Client-only-100 / Dedup<=1% / Coverage>=0.98

def main(batch: str, n_dialogs: int):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    mf = con.execute("SELECT * FROM mentions_final WHERE batch_id = ?", [batch]).fetch_df()
    if mf.empty:
        q = {"evidence100": 0.0, "clientOnly100": 0.0, "dedupRate": 0.0, "coverage": 0.0, "totalDialogs": n_dialogs, "totalMentions": 0, "passed": False}
    else:
        evidence100 = float((mf["text_quote"].astype(str).str.len() > 0).mean())
        client_only100 = float(mf["is_client_only"].mean())
        before = con.execute("SELECT COUNT(*) FROM mentions_norm WHERE batch_id = ?", [batch]).fetchone()[0]
        dedup_rate = 0.0 if before == 0 else (before - len(mf)) / before
        # coverage по таксономии
        with open("taxonomy.yaml", "r", encoding="utf-8") as f:
            tax = yaml.safe_load(f)
        valid_themes = set([t.strip().lower() for t in tax.get("themes", [])])
        coverage = float((mf["theme"].str.lower().isin(valid_themes)).mean())
        q = {
            "evidence100": evidence100,
            "clientOnly100": client_only100,
            "dedupRate": float(dedup_rate),
            "coverage": coverage,
            "totalDialogs": int(n_dialogs),
            "totalMentions": int(len(mf)),
            "passed": (evidence100 == 1.0) and (client_only100 == 1.0) and (dedup_rate <= 0.01) and (coverage >= 0.98),
        }

    pd.DataFrame([q]).to_parquet(f"data/warehouse/quality_{batch}.parquet", index=False)
    con.execute("CREATE OR REPLACE VIEW quality AS SELECT * FROM read_parquet('data/warehouse/quality_*.parquet');")
    print(q)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True); ap.add_argument("--n_dialogs", type=int, required=True)
    a = ap.parse_args(); main(a.batch, a.n_dialogs)

