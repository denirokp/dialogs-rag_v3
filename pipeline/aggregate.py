import duckdb, argparse, os, pandas as pd

# Считает summary_themes, summary_subthemes, index_quotes и co-occurrence

def main(batch: str, n_dialogs: int):
    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))

    # Themes
    themes = con.execute(
        """
        WITH x AS (
          SELECT theme, dialog_id FROM mentions_final WHERE batch_id = ? GROUP BY theme, dialog_id
        ), d AS (
          SELECT theme, COUNT(*) dialogs FROM x GROUP BY theme
        ), m AS (
          SELECT theme, COUNT(*) mentions FROM mentions_final WHERE batch_id = ? GROUP BY theme
        )
        SELECT d.theme, d.dialogs, m.mentions, ROUND(100.0 * d.dialogs / ?, 1) AS share
        FROM d JOIN m USING(theme)
        ORDER BY d.dialogs DESC
        """, [batch, batch, n_dialogs]).fetch_df()

    # Subthemes
    subs = con.execute(
        """
        WITH x AS (
          SELECT theme, subtheme, dialog_id FROM mentions_final WHERE batch_id = ? GROUP BY theme, subtheme, dialog_id
        ), d AS (
          SELECT theme, subtheme, COUNT(*) dialogs FROM x GROUP BY theme, subtheme
        ), m AS (
          SELECT theme, subtheme, COUNT(*) mentions FROM mentions_final WHERE batch_id = ? GROUP BY theme, subtheme
        )
        SELECT d.theme, d.subtheme, d.dialogs, m.mentions, ROUND(100.0 * d.dialogs / ?, 1) AS share
        FROM d JOIN m USING(theme, subtheme)
        ORDER BY d.dialogs DESC
        """, [batch, batch, n_dialogs]).fetch_df()

    # Quotes index
    quotes = con.execute(
        """
        SELECT dialog_id, turn_id, theme, subtheme, label_type, text_quote, confidence
        FROM mentions_final WHERE batch_id = ?
        ORDER BY dialog_id, turn_id
        """, [batch]).fetch_df()

    # Co-occurrence: пары тем в рамках одного диалога
    co = con.execute(
        """
        WITH t AS (
          SELECT DISTINCT dialog_id, theme FROM mentions_final WHERE batch_id = ?
        ), pairs AS (
          SELECT a.theme AS themeA, b.theme AS themeB
          FROM t a JOIN t b USING(dialog_id)
          WHERE a.theme < b.theme
        )
        SELECT themeA, themeB, COUNT(*) AS weight
        FROM pairs GROUP BY themeA, themeB ORDER BY weight DESC
        """, [batch]).fetch_df()

    os.makedirs("data/warehouse", exist_ok=True)
    themes.to_parquet(f"data/warehouse/summary_themes_{batch}.parquet", index=False)
    subs.to_parquet(f"data/warehouse/summary_subthemes_{batch}.parquet", index=False)
    quotes.to_parquet(f"data/warehouse/index_quotes_{batch}.parquet", index=False)
    co.to_parquet(f"data/warehouse/cooccur_{batch}.parquet", index=False)

    con.execute("CREATE OR REPLACE VIEW summary_themes AS SELECT * FROM read_parquet('data/warehouse/summary_themes_*.parquet');")
    con.execute("CREATE OR REPLACE VIEW summary_subthemes AS SELECT * FROM read_parquet('data/warehouse/summary_subthemes_*.parquet');")
    con.execute("CREATE OR REPLACE VIEW index_quotes AS SELECT * FROM read_parquet('data/warehouse/index_quotes_*.parquet');")
    con.execute("CREATE OR REPLACE VIEW cooccur AS SELECT * FROM read_parquet('data/warehouse/cooccur_*.parquet');")
    print("Aggregates:", len(themes), len(subs), len(quotes), len(co))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--batch", required=True); ap.add_argument("--n_dialogs", type=int, required=True)
    a = ap.parse_args(); main(a.batch, a.n_dialogs)

