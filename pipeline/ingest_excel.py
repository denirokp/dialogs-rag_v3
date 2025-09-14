import pandas as pd, argparse, os, duckdb
from pipeline.utils import detect_role

# Ожидается Excel с колонками: dialog_id, raw_text (или замените на ваши)
# Пример разбиения: одна строка = диалог; в raw_text — строки реплик, разделённые \n

def parse_dialog(raw: str):
    lines = [l.strip() for l in str(raw).splitlines() if l.strip()]
    for line in lines:
        role = detect_role(line)
        # Удаляем префикс роли "client:" / "operator:" если он есть
        text = line.split(":", 1)[-1].strip() if ":" in line else line
        yield role, text


def main(path: str, batch: str):
    df = pd.read_excel(path)
    assert "dialog_id" in df.columns, "Нужна колонка dialog_id"
    raw_col = "raw_text" if "raw_text" in df.columns else df.columns[-1]

    rows = []
    for _, r in df.iterrows():
        d = str(r["dialog_id"])
        for i, (role, text) in enumerate(parse_dialog(r[raw_col]), start=1):
            rows.append({"batch_id": batch, "dialog_id": d, "turn_id": i, "role": role, "text": text})

    out = pd.DataFrame(rows)
    os.makedirs("data/warehouse", exist_ok=True)
    out.to_parquet(f"data/warehouse/utterances_{batch}.parquet", index=False)

    con = duckdb.connect(os.getenv("DUCKDB_PATH", "data/rag.duckdb"))
    con.execute("CREATE OR REPLACE VIEW utterances AS SELECT * FROM read_parquet('data/warehouse/utterances_*.parquet');")
    print("Utterances:", len(out))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--batch", required=True)
    a = ap.parse_args()
    main(a.file, a.batch)

