import polars as pl

REQUIRED_COLS = {"dialog_id", "turn_id", "role", "text"}

def to_canonical(df: pl.DataFrame) -> pl.DataFrame:
    # приведём к нужным столбцам (пример — адаптируйте маппинг под ваши поля)
    rename_map = {
        "dlg_id": "dialog_id",
        "speaker": "role",
        "utterance": "text",
    }
    df = df.rename({k:v for k,v in rename_map.items() if k in df.columns})
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    # нормализация типов
    return (
        df.with_columns([
            pl.col("dialog_id").cast(pl.Utf8),
            pl.col("turn_id").cast(pl.Int64),
            pl.col("role").str.to_lowercase(),
            pl.col("text").cast(pl.Utf8).str.strip(),
        ])
        .filter(pl.col("text").str.len_bytes() > 0)
    )
