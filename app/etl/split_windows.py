import polars as pl

def client_only(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("role") == "client")

def windows_by_dialog(df: pl.DataFrame, max_tokens: int = 1800) -> pl.DataFrame:
    # Простая нарезка по длине текста (символы ~ токены/3–4). Замените при необходимости.
    with_len = df.with_columns(pl.col("text").str.len_bytes().alias("len"))
    # Группируем по диалогу и будем накапливать окна
    out = []
    for dlg_id, grp in with_len.group_by("dialog_id"):
        acc, acc_len, idx = [], 0, 0
        for row in grp.sort("turn_id").iter_rows(named=True):
            if acc_len + row["len"] > max_tokens * 4 and acc:
                out.append({"dialog_id": dlg_id, "window_id": idx, "turns": acc})
                idx += 1
                acc, acc_len = [], 0
            acc.append(row)
            acc_len += row["len"]
        if acc:
            out.append({"dialog_id": dlg_id, "window_id": idx, "turns": acc})
    return pl.DataFrame(out)
