import polars as pl
from pathlib import Path

def read_any(path: str) -> pl.DataFrame:
    p = Path(path)
    if p.suffix.lower() in {".xlsx", ".xls"}:
        return pl.read_excel(path)
    if p.suffix.lower() == ".csv":
        return pl.read_csv(path)
    if p.suffix.lower() in {".parquet", ".pq"}:
        return pl.read_parquet(path)
    raise ValueError(f"Unsupported format: {p.suffix}")
