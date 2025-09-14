from pathlib import Path
import duckdb, polars as pl

class Duck:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)

    def write_df(self, df: pl.DataFrame, table: str):
        self.con.register("df", df.to_pandas())
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df").fetchall()
        self.con.execute(f"INSERT INTO {table} SELECT * FROM df")
        self.con.unregister("df")

    def query(self, sql: str):
        return pl.from_pandas(self.con.execute(sql).df())
