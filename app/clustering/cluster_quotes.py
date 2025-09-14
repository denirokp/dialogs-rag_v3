import polars as pl
from hdbscan import HDBSCAN
from umap import UMAP
from app.clustering.embed import Embedder

def cluster_quotes(quotes_df: pl.DataFrame, embedder: Embedder) -> pl.DataFrame:
    if quotes_df.is_empty():
        return quotes_df.with_columns(pl.lit(-1).alias("cluster"))
    texts = quotes_df["text_quote"].to_list()
    X = embedder.encode(texts)
    umap = UMAP(n_neighbors=15, min_dist=0.0, n_components=15, metric="cosine")
    Xr = umap.fit_transform(X)
    hdb = HDBSCAN(min_cluster_size=25, metric="euclidean")
    labels = hdb.fit_predict(Xr)
    return quotes_df.with_columns(pl.Series("cluster", labels))
