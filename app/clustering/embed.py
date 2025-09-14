from typing import List

# Вариант A: local sentence-transformers (рус/мультиязычный)
try:
    from sentence_transformers import SentenceTransformer
    _model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
except Exception:
    _model = None

class Embedder:
    def __init__(self, api_embedder=None):
        self.api = api_embedder

    def encode(self, texts: List[str]) -> list[list[float]]:
        if _model is not None:
            return _model.encode(texts, show_progress_bar=False).tolist()
        assert self.api is not None, "No local model. Provide API embedder."
        return self.api.embed(texts)
