"""Embedding generation pipeline stage"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from loguru import logger
import pickle
from pathlib import Path

from .base_pipeline import BasePipeline
from ..config import ConfigManager


class EmbeddingGenerator(BasePipeline):
    """Generate embeddings for entities using sentence transformers"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize embedding generator
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "embedding_generator")
        
        # Load model configuration
        model_config = config_manager.get_model_config()
        self.model_name = model_config.embedding_name
        self.embedding_dimension = model_config.embedding_dimension
        
        # Initialize model
        self.model = self._load_model()
    
    def process(self, data: List[Dict[str, Any]], **kwargs) -> pd.DataFrame:
        """Generate embeddings for entities
        
        Args:
            data: List of flattened entity records
            **kwargs: Additional keyword arguments
            
        Returns:
            DataFrame with entities and their embeddings
        """
        self.log_stage_start(f"Processing {len(data)} entities")
        
        try:
            if not data:
                self.logger.warning("No entities to process")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Filter successful extractions
            successful_df = df[df['extraction_success'] == True].copy()
            
            if len(successful_df) == 0:
                self.logger.warning("No successful entity extractions found")
                return df
            
            # Generate embeddings
            embeddings = self._generate_embeddings(successful_df['entity_text'].tolist())
            
            # Add embeddings to DataFrame
            successful_df['embedding'] = embeddings.tolist()
            
            # Merge back with original data
            result_df = df.merge(
                successful_df[['dialog_id', 'entity_text', 'embedding']], 
                on=['dialog_id', 'entity_text'], 
                how='left'
            )
            
            # Fill NaN embeddings with zeros
            result_df['embedding'] = result_df['embedding'].apply(
                lambda x: [0.0] * self.embedding_dimension if pd.isna(x) else x
            )
            
            self.log_stage_end(f"Generated embeddings for {len(successful_df)} entities")
            return result_df
            
        except Exception as e:
            self.log_error(e, "Failed to generate embeddings")
            raise
    
    def _load_model(self) -> SentenceTransformer:
        """Load sentence transformer model
        
        Returns:
            Loaded model
        """
        try:
            self.logger.info(f"Loading embedding model: {self.model_name}")
            model = SentenceTransformer(self.model_name)
            
            # Verify embedding dimension
            test_embedding = model.encode(["test"])
            actual_dim = len(test_embedding[0])
            
            if actual_dim != self.embedding_dimension:
                self.logger.warning(
                    f"Model dimension mismatch: expected {self.embedding_dimension}, "
                    f"got {actual_dim}. Updating configuration."
                )
                self.embedding_dimension = actual_dim
            
            return model
            
        except Exception as e:
            self.log_error(e, f"Failed to load model {self.model_name}")
            raise
    
    def _generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings for a list of texts
        
        Args:
            texts: List of texts to embed
            
        Returns:
            Array of embeddings
        """
        try:
            # Clean texts
            cleaned_texts = [str(text).strip() for text in texts if str(text).strip()]
            
            if not cleaned_texts:
                return np.array([])
            
            # Generate embeddings
            embeddings = self.model.encode(
                cleaned_texts,
                convert_to_numpy=True,
                show_progress_bar=True
            )
            
            return embeddings
            
        except Exception as e:
            self.log_error(e, "Failed to generate embeddings")
            raise
    
    def save_embeddings(self, df: pd.DataFrame, filename: str) -> None:
        """Save embeddings to file
        
        Args:
            df: DataFrame with embeddings
            filename: Name of the file to save to
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = output_dir / filename
            
            # Save as pickle to preserve numpy arrays
            with open(filepath, 'wb') as f:
                pickle.dump(df, f)
            
            self.logger.info(f"Saved embeddings to {filepath}")
            
        except Exception as e:
            self.log_error(e, f"Failed to save embeddings to {filename}")
    
    def load_embeddings(self, filename: str) -> Optional[pd.DataFrame]:
        """Load embeddings from file
        
        Args:
            filename: Name of the file to load from
            
        Returns:
            DataFrame with embeddings or None if failed
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            filepath = output_dir / filename
            
            if not filepath.exists():
                self.logger.warning(f"Embeddings file not found: {filepath}")
                return None
            
            with open(filepath, 'rb') as f:
                df = pickle.load(f)
            
            self.logger.info(f"Loaded embeddings from {filepath}")
            return df
            
        except Exception as e:
            self.log_error(e, f"Failed to load embeddings from {filename}")
            return None
    
    def get_embedding_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get statistics about embeddings
        
        Args:
            df: DataFrame with embeddings
            
        Returns:
            Dictionary with embedding statistics
        """
        if df.empty:
            return {}
        
        # Count embeddings by type
        embeddings_by_type = df.groupby('entity_type').size().to_dict()
        
        # Calculate embedding statistics
        embedding_arrays = df['embedding'].dropna().tolist()
        
        if embedding_arrays:
            embeddings_matrix = np.array(embedding_arrays)
            
            stats = {
                'total_embeddings': len(embedding_arrays),
                'embedding_dimension': len(embedding_arrays[0]) if embedding_arrays else 0,
                'embeddings_by_type': embeddings_by_type,
                'mean_embedding_norm': np.mean([np.linalg.norm(emb) for emb in embedding_arrays]),
                'std_embedding_norm': np.std([np.linalg.norm(emb) for emb in embedding_arrays]),
                'min_embedding_norm': np.min([np.linalg.norm(emb) for emb in embedding_arrays]),
                'max_embedding_norm': np.max([np.linalg.norm(emb) for emb in embedding_arrays])
            }
        else:
            stats = {
                'total_embeddings': 0,
                'embedding_dimension': 0,
                'embeddings_by_type': embeddings_by_type
            }
        
        return stats
    
    def find_similar_entities(self, df: pd.DataFrame, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Find similar entities to a query
        
        Args:
            df: DataFrame with embeddings
            query: Query text
            top_k: Number of similar entities to return
            
        Returns:
            List of similar entities with similarity scores
        """
        try:
            # Generate embedding for query
            query_embedding = self.model.encode([query])
            
            # Get entity embeddings
            entity_embeddings = df['embedding'].dropna().tolist()
            entity_texts = df[df['embedding'].notna()]['entity_text'].tolist()
            
            if not entity_embeddings:
                return []
            
            # Calculate similarities
            similarities = np.dot(query_embedding, np.array(entity_embeddings).T)[0]
            
            # Get top-k similar entities
            top_indices = np.argsort(similarities)[::-1][:top_k]
            
            results = []
            for idx in top_indices:
                results.append({
                    'entity_text': entity_texts[idx],
                    'similarity': float(similarities[idx]),
                    'entity_type': df.iloc[idx]['entity_type']
                })
            
            return results
            
        except Exception as e:
            self.log_error(e, f"Failed to find similar entities for query: {query}")
            return []
