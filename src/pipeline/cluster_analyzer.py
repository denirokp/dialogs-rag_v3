"""Cluster analysis pipeline stage"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from sklearn.cluster import HDBSCAN
from sklearn.metrics import silhouette_score
from loguru import logger
import json
from pathlib import Path

from .base_pipeline import BasePipeline
from ..config import ConfigManager


class ClusterAnalyzer(BasePipeline):
    """Analyze and cluster entities using HDBSCAN"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize cluster analyzer
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "cluster_analyzer")
        
        # Load clustering configuration
        model_config = config_manager.get_model_config()
        self.algorithm = model_config.clustering_algorithm
        self.min_cluster_size = model_config.min_cluster_size
        self.min_samples = model_config.min_samples
        self.metric = model_config.metric
        
        # Initialize clustering model
        self.clusterer = self._initialize_clusterer()
    
    def process(self, data: pd.DataFrame, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Cluster entities and analyze clusters
        
        Args:
            data: DataFrame with entities and embeddings
            **kwargs: Additional keyword arguments
            
        Returns:
            Tuple of (DataFrame with cluster labels, cluster analysis results)
        """
        self.log_stage_start(f"Processing {len(data)} entities")
        
        try:
            if data.empty:
                self.logger.warning("No data to cluster")
                return data, {}
            
            # Filter entities with embeddings
            entities_with_embeddings = data[data['embedding'].notna()].copy()
            
            if len(entities_with_embeddings) == 0:
                self.logger.warning("No entities with embeddings found")
                return data, {}
            
            # Extract embeddings
            embeddings = np.array(entities_with_embeddings['embedding'].tolist())
            
            # Perform clustering
            cluster_labels = self._cluster_embeddings(embeddings)
            
            # Add cluster labels to data
            entities_with_embeddings['cluster_id'] = cluster_labels
            
            # Merge back with original data
            result_df = data.merge(
                entities_with_embeddings[['dialog_id', 'entity_text', 'cluster_id']], 
                on=['dialog_id', 'entity_text'], 
                how='left'
            )
            
            # Analyze clusters
            cluster_analysis = self._analyze_clusters(entities_with_embeddings, cluster_labels)
            
            self.log_stage_end(f"Clustered {len(entities_with_embeddings)} entities into {len(set(cluster_labels))} clusters")
            return result_df, cluster_analysis
            
        except Exception as e:
            self.log_error(e, "Failed to cluster entities")
            raise
    
    def _initialize_clusterer(self) -> HDBSCAN:
        """Initialize clustering model
        
        Returns:
            Initialized clusterer
        """
        return HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            metric=self.metric,
            cluster_selection_epsilon=0.0
        )
    
    def _cluster_embeddings(self, embeddings: np.ndarray) -> np.ndarray:
        """Cluster embeddings using HDBSCAN
        
        Args:
            embeddings: Array of embeddings
            
        Returns:
            Array of cluster labels
        """
        try:
            self.logger.info(f"Clustering {len(embeddings)} embeddings")
            
            # Fit the clusterer
            cluster_labels = self.clusterer.fit_predict(embeddings)
            
            # Log clustering results
            n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
            n_noise = list(cluster_labels).count(-1)
            
            self.logger.info(f"Found {n_clusters} clusters, {n_noise} noise points")
            
            return cluster_labels
            
        except Exception as e:
            self.log_error(e, "Failed to cluster embeddings")
            raise
    
    def _analyze_clusters(self, df: pd.DataFrame, cluster_labels: np.ndarray) -> Dict[str, Any]:
        """Analyze clustering results
        
        Args:
            df: DataFrame with entities
            cluster_labels: Cluster labels
            
        Returns:
            Dictionary with cluster analysis
        """
        try:
            # Add cluster labels to DataFrame
            df_with_clusters = df.copy()
            df_with_clusters['cluster_id'] = cluster_labels
            
            # Basic cluster statistics
            cluster_stats = df_with_clusters.groupby('cluster_id').agg({
                'entity_text': ['count', 'nunique'],
                'entity_type': lambda x: x.mode().iloc[0] if len(x) > 0 else 'unknown',
                'dialog_id': 'nunique'
            }).round(2)
            
            cluster_stats.columns = ['entity_count', 'unique_entities', 'dominant_type', 'unique_dialogs']
            
            # Calculate silhouette score if we have more than 1 cluster
            unique_clusters = set(cluster_labels)
            if len(unique_clusters) > 1 and -1 not in unique_clusters:
                try:
                    embeddings = np.array(df['embedding'].tolist())
                    silhouette_avg = silhouette_score(embeddings, cluster_labels)
                except:
                    silhouette_avg = None
            else:
                silhouette_avg = None
            
            # Cluster summaries
            cluster_summaries = {}
            for cluster_id in unique_clusters:
                if cluster_id == -1:  # Noise cluster
                    continue
                
                cluster_entities = df_with_clusters[df_with_clusters['cluster_id'] == cluster_id]
                
                # Get most common entity types
                type_counts = cluster_entities['entity_type'].value_counts()
                
                # Get representative entities (most frequent)
                entity_counts = cluster_entities['entity_text'].value_counts()
                
                cluster_summaries[cluster_id] = {
                    'size': len(cluster_entities),
                    'entity_types': type_counts.to_dict(),
                    'top_entities': entity_counts.head(5).to_dict(),
                    'unique_dialogs': cluster_entities['dialog_id'].nunique(),
                    'avg_entities_per_dialog': len(cluster_entities) / cluster_entities['dialog_id'].nunique()
                }
            
            # Overall statistics
            analysis = {
                'total_clusters': len(unique_clusters) - (1 if -1 in unique_clusters else 0),
                'noise_points': list(cluster_labels).count(-1),
                'silhouette_score': silhouette_avg,
                'cluster_statistics': cluster_stats.to_dict('index'),
                'cluster_summaries': cluster_summaries,
                'clustering_parameters': {
                    'algorithm': self.algorithm,
                    'min_cluster_size': self.min_cluster_size,
                    'min_samples': self.min_samples,
                    'metric': self.metric
                }
            }
            
            return analysis
            
        except Exception as e:
            self.log_error(e, "Failed to analyze clusters")
            return {}
    
    def get_cluster_representatives(self, df: pd.DataFrame, cluster_id: int, top_k: int = 5) -> List[Dict[str, Any]]:
        """Get representative entities for a cluster
        
        Args:
            df: DataFrame with clustered entities
            cluster_id: ID of the cluster
            top_k: Number of representatives to return
            
        Returns:
            List of representative entities
        """
        try:
            cluster_entities = df[df['cluster_id'] == cluster_id]
            
            if cluster_entities.empty:
                return []
            
            # Get most frequent entities
            entity_counts = cluster_entities['entity_text'].value_counts()
            
            representatives = []
            for entity, count in entity_counts.head(top_k).items():
                entity_info = cluster_entities[cluster_entities['entity_text'] == entity].iloc[0]
                representatives.append({
                    'entity_text': entity,
                    'frequency': count,
                    'entity_type': entity_info['entity_type'],
                    'dialog_id': entity_info['dialog_id']
                })
            
            return representatives
            
        except Exception as e:
            self.log_error(e, f"Failed to get representatives for cluster {cluster_id}")
            return []
    
    def save_cluster_analysis(self, analysis: Dict[str, Any], filename: str) -> None:
        """Save cluster analysis to file
        
        Args:
            analysis: Cluster analysis results
            filename: Name of the file to save to
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved cluster analysis to {filepath}")
            
        except Exception as e:
            self.log_error(e, f"Failed to save cluster analysis to {filename}")
    
    def load_cluster_analysis(self, filename: str) -> Optional[Dict[str, Any]]:
        """Load cluster analysis from file
        
        Args:
            filename: Name of the file to load from
            
        Returns:
            Cluster analysis dictionary or None if failed
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            filepath = output_dir / filename
            
            if not filepath.exists():
                self.logger.warning(f"Cluster analysis file not found: {filepath}")
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                analysis = json.load(f)
            
            self.logger.info(f"Loaded cluster analysis from {filepath}")
            return analysis
            
        except Exception as e:
            self.log_error(e, f"Failed to load cluster analysis from {filename}")
            return None
