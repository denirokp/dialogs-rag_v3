"""Main pipeline orchestrator for dialog processing"""

import pandas as pd
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
from loguru import logger
import json
from datetime import datetime

from .base_pipeline import BasePipeline
from .dialog_processor import DialogProcessor
from .entity_extractor import EntityExtractor
from .embedding_generator import EmbeddingGenerator
from .cluster_analyzer import ClusterAnalyzer
from ..config import ConfigManager


class MainPipeline(BasePipeline):
    """Main pipeline orchestrator for the entire dialog processing workflow"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize main pipeline
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "main_pipeline")
        
        # Initialize pipeline stages
        self.dialog_processor = DialogProcessor(config_manager)
        self.entity_extractor = EntityExtractor(config_manager)
        self.embedding_generator = EmbeddingGenerator(config_manager)
        self.cluster_analyzer = ClusterAnalyzer(config_manager)
        
        # Pipeline stages in order
        self.stages = [
            ("dialog_processing", self.dialog_processor),
            ("entity_extraction", self.entity_extractor),
            ("embedding_generation", self.embedding_generator),
            ("cluster_analysis", self.cluster_analyzer)
        ]
    
    def process(self, data: Any, **kwargs) -> Dict[str, Any]:
        """Process data through the entire pipeline
        
        Args:
            data: Input data (file path, DataFrame, or list of dialogs)
            **kwargs: Additional keyword arguments
            
        Returns:
            Dictionary with all pipeline results
        """
        self.log_stage_start()
        
        try:
            results = {
                'pipeline_start_time': datetime.now().isoformat(),
                'stages': {},
                'final_results': {},
                'statistics': {},
                'errors': []
            }
            
            current_data = data
            
            # Process through each stage
            for stage_name, stage in self.stages:
                try:
                    self.logger.info(f"Starting stage: {stage_name}")
                    stage_start_time = datetime.now()
                    
                    if stage_name == "dialog_processing":
                        current_data = stage.process(current_data, **kwargs)
                        results['stages'][stage_name] = {
                            'status': 'completed',
                            'start_time': stage_start_time.isoformat(),
                            'end_time': datetime.now().isoformat(),
                            'data_shape': current_data.shape if hasattr(current_data, 'shape') else len(current_data)
                        }
                    
                    elif stage_name == "entity_extraction":
                        current_data, flattened_entities = stage.process(current_data, **kwargs)
                        results['stages'][stage_name] = {
                            'status': 'completed',
                            'start_time': stage_start_time.isoformat(),
                            'end_time': datetime.now().isoformat(),
                            'data_shape': current_data.shape if hasattr(current_data, 'shape') else len(current_data),
                            'flattened_entities_count': len(flattened_entities)
                        }
                        results['flattened_entities'] = flattened_entities
                    
                    elif stage_name == "embedding_generation":
                        current_data = stage.process(current_data, **kwargs)
                        results['stages'][stage_name] = {
                            'status': 'completed',
                            'start_time': stage_start_time.isoformat(),
                            'end_time': datetime.now().isoformat(),
                            'data_shape': current_data.shape if hasattr(current_data, 'shape') else len(current_data)
                        }
                    
                    elif stage_name == "cluster_analysis":
                        current_data, cluster_analysis = stage.process(current_data, **kwargs)
                        results['stages'][stage_name] = {
                            'status': 'completed',
                            'start_time': stage_start_time.isoformat(),
                            'end_time': datetime.now().isoformat(),
                            'data_shape': current_data.shape if hasattr(current_data, 'shape') else len(current_data)
                        }
                        results['cluster_analysis'] = cluster_analysis
                    
                    # Save intermediate results
                    self._save_stage_result(stage_name, current_data)
                    
                except Exception as e:
                    error_msg = f"Error in stage {stage_name}: {str(e)}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['stages'][stage_name] = {
                        'status': 'failed',
                        'start_time': stage_start_time.isoformat(),
                        'end_time': datetime.now().isoformat(),
                        'error': str(e)
                    }
                    
                    # Continue with next stage or stop based on criticality
                    if stage_name in ["dialog_processing"]:
                        raise e  # Critical stages
                    else:
                        continue
            
            # Generate final results
            results['final_results'] = self._generate_final_results(current_data, results)
            results['statistics'] = self._generate_statistics(results)
            results['pipeline_end_time'] = datetime.now().isoformat()
            
            # Save final results
            self._save_final_results(results)
            
            self.log_stage_end("Pipeline completed successfully")
            return results
            
        except Exception as e:
            self.log_error(e, "Pipeline failed")
            results['pipeline_end_time'] = datetime.now().isoformat()
            results['status'] = 'failed'
            raise
    
    def _save_stage_result(self, stage_name: str, data: Any) -> None:
        """Save intermediate stage result
        
        Args:
            stage_name: Name of the stage
            data: Data to save
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{stage_name}_{timestamp}.pkl"
            
            if isinstance(data, pd.DataFrame):
                output_dir = Path(self.config_manager.get_paths_config().output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                filepath = output_dir / filename
                data.to_pickle(filepath)
                self.logger.info(f"Saved {stage_name} result to {filepath}")
            
        except Exception as e:
            self.logger.warning(f"Failed to save {stage_name} result: {e}")
    
    def _generate_final_results(self, data: pd.DataFrame, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate final results summary
        
        Args:
            data: Final processed data
            results: Pipeline results
            
        Returns:
            Dictionary with final results
        """
        try:
            final_results = {
                'total_dialogs': len(data['dialog_id'].unique()) if 'dialog_id' in data.columns else 0,
                'total_entities': len(data) if not data.empty else 0,
                'entity_types': data['entity_type'].value_counts().to_dict() if 'entity_type' in data.columns else {},
                'clusters_found': results.get('cluster_analysis', {}).get('total_clusters', 0),
                'processing_time': self._calculate_processing_time(results),
                'success_rate': self._calculate_success_rate(results)
            }
            
            return final_results
            
        except Exception as e:
            self.log_error(e, "Failed to generate final results")
            return {}
    
    def _generate_statistics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive statistics
        
        Args:
            results: Pipeline results
            
        Returns:
            Dictionary with statistics
        """
        try:
            stats = {
                'pipeline_duration': self._calculate_processing_time(results),
                'stages_completed': len([s for s in results['stages'].values() if s['status'] == 'completed']),
                'stages_failed': len([s for s in results['stages'].values() if s['status'] == 'failed']),
                'total_errors': len(results.get('errors', [])),
                'stage_details': results['stages']
            }
            
            return stats
            
        except Exception as e:
            self.log_error(e, "Failed to generate statistics")
            return {}
    
    def _calculate_processing_time(self, results: Dict[str, Any]) -> Optional[str]:
        """Calculate total processing time
        
        Args:
            results: Pipeline results
            
        Returns:
            Processing time as string or None
        """
        try:
            start_time = datetime.fromisoformat(results['pipeline_start_time'])
            end_time = datetime.fromisoformat(results['pipeline_end_time'])
            duration = end_time - start_time
            return str(duration)
        except:
            return None
    
    def _calculate_success_rate(self, results: Dict[str, Any]) -> float:
        """Calculate pipeline success rate
        
        Args:
            results: Pipeline results
            
        Returns:
            Success rate as float (0.0 to 1.0)
        """
        try:
            total_stages = len(results['stages'])
            successful_stages = len([s for s in results['stages'].values() if s['status'] == 'completed'])
            return successful_stages / total_stages if total_stages > 0 else 0.0
        except:
            return 0.0
    
    def _save_final_results(self, results: Dict[str, Any]) -> None:
        """Save final results to file
        
        Args:
            results: Final pipeline results
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pipeline_results_{timestamp}.json"
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            
            self.logger.info(f"Saved final results to {filepath}")
            
        except Exception as e:
            self.log_error(e, "Failed to save final results")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status
        
        Returns:
            Dictionary with pipeline status
        """
        return {
            'pipeline_name': 'main_pipeline',
            'stages': [stage_name for stage_name, _ in self.stages],
            'config_loaded': self.config_manager is not None,
            'directories_created': self._check_directories()
        }
    
    def _check_directories(self) -> bool:
        """Check if required directories exist
        
        Returns:
            True if all directories exist, False otherwise
        """
        try:
            paths_config = self.config_manager.get_paths_config()
            required_dirs = [
                paths_config.data_dir,
                paths_config.input_dir,
                paths_config.output_dir,
                paths_config.logs_dir
            ]
            
            return all(Path(d).exists() for d in required_dirs)
        except:
            return False
