"""Base pipeline class for dialog processing"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from loguru import logger

from ..config import ConfigManager


class BasePipeline(ABC):
    """Base class for all pipeline stages"""
    
    def __init__(self, config_manager: ConfigManager, stage_name: str):
        """Initialize base pipeline
        
        Args:
            config_manager: Configuration manager instance
            stage_name: Name of the pipeline stage
        """
        self.config_manager = config_manager
        self.stage_name = stage_name
        self.logger = logger.bind(stage=stage_name)
        
    @abstractmethod
    def process(self, data: Any, **kwargs) -> Any:
        """Process data through the pipeline stage
        
        Args:
            data: Input data to process
            **kwargs: Additional keyword arguments
            
        Returns:
            Processed data
        """
        pass
    
    def validate_input(self, data: Any) -> bool:
        """Validate input data
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        if data is None:
            self.logger.error("Input data is None")
            return False
        return True
    
    def log_stage_start(self, data_info: str = "") -> None:
        """Log stage start"""
        self.logger.info(f"Starting {self.stage_name} stage", extra={"data_info": data_info})
    
    def log_stage_end(self, result_info: str = "") -> None:
        """Log stage completion"""
        self.logger.info(f"Completed {self.stage_name} stage", extra={"result_info": result_info})
    
    def log_error(self, error: Exception, context: str = "") -> None:
        """Log error with context"""
        self.logger.error(f"Error in {self.stage_name}: {str(error)}", 
                         extra={"context": context, "error_type": type(error).__name__})
    
    def save_intermediate_result(self, data: Any, filename: str) -> None:
        """Save intermediate result to file
        
        Args:
            data: Data to save
            filename: Name of the file to save to
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = output_dir / filename
            
            if isinstance(data, pd.DataFrame):
                data.to_csv(filepath, index=False)
            elif isinstance(data, dict):
                import json
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            elif isinstance(data, list):
                import json
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                # Try to save as pickle
                import pickle
                with open(filepath, 'wb') as f:
                    pickle.dump(data, f)
            
            self.logger.info(f"Saved intermediate result to {filepath}")
            
        except Exception as e:
            self.log_error(e, f"Failed to save intermediate result to {filename}")
    
    def load_intermediate_result(self, filename: str) -> Optional[Any]:
        """Load intermediate result from file
        
        Args:
            filename: Name of the file to load
            
        Returns:
            Loaded data or None if failed
        """
        try:
            output_dir = Path(self.config_manager.get_paths_config().output_dir)
            filepath = output_dir / filename
            
            if not filepath.exists():
                self.logger.warning(f"Intermediate result file not found: {filepath}")
                return None
            
            # Try different file formats
            if filename.endswith('.csv'):
                return pd.read_csv(filepath)
            elif filename.endswith('.json'):
                import json
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            elif filename.endswith('.pkl'):
                import pickle
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            else:
                self.logger.error(f"Unsupported file format: {filename}")
                return None
                
        except Exception as e:
            self.log_error(e, f"Failed to load intermediate result from {filename}")
            return None
