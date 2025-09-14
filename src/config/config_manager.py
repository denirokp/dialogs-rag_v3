"""Configuration management for the Dialogs RAG system"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class OpenAIConfig(BaseModel):
    """OpenAI API configuration"""
    api_key: str = os.getenv("OPENAI_API_KEY", "")
    model: str = "gpt-4"
    temperature: float = 0.1
    max_tokens: int = 4000
    timeout: int = 60


class ProcessingConfig(BaseModel):
    """Processing configuration"""
    batch_size: int = 10
    max_workers: int = 4
    chunk_size: int = 1000
    retry_attempts: int = 3
    retry_delay: int = 1


class ModelConfig(BaseModel):
    """Model configuration"""
    embedding_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_dimension: int = 384
    clustering_algorithm: str = "hdbscan"
    min_cluster_size: int = 3
    min_samples: int = 2
    metric: str = "cosine"


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
    file: str = "logs/pipeline.log"
    max_size: str = "10 MB"
    retention: str = "7 days"


class PathsConfig(BaseModel):
    """Paths configuration"""
    data_dir: str = "data"
    input_dir: str = "data/input"
    output_dir: str = "data/output"
    logs_dir: str = "logs"
    models_dir: str = "models"


class Config(BaseModel):
    """Main configuration class"""
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    models: ModelConfig = Field(default_factory=ModelConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)


class ConfigManager:
    """Configuration manager for the Dialogs RAG system"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration manager
        
        Args:
            config_path: Path to configuration file. If None, uses default config.yaml
        """
        self.config_path = config_path or "config.yaml"
        self._config: Optional[Config] = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                
                # Extract nested configurations
                openai_data = config_data.get('openai', {})
                processing_data = config_data.get('processing', {})
                models_data = config_data.get('models', {})
                logging_data = config_data.get('logging', {})
                paths_data = config_data.get('paths', {})
                
                # Create model instances
                openai_config = OpenAIConfig(**openai_data)
                processing_config = ProcessingConfig(**processing_data)
                
                # Handle nested model config
                embedding_data = models_data.get('embedding', {})
                clustering_data = models_data.get('clustering', {})
                
                model_config = ModelConfig(
                    embedding_name=embedding_data.get('name', 'sentence-transformers/all-MiniLM-L6-v2'),
                    embedding_dimension=embedding_data.get('dimension', 384),
                    clustering_algorithm=clustering_data.get('algorithm', 'hdbscan'),
                    min_cluster_size=clustering_data.get('min_cluster_size', 3),
                    min_samples=clustering_data.get('min_samples', 2),
                    metric=clustering_data.get('metric', 'cosine')
                )
                
                logging_config = LoggingConfig(**logging_data)
                paths_config = PathsConfig(**paths_data)
                
                self._config = Config(
                    openai=openai_config,
                    processing=processing_config,
                    models=model_config,
                    logging=logging_config,
                    paths=paths_config
                )
            else:
                # Use default configuration
                self._config = Config()
                
        except Exception as e:
            print(f"Warning: Could not load configuration from {self.config_path}: {e}")
            print("Using default configuration")
            self._config = Config()
    
    @property
    def config(self) -> Config:
        """Get current configuration"""
        if self._config is None:
            self._load_config()
        return self._config
    
    def get_openai_config(self) -> OpenAIConfig:
        """Get OpenAI configuration"""
        return self.config.openai
    
    def get_processing_config(self) -> ProcessingConfig:
        """Get processing configuration"""
        return self.config.processing
    
    def get_model_config(self) -> ModelConfig:
        """Get model configuration"""
        return self.config.models
    
    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration"""
        return self.config.logging
    
    def get_paths_config(self) -> PathsConfig:
        """Get paths configuration"""
        return self.config.paths
    
    def reload(self) -> None:
        """Reload configuration from file"""
        self._load_config()
    
    def create_directories(self) -> None:
        """Create necessary directories based on configuration"""
        paths = self.get_paths_config()
        
        directories = [
            paths.data_dir,
            paths.input_dir,
            paths.output_dir,
            paths.logs_dir,
            paths.models_dir
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
