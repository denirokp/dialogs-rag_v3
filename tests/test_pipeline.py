"""Tests for pipeline modules"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.config import ConfigManager
from src.pipeline import DialogProcessor, EntityExtractor, EmbeddingGenerator, ClusterAnalyzer


class TestDialogProcessor:
    """Test DialogProcessor class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.config_manager = Mock(spec=ConfigManager)
        self.processor = DialogProcessor(self.config_manager)
    
    def test_process_dataframe(self):
        """Test processing DataFrame input"""
        data = pd.DataFrame({
            'text': ['Test dialog 1', 'Test dialog 2'],
            'dialog_id': [1, 2]
        })
        
        result = self.processor.process(data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'word_count' in result.columns
        assert 'char_count' in result.columns
    
    def test_process_dialog_list(self):
        """Test processing list of dialogs"""
        data = [
            {'text': 'Test dialog 1', 'dialog_id': 1},
            {'text': 'Test dialog 2', 'dialog_id': 2}
        ]
        
        result = self.processor.process(data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
    
    def test_validate_and_clean(self):
        """Test data validation and cleaning"""
        data = pd.DataFrame({
            'text': ['Valid text', '', 'Another valid text'],
            'dialog_id': [1, 2, 3]
        })
        
        result = self.processor._validate_and_clean(data)
        
        assert len(result) == 2  # Empty text should be removed
        assert 'word_count' in result.columns
    
    def test_detect_language(self):
        """Test language detection"""
        # Test Russian text
        russian_text = "Это русский текст"
        assert self.processor._detect_language(russian_text) == 'ru'
        
        # Test English text
        english_text = "This is English text"
        assert self.processor._detect_language(english_text) == 'en'


class TestEntityExtractor:
    """Test EntityExtractor class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.config_manager = Mock(spec=ConfigManager)
        self.config_manager.get_openai_config.return_value = Mock(
            model="gpt-3.5-turbo",
            temperature=0.1,
            max_tokens=1000
        )
        self.config_manager.get_processing_config.return_value = Mock(
            batch_size=2
        )
        self.config_manager.config.openai.api_key = "test-key"
        
        self.extractor = EntityExtractor(self.config_manager)
    
    def test_parse_entity_response(self):
        """Test parsing entity extraction response"""
        response = '''
        {
            "проблемы": ["проблема1", "проблема2"],
            "идеи": ["идея1"],
            "доставка": [],
            "сигналы": ["сигнал1"],
            "другие": []
        }
        '''
        
        result = self.extractor._parse_entity_response(response)
        
        assert isinstance(result, dict)
        assert "проблемы" in result
        assert len(result["проблемы"]) == 2
        assert result["доставка"] == []
    
    def test_count_tokens(self):
        """Test token counting"""
        text = "This is a test text"
        count = self.extractor._count_tokens(text)
        assert isinstance(count, int)
        assert count > 0
    
    def test_truncate_text(self):
        """Test text truncation"""
        # Create a long text
        long_text = "word " * 1000
        
        truncated = self.extractor._truncate_text(long_text)
        assert len(truncated) < len(long_text)
        assert self.extractor._count_tokens(truncated) <= self.extractor.max_tokens - 1000


class TestEmbeddingGenerator:
    """Test EmbeddingGenerator class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.config_manager = Mock(spec=ConfigManager)
        self.config_manager.get_model_config.return_value = Mock(
            embedding_name="sentence-transformers/all-MiniLM-L6-v2",
            embedding_dimension=384
        )
        self.config_manager.get_paths_config.return_value = Mock(
            output_dir="test_output"
        )
        
        self.generator = EmbeddingGenerator(self.config_manager)
    
    def test_generate_embeddings(self):
        """Test embedding generation"""
        texts = ["Test text 1", "Test text 2"]
        
        # Mock the model
        with patch.object(self.generator, 'model') as mock_model:
            mock_model.encode.return_value = [[0.1] * 384, [0.2] * 384]
            
            embeddings = self.generator._generate_embeddings(texts)
            
            assert len(embeddings) == 2
            assert len(embeddings[0]) == 384
    
    def test_get_embedding_statistics(self):
        """Test embedding statistics calculation"""
        data = pd.DataFrame({
            'entity_type': ['проблемы', 'идеи'],
            'embedding': [[0.1] * 384, [0.2] * 384],
            'extraction_success': [True, True]
        })
        
        stats = self.generator.get_embedding_statistics(data)
        
        assert 'total_embeddings' in stats
        assert 'embedding_dimension' in stats
        assert stats['total_embeddings'] == 2


class TestClusterAnalyzer:
    """Test ClusterAnalyzer class"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.config_manager = Mock(spec=ConfigManager)
        self.config_manager.get_model_config.return_value = Mock(
            clustering_algorithm="hdbscan",
            min_cluster_size=2,
            min_samples=1,
            metric="cosine"
        )
        self.config_manager.get_paths_config.return_value = Mock(
            output_dir="test_output"
        )
        
        self.analyzer = ClusterAnalyzer(self.config_manager)
    
    def test_cluster_embeddings(self):
        """Test clustering embeddings"""
        import numpy as np
        
        # Create test embeddings
        embeddings = np.random.rand(10, 384)
        
        cluster_labels = self.analyzer._cluster_embeddings(embeddings)
        
        assert len(cluster_labels) == 10
        assert isinstance(cluster_labels, np.ndarray)
    
    def test_analyze_clusters(self):
        """Test cluster analysis"""
        data = pd.DataFrame({
            'entity_text': ['text1', 'text2', 'text3'],
            'entity_type': ['проблемы', 'проблемы', 'идеи'],
            'dialog_id': [1, 2, 3]
        })
        
        cluster_labels = [0, 0, 1]
        
        analysis = self.analyzer._analyze_clusters(data, cluster_labels)
        
        assert 'total_clusters' in analysis
        assert 'cluster_statistics' in analysis
        assert 'cluster_summaries' in analysis


if __name__ == "__main__":
    pytest.main([__file__])
