#!/usr/bin/env python3
"""Test script for Dialogs RAG System v2.0 without OpenAI"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.config import ConfigManager
from src.pipeline import DialogProcessor


def test_dialog_processor():
    """Test dialog processor without OpenAI"""
    print("🧪 Testing Dialog Processor...")
    
    # Initialize configuration
    config_manager = ConfigManager()
    
    # Create test data
    test_data = pd.DataFrame({
        'text': [
            'Проблема с доставкой заказа 12345. Нужно улучшить логистику.',
            'Предлагаю использовать курьерскую службу для быстрой доставки.',
            'Сигнал: клиенты жалуются на медленную доставку.',
            'Идея: создать систему отслеживания заказов в реальном времени.'
        ],
        'dialog_id': [1, 2, 3, 4]
    })
    
    # Test dialog processor
    processor = DialogProcessor(config_manager)
    result = processor.process(test_data)
    
    print(f"✅ Processed {len(result)} dialogs")
    print(f"✅ Columns: {list(result.columns)}")
    print(f"✅ Word counts: {result['word_count'].tolist()}")
    print(f"✅ Languages: {result['language'].value_counts().to_dict()}")
    
    # Test statistics
    stats = processor.get_statistics(result)
    print(f"✅ Statistics: {stats}")
    
    return result


def test_config_manager():
    """Test configuration manager"""
    print("\n🧪 Testing Configuration Manager...")
    
    config_manager = ConfigManager()
    
    # Test configuration access
    openai_config = config_manager.get_openai_config()
    processing_config = config_manager.get_processing_config()
    model_config = config_manager.get_model_config()
    logging_config = config_manager.get_logging_config()
    paths_config = config_manager.get_paths_config()
    
    print(f"✅ OpenAI model: {openai_config.model}")
    print(f"✅ Batch size: {processing_config.batch_size}")
    print(f"✅ Embedding model: {model_config.embedding_name}")
    print(f"✅ Log level: {logging_config.level}")
    print(f"✅ Data dir: {paths_config.data_dir}")
    
    # Test directory creation
    config_manager.create_directories()
    print("✅ Directories created successfully")
    
    return config_manager


def main():
    """Main test function"""
    print("🚀 Testing Dialogs RAG System v2.0")
    print("=" * 50)
    
    try:
        # Test configuration
        config_manager = test_config_manager()
        
        # Test dialog processor
        result = test_dialog_processor()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed successfully!")
        print("\nSystem is ready for use!")
        print("\nNext steps:")
        print("1. Add your OpenAI API key to .env file")
        print("2. Run: python main.py --input data/input/dialogs.xlsx")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
