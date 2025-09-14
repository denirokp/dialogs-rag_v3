"""Dialog processing pipeline stage"""

import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
import openpyxl
from loguru import logger

from .base_pipeline import BasePipeline
from ..config import ConfigManager


class DialogProcessor(BasePipeline):
    """Process and validate dialog data"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize dialog processor
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "dialog_processor")
    
    def process(self, data: Any, **kwargs) -> pd.DataFrame:
        """Process dialog data
        
        Args:
            data: Input data (file path, DataFrame, or list of dialogs)
            **kwargs: Additional keyword arguments
            
        Returns:
            Processed DataFrame with dialogs
        """
        self.log_stage_start()
        
        try:
            # Handle different input types
            if isinstance(data, str):
                # File path
                dialogs_df = self._load_from_file(data)
            elif isinstance(data, pd.DataFrame):
                # Already a DataFrame
                dialogs_df = data.copy()
            elif isinstance(data, list):
                # List of dialogs
                dialogs_df = self._process_dialog_list(data)
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            # Validate and clean data
            dialogs_df = self._validate_and_clean(dialogs_df)
            
            # Add metadata
            dialogs_df = self._add_metadata(dialogs_df)
            
            self.log_stage_end(f"Processed {len(dialogs_df)} dialogs")
            return dialogs_df
            
        except Exception as e:
            self.log_error(e, "Failed to process dialogs")
            raise
    
    def _load_from_file(self, file_path: str) -> pd.DataFrame:
        """Load dialogs from file
        
        Args:
            file_path: Path to the input file
            
        Returns:
            DataFrame with dialogs
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.logger.info(f"Loading dialogs from {file_path}")
        
        if file_path.suffix.lower() == '.xlsx':
            return self._load_from_excel(file_path)
        elif file_path.suffix.lower() == '.csv':
            return pd.read_csv(file_path)
        elif file_path.suffix.lower() == '.json':
            import json
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _load_from_excel(self, file_path: Path) -> pd.DataFrame:
        """Load dialogs from Excel file
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            DataFrame with dialogs
        """
        try:
            # Try to read with pandas first
            df = pd.read_excel(file_path)
            return df
        except Exception as e:
            self.logger.warning(f"Pandas failed to read Excel file: {e}")
            
            # Fallback to openpyxl
            try:
                workbook = openpyxl.load_workbook(file_path, read_only=True)
                worksheet = workbook.active
                
                # Convert to DataFrame
                data = []
                headers = []
                
                for row_idx, row in enumerate(worksheet.iter_rows(values_only=True)):
                    if row_idx == 0:
                        headers = [str(cell) if cell is not None else f"col_{i}" for i, cell in enumerate(row)]
                    else:
                        data.append([str(cell) if cell is not None else "" for cell in row])
                
                return pd.DataFrame(data, columns=headers)
                
            except Exception as e2:
                raise Exception(f"Failed to read Excel file with both pandas and openpyxl: {e2}")
    
    def _process_dialog_list(self, dialogs: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process list of dialog dictionaries
        
        Args:
            dialogs: List of dialog dictionaries
            
        Returns:
            DataFrame with dialogs
        """
        return pd.DataFrame(dialogs)
    
    def _validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean dialog data
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Validating and cleaning dialog data")
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Ensure we have required columns
        required_columns = ['text', 'dialog_id']
        
        # Check if we have text column (might be named differently)
        text_columns = [col for col in df.columns if 'text' in col.lower() or 'message' in col.lower() or 'content' in col.lower()]
        if not text_columns:
            raise ValueError("No text column found. Expected column names containing 'text', 'message', or 'content'")
        
        # Rename the first text column to 'text'
        df = df.rename(columns={text_columns[0]: 'text'})
        
        # Add dialog_id if not present
        if 'dialog_id' not in df.columns:
            df['dialog_id'] = range(len(df))
        
        # Clean text data
        df['text'] = df['text'].astype(str).str.strip()
        
        # Remove empty texts
        df = df[df['text'].str.len() > 0]
        
        # Add word count
        df['word_count'] = df['text'].str.split().str.len()
        
        # Add character count
        df['char_count'] = df['text'].str.len()
        
        self.logger.info(f"Cleaned data: {len(df)} dialogs remaining")
        return df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata to dialogs
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with added metadata
        """
        # Add processing timestamp
        from datetime import datetime
        df['processed_at'] = datetime.now().isoformat()
        
        # Add source information
        df['source'] = 'dialog_processor'
        
        # Add language detection (simple heuristic)
        df['language'] = df['text'].apply(self._detect_language)
        
        return df
    
    def _detect_language(self, text: str) -> str:
        """Simple language detection
        
        Args:
            text: Text to analyze
            
        Returns:
            Detected language code
        """
        # Simple heuristic based on character sets
        cyrillic_chars = sum(1 for char in text if '\u0400' <= char <= '\u04FF')
        latin_chars = sum(1 for char in text if char.isalpha() and ord(char) < 128)
        
        if cyrillic_chars > latin_chars:
            return 'ru'
        else:
            return 'en'
    
    def get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get statistics about the dialog data
        
        Args:
            df: Dialog DataFrame
            
        Returns:
            Dictionary with statistics
        """
        return {
            'total_dialogs': len(df),
            'total_words': df['word_count'].sum(),
            'total_characters': df['char_count'].sum(),
            'avg_words_per_dialog': df['word_count'].mean(),
            'avg_chars_per_dialog': df['char_count'].mean(),
            'languages': df['language'].value_counts().to_dict(),
            'empty_dialogs': (df['text'].str.len() == 0).sum()
        }
