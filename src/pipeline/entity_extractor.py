"""Entity extraction pipeline stage using OpenAI"""

import json
import asyncio
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from openai import AsyncOpenAI
from loguru import logger
import tiktoken

from .base_pipeline import BasePipeline
from ..config import ConfigManager


class EntityExtractor(BasePipeline):
    """Extract entities from dialogs using OpenAI"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize entity extractor
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "entity_extractor")
        
        # Initialize OpenAI client
        openai_config = config_manager.get_openai_config()
        self.client = AsyncOpenAI(api_key=config_manager.config.openai.api_key)
        self.model = openai_config.model
        self.temperature = openai_config.temperature
        self.max_tokens = openai_config.max_tokens
        
        # Initialize tokenizer
        self.tokenizer = tiktoken.encoding_for_model(self.model)
        
        # Load extraction prompt
        self.extraction_prompt = self._load_extraction_prompt()
    
    def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Extract entities from dialogs
        
        Args:
            data: DataFrame with dialogs
            **kwargs: Additional keyword arguments
            
        Returns:
            DataFrame with extracted entities
        """
        self.log_stage_start(f"Processing {len(data)} dialogs")
        
        try:
            # Process dialogs in batches
            processing_config = self.config_manager.get_processing_config()
            batch_size = processing_config.batch_size
            
            results = []
            for i in range(0, len(data), batch_size):
                batch = data.iloc[i:i + batch_size]
                batch_results = asyncio.run(self._process_batch(batch))
                results.extend(batch_results)
                
                self.logger.info(f"Processed batch {i//batch_size + 1}/{(len(data) + batch_size - 1)//batch_size}")
            
            # Add results to DataFrame
            data = data.copy()
            data['extracted_entities'] = results
            
            # Flatten entities for analysis
            flattened_entities = self._flatten_entities(data)
            
            self.log_stage_end(f"Extracted entities from {len(data)} dialogs")
            return data, flattened_entities
            
        except Exception as e:
            self.log_error(e, "Failed to extract entities")
            raise
    
    async def _process_batch(self, batch: pd.DataFrame) -> List[Dict[str, Any]]:
        """Process a batch of dialogs asynchronously
        
        Args:
            batch: Batch of dialogs
            
        Returns:
            List of extracted entities for each dialog
        """
        tasks = []
        for _, row in batch.iterrows():
            task = self._extract_entities_from_dialog(row['text'], row['dialog_id'])
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    async def _extract_entities_from_dialog(self, text: str, dialog_id: int) -> Dict[str, Any]:
        """Extract entities from a single dialog
        
        Args:
            text: Dialog text
            dialog_id: Dialog identifier
            
        Returns:
            Dictionary with extracted entities
        """
        try:
            # Check token limit
            if self._count_tokens(text) > self.max_tokens - 1000:  # Leave room for response
                text = self._truncate_text(text)
            
            # Prepare prompt
            prompt = self.extraction_prompt.format(text=text)
            
            # Call OpenAI API
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert entity extraction system for dialog analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            # Parse response
            content = response.choices[0].message.content
            entities = self._parse_entity_response(content)
            
            return {
                'dialog_id': dialog_id,
                'entities': entities,
                'raw_response': content,
                'extraction_success': True
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract entities from dialog {dialog_id}: {e}")
            return {
                'dialog_id': dialog_id,
                'entities': {},
                'raw_response': str(e),
                'extraction_success': False
            }
    
    def _load_extraction_prompt(self) -> str:
        """Load entity extraction prompt"""
        return """
Проанализируй следующий диалог и извлеки из него все значимые сущности. 

Диалог:
{text}

Извлеки следующие типы сущностей:
1. Проблемы и барьеры (что мешает, какие сложности возникают)
2. Идеи и предложения (что предлагается, какие решения)
3. Упоминания доставки (способы доставки, логистика)
4. Сигналы (индикаторы, признаки чего-либо)
5. Другие важные сущности

Верни результат в формате JSON:
{{
    "проблемы": ["проблема1", "проблема2"],
    "идеи": ["идея1", "идея2"],
    "доставка": ["способ1", "способ2"],
    "сигналы": ["сигнал1", "сигнал2"],
    "другие": ["сущность1", "сущность2"]
}}

Если какой-то тип сущностей не найден, верни пустой массив для этого типа.
"""
    
    def _parse_entity_response(self, response: str) -> Dict[str, List[str]]:
        """Parse entity extraction response
        
        Args:
            response: Raw response from OpenAI
            
        Returns:
            Parsed entities dictionary
        """
        try:
            # Try to find JSON in response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            
            if start_idx != -1 and end_idx != -1:
                json_str = response[start_idx:end_idx]
                entities = json.loads(json_str)
                
                # Ensure all expected keys exist
                expected_keys = ['проблемы', 'идеи', 'доставка', 'сигналы', 'другие']
                for key in expected_keys:
                    if key not in entities:
                        entities[key] = []
                    elif not isinstance(entities[key], list):
                        entities[key] = [entities[key]]
                
                return entities
            else:
                self.logger.warning("No JSON found in response")
                return {key: [] for key in ['проблемы', 'идеи', 'доставка', 'сигналы', 'другие']}
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            return {key: [] for key in ['проблемы', 'идеи', 'доставка', 'сигналы', 'другие']}
    
    def _count_tokens(self, text: str) -> int:
        """Count tokens in text
        
        Args:
            text: Text to count tokens for
            
        Returns:
            Number of tokens
        """
        return len(self.tokenizer.encode(text))
    
    def _truncate_text(self, text: str) -> str:
        """Truncate text to fit token limit
        
        Args:
            text: Text to truncate
            
        Returns:
            Truncated text
        """
        max_tokens = self.max_tokens - 1000  # Leave room for response
        tokens = self.tokenizer.encode(text)
        
        if len(tokens) > max_tokens:
            truncated_tokens = tokens[:max_tokens]
            return self.tokenizer.decode(truncated_tokens)
        
        return text
    
    def _flatten_entities(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Flatten entities for analysis
        
        Args:
            data: DataFrame with extracted entities
            
        Returns:
            List of flattened entity records
        """
        flattened = []
        
        for _, row in data.iterrows():
            dialog_id = row['dialog_id']
            entities = row['extracted_entities']['entities']
            
            for entity_type, entity_list in entities.items():
                for entity in entity_list:
                    flattened.append({
                        'dialog_id': dialog_id,
                        'entity_type': entity_type,
                        'entity_text': entity,
                        'text': row['text'],
                        'extraction_success': row['extracted_entities']['extraction_success']
                    })
        
        return flattened
    
    def get_entity_statistics(self, flattened_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about extracted entities
        
        Args:
            flattened_entities: List of flattened entity records
            
        Returns:
            Dictionary with entity statistics
        """
        if not flattened_entities:
            return {}
        
        df = pd.DataFrame(flattened_entities)
        
        stats = {
            'total_entities': len(df),
            'entities_by_type': df['entity_type'].value_counts().to_dict(),
            'successful_extractions': df['extraction_success'].sum(),
            'failed_extractions': (~df['extraction_success']).sum(),
            'unique_entities': df['entity_text'].nunique(),
            'avg_entities_per_dialog': len(df) / df['dialog_id'].nunique() if len(df) > 0 else 0
        }
        
        return stats
