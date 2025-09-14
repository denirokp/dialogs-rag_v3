"""Enhanced entity extractor with improved quality and context awareness"""

import json
import asyncio
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from openai import AsyncOpenAI
from loguru import logger
import tiktoken
import re

from .base_pipeline import BasePipeline
from ..config import ConfigManager


class EnhancedEntityExtractor(BasePipeline):
    """Enhanced entity extractor with improved quality and context awareness"""
    
    def __init__(self, config_manager: ConfigManager):
        """Initialize enhanced entity extractor
        
        Args:
            config_manager: Configuration manager instance
        """
        super().__init__(config_manager, "enhanced_entity_extractor")
        
        # Initialize OpenAI client
        openai_config = config_manager.get_openai_config()
        self.client = AsyncOpenAI(api_key=openai_config.api_key)
        self.model = openai_config.model
        self.temperature = openai_config.temperature
        self.max_tokens = openai_config.max_tokens
        
        # Initialize tokenizer
        self.tokenizer = tiktoken.encoding_for_model(self.model)
        
        # Load enhanced extraction prompt
        self.extraction_prompt = self._load_enhanced_extraction_prompt()
    
    def process(self, data: pd.DataFrame, **kwargs) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Extract entities from dialogs with enhanced quality
        
        Args:
            data: DataFrame with dialogs
            **kwargs: Additional keyword arguments
            
        Returns:
            Tuple of (DataFrame with extracted entities, flattened entities)
        """
        self.log_stage_start(f"Processing {len(data)} dialogs with enhanced extraction")
        
        try:
            # Process dialogs in batches
            processing_config = self.config_manager.get_processing_config()
            batch_size = processing_config.batch_size
            
            results = []
            for i in range(0, len(data), batch_size):
                batch = data.iloc[i:i + batch_size]
                batch_results = asyncio.run(self._process_batch_enhanced(batch))
                results.extend(batch_results)
                
                self.logger.info(f"Processed enhanced batch {i//batch_size + 1}/{(len(data) + batch_size - 1)//batch_size}")
            
            # Add results to DataFrame
            data = data.copy()
            data['extracted_entities'] = results
            
            # Flatten entities for analysis
            flattened_entities = self._flatten_enhanced_entities(data)
            
            self.log_stage_end(f"Enhanced extraction completed for {len(data)} dialogs")
            return data, flattened_entities
            
        except Exception as e:
            self.log_error(e, "Failed to extract entities with enhanced method")
            raise
    
    async def _process_batch_enhanced(self, batch: pd.DataFrame) -> List[Dict[str, Any]]:
        """Process a batch of dialogs with enhanced extraction
        
        Args:
            batch: Batch of dialogs
            
        Returns:
            List of enhanced extracted entities for each dialog
        """
        tasks = []
        for _, row in batch.iterrows():
            task = self._extract_entities_enhanced(row['text'], row['dialog_id'])
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    async def _extract_entities_enhanced(self, text: str, dialog_id: int) -> Dict[str, Any]:
        """Extract entities with enhanced quality and context awareness
        
        Args:
            text: Dialog text
            dialog_id: Dialog identifier
            
        Returns:
            Dictionary with enhanced extracted entities
        """
        try:
            # Check token limit
            if self._count_tokens(text) > self.max_tokens - 2000:  # Leave more room for response
                text = self._truncate_text(text)
            
            # Prepare enhanced prompt
            prompt = self.extraction_prompt.format(text=text)
            
            # Call OpenAI API
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert dialog analyst specializing in customer service and business communication analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            # Parse response
            content = response.choices[0].message.content
            entities = self._parse_enhanced_entity_response(content)
            
            return {
                'dialog_id': dialog_id,
                'entities': entities,
                'raw_response': content,
                'extraction_success': True,
                'extraction_method': 'enhanced'
            }
            
        except Exception as e:
            self.logger.error(f"Failed to extract entities from dialog {dialog_id}: {e}")
            return {
                'dialog_id': dialog_id,
                'entities': {},
                'raw_response': str(e),
                'extraction_success': False,
                'extraction_method': 'enhanced'
            }
    
    def _load_enhanced_extraction_prompt(self) -> str:
        """Load enhanced entity extraction prompt"""
        return """
Проанализируй следующий диалог между оператором и клиентом. Внимательно раздели реплики оператора и клиента, и извлеки информацию ТОЛЬКО из реплик клиента.

Диалог:
{text}

ЗАДАЧА: Извлеки детальную информацию из реплик клиента по следующим категориям:

1. ПРОБЛЕМЫ И БАРЬЕРЫ (только из реплик клиента):
   - Технические проблемы (сбои, ошибки, неработающие функции)
   - Проблемы с доставкой (конкретные случаи, что не работает)
   - Экономические проблемы (неокупаемость, высокие затраты)
   - Операционные проблемы (сложности в работе, неэффективность)
   - Проблемы с поддержкой (нерешенные обращения)

2. ИДЕИ И ПРЕДЛОЖЕНИЯ (только из реплик клиента):
   - Конкретные предложения по улучшению
   - Запросы на помощь или консультацию
   - Предложения по решению проблем
   - Пожелания по развитию сервиса

3. ДОСТАВКА (только из реплик клиента):
   - Упоминания конкретных служб доставки
   - Проблемы с доставкой (что именно не работает)
   - Предпочтения по доставке
   - Опыт использования разных служб

4. СИГНАЛЫ И ИНДИКАТОРЫ (только из реплик клиента):
   - Метрики эффективности (просмотры, продажи, конверсия)
   - Финансовые показатели (затраты, доходы, окупаемость)
   - Поведенческие сигналы (предпочтения, привычки)
   - Технические сигналы (статистика использования)

5. ДРУГИЕ ВАЖНЫЕ СУЩНОСТИ (только из реплик клиента):
   - Конкретные товары/услуги
   - Цены и тарифы
   - Конкурентов и сравнения
   - Временные рамки и сроки
   - Географические данные

ВАЖНО:
- Анализируй ТОЛЬКО реплики клиента (ищи "Клиент:" или аналогичные маркеры)
- Извлекай конкретные факты, а не общие фразы
- Указывай контекст и детали
- Группируй связанные проблемы
- Выделяй экономические аспекты (затраты, окупаемость, прибыль)

Верни результат в формате JSON:
{{
    "проблемы": [
        {{
            "категория": "техническая/доставка/экономическая/операционная/поддержка",
            "описание": "конкретное описание проблемы",
            "контекст": "дополнительные детали",
            "критичность": "высокая/средняя/низкая"
        }}
    ],
    "идеи": [
        {{
            "тип": "улучшение/запрос/предложение/пожелание",
            "описание": "конкретное предложение",
            "контекст": "в каких условиях упоминается"
        }}
    ],
    "доставка": [
        {{
            "служба": "название службы доставки",
            "статус": "работает/не работает/проблемы",
            "детали": "конкретные проблемы или предпочтения"
        }}
    ],
    "сигналы": [
        {{
            "тип": "метрика/финансы/поведение/техника",
            "показатель": "конкретное значение или описание",
            "контекст": "в каких условиях упоминается"
        }}
    ],
    "другие": [
        {{
            "категория": "товар/цена/конкурент/время/география",
            "название": "конкретное название или значение",
            "контекст": "дополнительные детали"
        }}
    ]
}}

Если какой-то тип сущностей не найден, верни пустой массив для этого типа.
"""
    
    def _parse_enhanced_entity_response(self, response: str) -> Dict[str, List[Dict[str, Any]]]:
        """Parse enhanced entity extraction response
        
        Args:
            response: Raw response from OpenAI
            
        Returns:
            Parsed entities dictionary with structured data
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
                self.logger.warning("No JSON found in enhanced response")
                return {key: [] for key in ['проблемы', 'идеи', 'доставка', 'сигналы', 'другие']}
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse enhanced JSON response: {e}")
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
        max_tokens = self.max_tokens - 2000  # Leave room for response
        tokens = self.tokenizer.encode(text)
        
        if len(tokens) > max_tokens:
            truncated_tokens = tokens[:max_tokens]
            return self.tokenizer.decode(truncated_tokens)
        
        return text
    
    def _flatten_enhanced_entities(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Flatten enhanced entities for analysis
        
        Args:
            data: DataFrame with extracted entities
            
        Returns:
            List of flattened entity records with enhanced structure
        """
        flattened = []
        
        for _, row in data.iterrows():
            dialog_id = row['dialog_id']
            entities = row['extracted_entities']['entities']
            
            for entity_type, entity_list in entities.items():
                for entity in entity_list:
                    if isinstance(entity, dict):
                        # Enhanced structured entity
                        flattened.append({
                            'dialog_id': dialog_id,
                            'entity_type': entity_type,
                            'entity_text': entity.get('описание', str(entity)),
                            'entity_data': entity,
                            'text': row['text'],
                            'extraction_success': row['extracted_entities']['extraction_success'],
                            'extraction_method': 'enhanced'
                        })
                    else:
                        # Fallback for simple entities
                        flattened.append({
                            'dialog_id': dialog_id,
                            'entity_type': entity_type,
                            'entity_text': str(entity),
                            'entity_data': {'описание': str(entity)},
                            'text': row['text'],
                            'extraction_success': row['extracted_entities']['extraction_success'],
                            'extraction_method': 'enhanced'
                        })
        
        return flattened
    
    def get_enhanced_entity_statistics(self, flattened_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get enhanced statistics about extracted entities
        
        Args:
            flattened_entities: List of flattened entity records
            
        Returns:
            Dictionary with enhanced entity statistics
        """
        if not flattened_entities:
            return {}
        
        df = pd.DataFrame(flattened_entities)
        
        # Basic statistics
        stats = {
            'total_entities': len(df),
            'entities_by_type': df['entity_type'].value_counts().to_dict(),
            'successful_extractions': df['extraction_success'].sum(),
            'failed_extractions': (~df['extraction_success']).sum(),
            'unique_entities': df['entity_text'].nunique(),
            'avg_entities_per_dialog': len(df) / df['dialog_id'].nunique() if len(df) > 0 else 0
        }
        
        # Enhanced statistics for structured entities
        if 'entity_data' in df.columns:
            # Analyze problem categories
            problems = df[df['entity_type'] == 'проблемы']
            if not problems.empty:
                problem_categories = []
                for _, row in problems.iterrows():
                    if isinstance(row['entity_data'], dict) and 'категория' in row['entity_data']:
                        problem_categories.append(row['entity_data']['категория'])
                
                if problem_categories:
                    stats['problem_categories'] = pd.Series(problem_categories).value_counts().to_dict()
            
            # Analyze delivery services
            delivery = df[df['entity_type'] == 'доставка']
            if not delivery.empty:
                delivery_services = []
                for _, row in delivery.iterrows():
                    if isinstance(row['entity_data'], dict) and 'служба' in row['entity_data']:
                        delivery_services.append(row['entity_data']['служба'])
                
                if delivery_services:
                    stats['delivery_services'] = pd.Series(delivery_services).value_counts().to_dict()
        
        return stats
