#!/usr/bin/env python3
"""
Continuous Learning System
Система непрерывного обучения на новых данных
"""

import json
import logging
import asyncio
import pickle
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

@dataclass
class LearningExample:
    """Пример для обучения"""
    dialog: str
    extracted_entities: Dict[str, List[str]]
    quality_score: float
    timestamp: datetime
    source: str  # 'manual', 'auto_correction', 'user_feedback'
    metadata: Dict[str, Any] = None

@dataclass
class LearningPattern:
    """Паттерн для обучения"""
    pattern_type: str  # 'quote_quality', 'entity_extraction', 'context_understanding'
    pattern_data: Dict[str, Any]
    confidence: float
    frequency: int
    last_seen: datetime

@dataclass
class ModelUpdate:
    """Обновление модели"""
    model_name: str
    update_type: str  # 'incremental', 'full_retrain', 'fine_tune'
    performance_improvement: float
    timestamp: datetime
    training_data_size: int
    validation_score: float

class ContinuousLearningSystem:
    """Система непрерывного обучения"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.learning_examples: List[LearningExample] = []
        self.learned_patterns: Dict[str, LearningPattern] = {}
        self.model_updates: List[ModelUpdate] = []
        
        # Модели для обучения
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.cluster_model = None
        
        # Пороги для обучения
        self.learning_thresholds = {
            'min_quality_score': 0.7,
            'min_examples_for_update': 50,
            'pattern_confidence_threshold': 0.8,
            'performance_improvement_threshold': 0.05
        }
        
        # Кэш для быстрого доступа
        self.quality_patterns_cache = {}
        self.entity_patterns_cache = {}
        
    def add_learning_example(self, dialog: str, extracted_entities: Dict[str, List[str]], 
                           quality_score: float, source: str = 'auto_correction',
                           metadata: Dict[str, Any] = None) -> LearningExample:
        """Добавление примера для обучения"""
        
        example = LearningExample(
            dialog=dialog,
            extracted_entities=extracted_entities,
            quality_score=quality_score,
            timestamp=datetime.now(),
            source=source,
            metadata=metadata or {}
        )
        
        self.learning_examples.append(example)
        
        # Автоматическое обучение на новом примере
        if quality_score >= self.learning_thresholds['min_quality_score']:
            self._learn_from_example(example)
        
        logger.info(f"Добавлен пример обучения: качество {quality_score:.2f}, источник {source}")
        return example
    
    def _learn_from_example(self, example: LearningExample):
        """Обучение на конкретном примере"""
        
        # 1. Обучение паттернов качества цитат
        self._learn_quote_quality_patterns(example)
        
        # 2. Обучение паттернов извлечения сущностей
        self._learn_entity_extraction_patterns(example)
        
        # 3. Обучение контекстных паттернов
        self._learn_context_patterns(example)
        
        # 4. Проверка на необходимость обновления модели
        if len(self.learning_examples) % self.learning_thresholds['min_examples_for_update'] == 0:
            self._check_model_update_needed()
    
    def _learn_quote_quality_patterns(self, example: LearningExample):
        """Обучение паттернов качества цитат"""
        quotes = example.extracted_entities.get('quotes', [])
        
        for quote in quotes:
            if len(quote) < 10:
                continue
            
            # Анализируем качество цитаты
            quality_features = self._extract_quote_quality_features(quote)
            
            # Создаем или обновляем паттерн
            pattern_key = f"quote_quality_{hash(quote[:50])}"
            
            if pattern_key in self.learned_patterns:
                pattern = self.learned_patterns[pattern_key]
                pattern.frequency += 1
                pattern.last_seen = datetime.now()
                
                # Обновляем паттерн с учетом нового примера
                pattern.pattern_data = self._merge_pattern_data(
                    pattern.pattern_data, quality_features
                )
            else:
                self.learned_patterns[pattern_key] = LearningPattern(
                    pattern_type='quote_quality',
                    pattern_data=quality_features,
                    confidence=example.quality_score,
                    frequency=1,
                    last_seen=datetime.now()
                )
    
    def _learn_entity_extraction_patterns(self, example: LearningExample):
        """Обучение паттернов извлечения сущностей"""
        dialog = example.dialog
        
        for entity_type, entities in example.extracted_entities.items():
            if not entities:
                continue
            
            # Анализируем контекст для каждой сущности
            for entity in entities:
                context_pattern = self._extract_entity_context_pattern(dialog, entity)
                
                pattern_key = f"entity_{entity_type}_{hash(entity[:30])}"
                
                if pattern_key in self.learned_patterns:
                    pattern = self.learned_patterns[pattern_key]
                    pattern.frequency += 1
                    pattern.last_seen = datetime.now()
                else:
                    self.learned_patterns[pattern_key] = LearningPattern(
                        pattern_type='entity_extraction',
                        pattern_data=context_pattern,
                        confidence=example.quality_score,
                        frequency=1,
                        last_seen=datetime.now()
                    )
    
    def _learn_context_patterns(self, example: LearningExample):
        """Обучение контекстных паттернов"""
        dialog = example.dialog
        
        # Анализируем общий контекст диалога
        context_features = self._extract_dialog_context_features(dialog)
        
        # Создаем паттерн контекста
        pattern_key = f"context_{hash(dialog[:100])}"
        
        if pattern_key in self.learned_patterns:
            pattern = self.learned_patterns[pattern_key]
            pattern.frequency += 1
            pattern.last_seen = datetime.now()
        else:
            self.learned_patterns[pattern_key] = LearningPattern(
                pattern_type='context_understanding',
                pattern_data=context_features,
                confidence=example.quality_score,
                frequency=1,
                last_seen=datetime.now()
            )
    
    def _extract_quote_quality_features(self, quote: str) -> Dict[str, Any]:
        """Извлечение признаков качества цитаты"""
        return {
            'length': len(quote),
            'word_count': len(quote.split()),
            'has_verbs': bool(re.search(r'\b(есть|быть|делать|работать|звонить|приехать|доставить|забрать|оплатить|получить)\b', quote.lower())),
            'has_delivery_keywords': any(kw in quote.lower() for kw in ['доставка', 'заказ', 'курьер', 'адрес', 'время', 'оплата']),
            'has_garbage': any(word in quote.lower() for word in ['угу', 'ага', 'да', 'нет', 'хм', 'эм']),
            'sentence_count': quote.count('.') + quote.count('!') + quote.count('?'),
            'capitalization_ratio': sum(1 for c in quote if c.isupper()) / len(quote) if quote else 0
        }
    
    def _extract_entity_context_pattern(self, dialog: str, entity: str) -> Dict[str, Any]:
        """Извлечение паттерна контекста для сущности"""
        # Находим позицию сущности в диалоге
        entity_pos = dialog.lower().find(entity.lower())
        
        if entity_pos == -1:
            return {}
        
        # Извлекаем контекст вокруг сущности
        context_start = max(0, entity_pos - 50)
        context_end = min(len(dialog), entity_pos + len(entity) + 50)
        context = dialog[context_start:context_end]
        
        return {
            'entity': entity,
            'context': context,
            'position_in_dialog': entity_pos / len(dialog),
            'surrounding_words': context.split()[:10],  # Первые 10 слов контекста
            'has_question_mark': '?' in context,
            'has_exclamation_mark': '!' in context,
            'context_length': len(context)
        }
    
    def _extract_dialog_context_features(self, dialog: str) -> Dict[str, Any]:
        """Извлечение признаков контекста диалога"""
        return {
            'length': len(dialog),
            'sentence_count': dialog.count('.') + dialog.count('!') + dialog.count('?'),
            'question_count': dialog.count('?'),
            'exclamation_count': dialog.count('!'),
            'has_delivery_keywords': any(kw in dialog.lower() for kw in ['доставка', 'заказ', 'курьер', 'адрес', 'время', 'оплата']),
            'has_problem_keywords': any(kw in dialog.lower() for kw in ['проблема', 'жалоба', 'недовольство', 'ошибка', 'задержка']),
            'has_solution_keywords': any(kw in dialog.lower() for kw in ['решение', 'исправить', 'помочь', 'предложение', 'идея']),
            'speaker_changes': dialog.count('Клиент:') + dialog.count('Менеджер:'),
            'avg_sentence_length': len(dialog.split()) / max(1, dialog.count('.') + dialog.count('!') + dialog.count('?'))
        }
    
    def _merge_pattern_data(self, existing_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
        """Слияние данных паттернов"""
        merged = existing_data.copy()
        
        for key, value in new_data.items():
            if key in merged:
                if isinstance(value, (int, float)):
                    # Усредняем числовые значения
                    merged[key] = (merged[key] + value) / 2
                elif isinstance(value, list):
                    # Объединяем списки
                    merged[key] = list(set(merged[key] + value))
                else:
                    # Заменяем строковые значения
                    merged[key] = value
            else:
                merged[key] = value
        
        return merged
    
    def _check_model_update_needed(self):
        """Проверка необходимости обновления модели"""
        if len(self.learning_examples) < self.learning_thresholds['min_examples_for_update']:
            return
        
        # Анализируем производительность на новых данных
        recent_examples = [ex for ex in self.learning_examples 
                          if ex.timestamp > datetime.now() - timedelta(days=7)]
        
        if len(recent_examples) < 10:
            return
        
        # Вычисляем среднюю производительность
        avg_quality = np.mean([ex.quality_score for ex in recent_examples])
        
        # Сравниваем с предыдущими обновлениями
        if self.model_updates:
            last_update = self.model_updates[-1]
            if avg_quality - last_update.validation_score > self.learning_thresholds['performance_improvement_threshold']:
                self._trigger_model_update(avg_quality)
        else:
            # Первое обновление
            self._trigger_model_update(avg_quality)
    
    def _trigger_model_update(self, validation_score: float):
        """Запуск обновления модели"""
        logger.info("Запуск обновления модели...")
        
        # Подготавливаем данные для обучения
        training_data = self._prepare_training_data()
        
        if not training_data:
            logger.warning("Недостаточно данных для обучения")
            return
        
        # Выполняем обновление модели
        try:
            # Здесь можно добавить реальное обучение модели
            # Например, fine-tuning или incremental learning
            
            # Создаем запись об обновлении
            update = ModelUpdate(
                model_name="dialogs_rag_model",
                update_type="incremental",
                performance_improvement=validation_score - (self.model_updates[-1].validation_score if self.model_updates else 0),
                timestamp=datetime.now(),
                training_data_size=len(training_data),
                validation_score=validation_score
            )
            
            self.model_updates.append(update)
            
            logger.info(f"Модель обновлена: улучшение {update.performance_improvement:.3f}")
            
        except Exception as e:
            logger.error(f"Ошибка обновления модели: {e}")
    
    def _prepare_training_data(self) -> List[Dict[str, Any]]:
        """Подготовка данных для обучения"""
        training_data = []
        
        for example in self.learning_examples:
            if example.quality_score >= self.learning_thresholds['min_quality_score']:
                training_data.append({
                    'dialog': example.dialog,
                    'entities': example.extracted_entities,
                    'quality_score': example.quality_score,
                    'timestamp': example.timestamp
                })
        
        return training_data
    
    def get_learning_insights(self) -> Dict[str, Any]:
        """Получение инсайтов обучения"""
        if not self.learning_examples:
            return {"error": "Нет данных для анализа"}
        
        # Анализ качества обучения
        quality_scores = [ex.quality_score for ex in self.learning_examples]
        
        # Анализ источников данных
        sources = {}
        for example in self.learning_examples:
            sources[example.source] = sources.get(example.source, 0) + 1
        
        # Анализ трендов
        recent_examples = [ex for ex in self.learning_examples 
                          if ex.timestamp > datetime.now() - timedelta(days=7)]
        
        # Анализ паттернов
        pattern_types = {}
        for pattern in self.learned_patterns.values():
            pattern_types[pattern.pattern_type] = pattern_types.get(pattern.pattern_type, 0) + 1
        
        return {
            "total_examples": len(self.learning_examples),
            "avg_quality_score": np.mean(quality_scores),
            "quality_std": np.std(quality_scores),
            "sources_distribution": sources,
            "recent_examples_7d": len(recent_examples),
            "learned_patterns": len(self.learned_patterns),
            "pattern_types": pattern_types,
            "model_updates": len(self.model_updates),
            "last_update": self.model_updates[-1].timestamp if self.model_updates else None,
            "learning_effectiveness": self._calculate_learning_effectiveness()
        }
    
    def _calculate_learning_effectiveness(self) -> float:
        """Вычисление эффективности обучения"""
        if len(self.learning_examples) < 10:
            return 0.0
        
        # Анализируем улучшение качества со временем
        examples_by_time = sorted(self.learning_examples, key=lambda x: x.timestamp)
        
        # Разделяем на две половины
        mid_point = len(examples_by_time) // 2
        early_examples = examples_by_time[:mid_point]
        late_examples = examples_by_time[mid_point:]
        
        early_avg = np.mean([ex.quality_score for ex in early_examples])
        late_avg = np.mean([ex.quality_score for ex in late_examples])
        
        return (late_avg - early_avg) / early_avg if early_avg > 0 else 0.0
    
    def predict_quality(self, dialog: str, extracted_entities: Dict[str, List[str]]) -> float:
        """Предсказание качества на основе обученных паттернов"""
        if not self.learned_patterns:
            return 0.5  # Нейтральная оценка
        
        # Извлекаем признаки
        dialog_features = self._extract_dialog_context_features(dialog)
        
        # Сравниваем с обученными паттернами
        similarities = []
        confidences = []
        
        for pattern in self.learned_patterns.values():
            if pattern.pattern_type == 'context_understanding':
                # Вычисляем схожесть с контекстными паттернами
                similarity = self._calculate_pattern_similarity(dialog_features, pattern.pattern_data)
                similarities.append(similarity)
                confidences.append(pattern.confidence)
        
        if not similarities:
            return 0.5
        
        # Взвешенная оценка качества
        weighted_score = sum(s * c for s, c in zip(similarities, confidences))
        total_weight = sum(confidences)
        
        return weighted_score / total_weight if total_weight > 0 else 0.5
    
    def _calculate_pattern_similarity(self, features1: Dict[str, Any], features2: Dict[str, Any]) -> float:
        """Вычисление схожести между паттернами"""
        common_keys = set(features1.keys()) & set(features2.keys())
        
        if not common_keys:
            return 0.0
        
        similarities = []
        
        for key in common_keys:
            val1 = features1[key]
            val2 = features2[key]
            
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                # Нормализованная разность для числовых значений
                max_val = max(abs(val1), abs(val2))
                if max_val > 0:
                    similarity = 1 - abs(val1 - val2) / max_val
                    similarities.append(max(0, similarity))
            elif isinstance(val1, str) and isinstance(val2, str):
                # Строковое сравнение
                similarity = 1 if val1.lower() == val2.lower() else 0
                similarities.append(similarity)
        
        return np.mean(similarities) if similarities else 0.0
    
    def save_learning_data(self, filepath: str):
        """Сохранение данных обучения"""
        data = {
            "learning_examples": [asdict(ex) for ex in self.learning_examples],
            "learned_patterns": {k: asdict(v) for k, v in self.learned_patterns.items()},
            "model_updates": [asdict(update) for update in self.model_updates],
            "learning_thresholds": self.learning_thresholds
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    def load_learning_data(self, filepath: str):
        """Загрузка данных обучения"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Восстанавливаем примеры обучения
        self.learning_examples = []
        for ex_data in data.get("learning_examples", []):
            ex_data["timestamp"] = datetime.fromisoformat(ex_data["timestamp"])
            self.learning_examples.append(LearningExample(**ex_data))
        
        # Восстанавливаем паттерны
        self.learned_patterns = {}
        for k, pattern_data in data.get("learned_patterns", {}).items():
            pattern_data["last_seen"] = datetime.fromisoformat(pattern_data["last_seen"])
            self.learned_patterns[k] = LearningPattern(**pattern_data)
        
        # Восстанавливаем обновления модели
        self.model_updates = []
        for update_data in data.get("model_updates", []):
            update_data["timestamp"] = datetime.fromisoformat(update_data["timestamp"])
            self.model_updates.append(ModelUpdate(**update_data))
        
        self.learning_thresholds = data.get("learning_thresholds", self.learning_thresholds)

# Пример использования
if __name__ == "__main__":
    # Тестирование системы непрерывного обучения
    config = {
        'openai_api_key': 'your-api-key'
    }
    
    learning_system = ContinuousLearningSystem(config)
    
    # Добавляем примеры обучения
    test_examples = [
        {
            "dialog": "Клиент: Здравствуйте, у меня проблема с доставкой. Менеджер: Расскажите подробнее.",
            "entities": {
                "problems": ["проблема с доставкой"],
                "quotes": ["у меня проблема с доставкой"]
            },
            "quality_score": 0.9
        },
        {
            "dialog": "Угу. Угу угу угу. Угу. Угу. Угу, ну угу",
            "entities": {
                "problems": [],
                "quotes": ["Угу. Угу угу угу. Угу. Угу. Угу, ну угу"]
            },
            "quality_score": 0.1
        }
    ]
    
    print("=== Система непрерывного обучения ===")
    
    for example in test_examples:
        learning_system.add_learning_example(
            dialog=example["dialog"],
            extracted_entities=example["entities"],
            quality_score=example["quality_score"],
            source="test"
        )
    
    # Получаем инсайты
    insights = learning_system.get_learning_insights()
    print(f"Инсайты обучения: {insights}")
    
    # Тестируем предсказание качества
    test_dialog = "Клиент: Доставка задерживается на 2 часа. Менеджер: Извините, курьер в пути."
    test_entities = {
        "problems": ["доставка задерживается"],
        "quotes": ["Доставка задерживается на 2 часа"]
    }
    
    predicted_quality = learning_system.predict_quality(test_dialog, test_entities)
    print(f"Предсказанное качество: {predicted_quality:.2f}")
