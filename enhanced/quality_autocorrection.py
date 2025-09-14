#!/usr/bin/env python3
"""
Quality Auto-Correction System
Система автокоррекции качества цитат и извлеченных сущностей
"""

import re
import logging
import json
import asyncio
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

@dataclass
class QualityIssue:
    """Проблема качества в цитате или сущности"""
    type: str  # 'garbage', 'duplicate', 'irrelevant', 'incomplete'
    severity: float  # 0.0 - 1.0
    description: str
    suggested_fix: str
    confidence: float

@dataclass
class CorrectedQuote:
    """Исправленная цитата"""
    original: str
    corrected: str
    quality_score: float
    issues_fixed: List[str]
    confidence: float

class QualityAutoCorrector:
    """Система автокоррекции качества"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Паттерны для фильтрации мусора
        self.garbage_patterns = [
            r'^(угу|ага|да|нет|хм|эм|мм)\s*$',
            r'^(угу|ага|да|нет|хм|эм|мм)\s+(угу|ага|да|нет|хм|эм|мм)\s*$',
            r'^(угу|ага|да|нет|хм|эм|мм)\s+(угу|ага|да|нет|хм|эм|мм)\s+(угу|ага|да|нет|хм|эм|мм)\s*$',
            r'^[а-яё]{1,3}\s*$',  # Слова короче 4 символов
            r'^[^а-яё]*$',  # Только не-кириллические символы
            r'^\s*$',  # Пустые строки
        ]
        
        # Ключевые слова для релевантности
        self.relevance_keywords = [
            'доставка', 'заказ', 'курьер', 'адрес', 'время', 'оплата',
            'проблема', 'жалоба', 'предложение', 'вопрос', 'помощь',
            'клиент', 'менеджер', 'звонок', 'звонить', 'связь'
        ]
        
        # Кэш для семантических сравнений
        self.semantic_cache = {}
        
    def detect_quality_issues(self, quote: str, context: str = "") -> List[QualityIssue]:
        """Обнаружение проблем качества в цитате"""
        issues = []
        
        # 1. Проверка на мусор
        if self._is_garbage(quote):
            issues.append(QualityIssue(
                type='garbage',
                severity=0.9,
                description=f"Цитата содержит мусор: '{quote}'",
                suggested_fix="Удалить или заменить на релевантную цитату",
                confidence=0.95
            ))
        
        # 2. Проверка релевантности
        if not self._is_relevant(quote, context):
            issues.append(QualityIssue(
                type='irrelevant',
                severity=0.7,
                description=f"Цитата не релевантна контексту: '{quote}'",
                suggested_fix="Найти более релевантную цитату",
                confidence=0.8
            ))
        
        # 3. Проверка полноты
        if self._is_incomplete(quote):
            issues.append(QualityIssue(
                type='incomplete',
                severity=0.6,
                description=f"Цитата неполная: '{quote}'",
                suggested_fix="Дополнить контекстом",
                confidence=0.7
            ))
        
        return issues
    
    def _is_garbage(self, text: str) -> bool:
        """Проверка на мусор"""
        text = text.strip().lower()
        
        for pattern in self.garbage_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                return True
        
        # Проверка на повторяющиеся слова
        words = text.split()
        if len(words) > 1:
            unique_words = set(words)
            if len(unique_words) == 1 and len(words) > 2:
                return True
        
        return False
    
    def _is_relevant(self, quote: str, context: str) -> bool:
        """Проверка релевантности цитаты"""
        quote_lower = quote.lower()
        context_lower = context.lower()
        
        # Проверка ключевых слов
        quote_keywords = sum(1 for kw in self.relevance_keywords if kw in quote_lower)
        context_keywords = sum(1 for kw in self.relevance_keywords if kw in context_lower)
        
        if quote_keywords == 0 and context_keywords > 0:
            return False
        
        # Семантическая проверка (если есть контекст)
        if context and len(quote) > 10:
            try:
                similarity = self._get_semantic_similarity(quote, context)
                return similarity > 0.3
            except Exception as e:
                logger.warning(f"Ошибка семантической проверки: {e}")
                return True  # По умолчанию считаем релевантной
        
        return True
    
    def _is_incomplete(self, quote: str) -> bool:
        """Проверка полноты цитаты"""
        # Слишком короткие цитаты
        if len(quote.strip()) < 10:
            return True
        
        # Цитаты без глаголов (возможно неполные)
        if not re.search(r'\b(есть|быть|делать|работать|звонить|приехать|доставить|забрать|оплатить|получить)\b', quote.lower()):
            return True
        
        return False
    
    def _get_semantic_similarity(self, text1: str, text2: str) -> float:
        """Получение семантического сходства"""
        cache_key = f"{text1[:50]}_{text2[:50]}"
        
        if cache_key in self.semantic_cache:
            return self.semantic_cache[cache_key]
        
        try:
            embeddings = self.model.encode([text1, text2])
            similarity = cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]
            self.semantic_cache[cache_key] = similarity
            return similarity
        except Exception as e:
            logger.warning(f"Ошибка семантического анализа: {e}")
            return 0.0
    
    def correct_quote(self, quote: str, context: str = "", entity_type: str = "") -> CorrectedQuote:
        """Автокоррекция цитаты"""
        issues = self.detect_quality_issues(quote, context)
        
        if not issues:
            return CorrectedQuote(
                original=quote,
                corrected=quote,
                quality_score=1.0,
                issues_fixed=[],
                confidence=1.0
            )
        
        # Применяем исправления
        corrected = quote
        fixed_issues = []
        
        for issue in issues:
            if issue.type == 'garbage':
                # Пытаемся найти лучшую цитату в контексте
                better_quote = self._find_better_quote_in_context(context, entity_type)
                if better_quote:
                    corrected = better_quote
                    fixed_issues.append('garbage')
                else:
                    corrected = ""  # Удаляем мусор
                    fixed_issues.append('garbage')
            
            elif issue.type == 'incomplete':
                # Дополняем контекстом
                corrected = self._complete_quote(corrected, context)
                fixed_issues.append('incomplete')
        
        # Вычисляем качество исправленной цитаты
        quality_score = self._calculate_quality_score(corrected, context)
        
        return CorrectedQuote(
            original=quote,
            corrected=corrected,
            quality_score=quality_score,
            issues_fixed=fixed_issues,
            confidence=0.8 if fixed_issues else 1.0
        )
    
    def _find_better_quote_in_context(self, context: str, entity_type: str) -> Optional[str]:
        """Поиск лучшей цитаты в контексте"""
        if not context:
            return None
        
        # Разбиваем контекст на предложения
        sentences = re.split(r'[.!?]+', context)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        # Ищем предложения с ключевыми словами
        for sentence in sentences:
            if len(sentence) > 20 and not self._is_garbage(sentence):
                if entity_type and any(kw in sentence.lower() for kw in self.relevance_keywords):
                    return sentence
        
        return None
    
    def _complete_quote(self, quote: str, context: str) -> str:
        """Дополнение цитаты контекстом"""
        if not context or len(quote) > 50:
            return quote
        
        # Ищем предложения, содержащие цитату
        sentences = re.split(r'[.!?]+', context)
        for sentence in sentences:
            if quote.lower() in sentence.lower():
                return sentence.strip()
        
        return quote
    
    def _calculate_quality_score(self, quote: str, context: str) -> float:
        """Вычисление оценки качества цитаты"""
        if not quote:
            return 0.0
        
        score = 1.0
        
        # Штраф за мусор
        if self._is_garbage(quote):
            score -= 0.8
        
        # Штраф за нерелевантность
        if not self._is_relevant(quote, context):
            score -= 0.6
        
        # Штраф за неполноту
        if self._is_incomplete(quote):
            score -= 0.4
        
        # Бонус за длину (но не слишком длинные)
        if 20 <= len(quote) <= 200:
            score += 0.1
        
        # Бонус за ключевые слова
        keyword_count = sum(1 for kw in self.relevance_keywords if kw in quote.lower())
        score += min(keyword_count * 0.1, 0.3)
        
        return max(0.0, min(1.0, score))
    
    def remove_duplicates(self, quotes: List[str], threshold: float = 0.8) -> List[str]:
        """Удаление дубликатов цитат"""
        if not quotes:
            return []
        
        unique_quotes = []
        seen_embeddings = []
        
        for quote in quotes:
            if not quote.strip():
                continue
            
            # Получаем эмбеддинг цитаты
            try:
                embedding = self.model.encode([quote])[0]
                
                # Проверяем на дубликаты
                is_duplicate = False
                for seen_emb in seen_embeddings:
                    similarity = cosine_similarity([embedding], [seen_emb])[0][0]
                    if similarity > threshold:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    unique_quotes.append(quote)
                    seen_embeddings.append(embedding)
                    
            except Exception as e:
                logger.warning(f"Ошибка при проверке дубликатов: {e}")
                # Если не можем проверить семантически, проверяем текстово
                if quote not in unique_quotes:
                    unique_quotes.append(quote)
        
        return unique_quotes
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def enhance_quote_with_llm(self, quote: str, context: str, entity_type: str) -> str:
        """Улучшение цитаты с помощью LLM"""
        if not quote or len(quote) < 5:
            return quote
        
        prompt = f"""
Улучши эту цитату для анализа диалогов о доставке:

Контекст: {context[:500]}
Тип сущности: {entity_type}
Цитата: {quote}

Требования:
1. Сохрани смысл, но сделай более информативной
2. Убери мусор и междометия
3. Добавь контекст если нужно
4. Максимум 100 символов

Улучшенная цитата:"""

        try:
            response = await openai.ChatCompletion.acreate(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.3
            )
            
            enhanced = response.choices[0].message.content.strip()
            return enhanced if enhanced else quote
            
        except Exception as e:
            logger.warning(f"Ошибка LLM улучшения: {e}")
            return quote

class QualityMonitor:
    """Мониторинг качества системы"""
    
    def __init__(self):
        self.metrics = {
            'total_quotes': 0,
            'corrected_quotes': 0,
            'garbage_removed': 0,
            'duplicates_removed': 0,
            'quality_improvement': 0.0,
            'avg_quality_score': 0.0
        }
    
    def update_metrics(self, correction_result: CorrectedQuote):
        """Обновление метрик качества"""
        self.metrics['total_quotes'] += 1
        
        if correction_result.issues_fixed:
            self.metrics['corrected_quotes'] += 1
        
        if 'garbage' in correction_result.issues_fixed:
            self.metrics['garbage_removed'] += 1
        
        if 'duplicate' in correction_result.issues_fixed:
            self.metrics['duplicates_removed'] += 1
        
        # Обновляем среднюю оценку качества
        total = self.metrics['total_quotes']
        current_avg = self.metrics['avg_quality_score']
        self.metrics['avg_quality_score'] = (current_avg * (total - 1) + correction_result.quality_score) / total
    
    def get_quality_report(self) -> Dict[str, Any]:
        """Получение отчета о качестве"""
        if self.metrics['total_quotes'] == 0:
            return self.metrics
        
        correction_rate = self.metrics['corrected_quotes'] / self.metrics['total_quotes']
        garbage_rate = self.metrics['garbage_removed'] / self.metrics['total_quotes']
        
        return {
            **self.metrics,
            'correction_rate': correction_rate,
            'garbage_removal_rate': garbage_rate,
            'quality_grade': self._get_quality_grade()
        }
    
    def _get_quality_grade(self) -> str:
        """Получение оценки качества"""
        avg_score = self.metrics['avg_quality_score']
        
        if avg_score >= 0.9:
            return 'A+'
        elif avg_score >= 0.8:
            return 'A'
        elif avg_score >= 0.7:
            return 'B'
        elif avg_score >= 0.6:
            return 'C'
        else:
            return 'D'

# Пример использования
if __name__ == "__main__":
    # Тестирование системы автокоррекции
    config = {
        'openai_api_key': 'your-api-key',
        'model_name': 'gpt-3.5-turbo'
    }
    
    corrector = QualityAutoCorrector(config)
    monitor = QualityMonitor()
    
    # Тестовые цитаты
    test_quotes = [
        "Угу. Угу угу угу. Угу. Угу. Угу, ну угу",
        "Доставка будет завтра в 10 утра",
        "Да",
        "Курьер приедет по адресу ул. Ленина, 1",
        "Хм, не знаю"
    ]
    
    context = "Клиент звонит по поводу доставки заказа. Менеджер объясняет условия доставки."
    
    print("=== Тестирование автокоррекции качества ===")
    for quote in test_quotes:
        result = corrector.correct_quote(quote, context, "delivery_info")
        monitor.update_metrics(result)
        
        print(f"\nОригинал: '{quote}'")
        print(f"Исправлено: '{result.corrected}'")
        print(f"Качество: {result.quality_score:.2f}")
        print(f"Исправления: {result.issues_fixed}")
    
    print(f"\n=== Отчет о качестве ===")
    report = monitor.get_quality_report()
    for key, value in report.items():
        print(f"{key}: {value}")
