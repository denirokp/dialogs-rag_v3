#!/usr/bin/env python3
"""
Adaptive Prompts System with A/B Testing
Система адаптивных промптов с A/B тестированием
"""

import json
import logging
import asyncio
import random
import statistics
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from scipy import stats
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

@dataclass
class PromptVariant:
    """Вариант промпта для A/B тестирования"""
    name: str
    prompt_text: str
    model_config: Dict[str, Any]
    expected_improvement: float
    weight: float = 1.0
    is_active: bool = True
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

@dataclass
class TestResult:
    """Результат тестирования промпта"""
    variant_name: str
    input_text: str
    output_text: str
    quality_score: float
    processing_time: float
    timestamp: datetime
    metadata: Dict[str, Any] = None

@dataclass
class ABTestConfig:
    """Конфигурация A/B теста"""
    test_name: str
    variants: List[PromptVariant]
    traffic_split: Dict[str, float]  # Распределение трафика
    min_sample_size: int = 100
    confidence_level: float = 0.95
    test_duration_days: int = 7
    auto_switch: bool = True

class AdaptivePromptSystem:
    """Система адаптивных промптов"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.variants: Dict[str, PromptVariant] = {}
        self.test_results: List[TestResult] = []
        self.active_tests: Dict[str, ABTestConfig] = {}
        self.performance_history: Dict[str, List[float]] = {}
        
        # Инициализация базовых промптов
        self._initialize_base_prompts()
    
    def _initialize_base_prompts(self):
        """Инициализация базовых вариантов промптов"""
        
        # Базовый промпт
        base_prompt = PromptVariant(
            name="base",
            prompt_text="""Извлеки ключевые сущности из диалога о доставке.

Диалог: {dialog}

Извлеки:
1. Проблемы доставки
2. Идеи улучшения
3. Барьеры
4. Релевантные цитаты

Формат JSON:
{{
    "problems": ["проблема1", "проблема2"],
    "ideas": ["идея1", "идея2"],
    "barriers": ["барьер1", "барьер2"],
    "quotes": ["цитата1", "цитата2"]
}}""",
            model_config={"model": "gpt-3.5-turbo", "temperature": 0.3},
            expected_improvement=0.0
        )
        
        # Детальный промпт
        detailed_prompt = PromptVariant(
            name="detailed",
            prompt_text="""Проанализируй диалог о доставке и извлеки максимально детальную информацию.

Диалог: {dialog}

Контекст: Это диалог между клиентом и менеджером службы доставки.

Задачи:
1. Проблемы доставки - конкретные проблемы, с которыми столкнулся клиент
2. Идеи улучшения - предложения по улучшению сервиса
3. Барьеры - препятствия в процессе доставки
4. Релевантные цитаты - точные высказывания клиента

Требования к цитатам:
- Минимум 10 символов
- Содержат ключевые слова о доставке
- Без междометий и мусора
- Максимально информативные

Формат JSON:
{{
    "problems": ["конкретная проблема с деталями"],
    "ideas": ["конкретная идея улучшения"],
    "barriers": ["конкретный барьер"],
    "quotes": ["информативная цитата клиента"]
}}""",
            model_config={"model": "gpt-3.5-turbo", "temperature": 0.2},
            expected_improvement=0.15
        )
        
        # Минималистичный промпт
        minimal_prompt = PromptVariant(
            name="minimal",
            prompt_text="""Извлеки сущности из диалога о доставке.

{dialog}

JSON:
{{
    "problems": [],
    "ideas": [],
    "barriers": [],
    "quotes": []
}}""",
            model_config={"model": "gpt-3.5-turbo", "temperature": 0.1},
            expected_improvement=0.05
        )
        
        # Контекстный промпт
        contextual_prompt = PromptVariant(
            name="contextual",
            prompt_text="""Анализ диалога службы доставки.

Диалог: {dialog}

Тип диалога: {dialog_type}
Длина диалога: {dialog_length} символов
Сложность: {complexity_level}

Извлеки сущности с учетом контекста:

1. Проблемы доставки (проблемы клиента)
2. Идеи улучшения (предложения клиента)
3. Барьеры (препятствия в процессе)
4. Цитаты (ключевые высказывания)

Критерии качества цитат:
- Релевантность теме доставки
- Информативность
- Отсутствие мусора
- Длина 15-100 символов

Формат JSON:
{{
    "problems": ["проблема"],
    "ideas": ["идея"],
    "barriers": ["барьер"],
    "quotes": ["цитата"]
}}""",
            model_config={"model": "gpt-3.5-turbo", "temperature": 0.25},
            expected_improvement=0.20
        )
        
        self.variants = {
            "base": base_prompt,
            "detailed": detailed_prompt,
            "minimal": minimal_prompt,
            "contextual": contextual_prompt
        }
    
    def create_ab_test(self, test_name: str, variants: List[str], 
                      traffic_split: Optional[Dict[str, float]] = None) -> ABTestConfig:
        """Создание A/B теста"""
        
        if traffic_split is None:
            # Равномерное распределение
            traffic_split = {v: 1.0/len(variants) for v in variants}
        
        # Проверяем, что все варианты существуют
        for variant_name in variants:
            if variant_name not in self.variants:
                raise ValueError(f"Вариант {variant_name} не найден")
        
        test_config = ABTestConfig(
            test_name=test_name,
            variants=[self.variants[v] for v in variants],
            traffic_split=traffic_split
        )
        
        self.active_tests[test_name] = test_config
        logger.info(f"Создан A/B тест: {test_name}")
        
        return test_config
    
    def select_variant(self, test_name: str) -> str:
        """Выбор варианта для A/B теста"""
        if test_name not in self.active_tests:
            return "base"  # Возвращаем базовый вариант
        
        test = self.active_tests[test_name]
        variants = [v.name for v in test.variants if v.is_active]
        
        if not variants:
            return "base"
        
        # Взвешенный случайный выбор
        weights = [test.traffic_split.get(v, 0) for v in variants]
        total_weight = sum(weights)
        
        if total_weight == 0:
            return random.choice(variants)
        
        # Нормализуем веса
        normalized_weights = [w/total_weight for w in weights]
        
        return np.random.choice(variants, p=normalized_weights)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def process_with_variant(self, dialog: str, variant_name: str, 
                                 context: Dict[str, Any] = None) -> TestResult:
        """Обработка диалога с выбранным вариантом промпта"""
        
        if variant_name not in self.variants:
            variant_name = "base"
        
        variant = self.variants[variant_name]
        start_time = datetime.now()
        
        # Подготовка промпта с контекстом
        prompt_text = variant.prompt_text.format(
            dialog=dialog,
            dialog_type=context.get('dialog_type', 'unknown') if context else 'unknown',
            dialog_length=len(dialog),
            complexity_level=self._assess_complexity(dialog)
        )
        
        try:
            # Вызов LLM
            response = await openai.ChatCompletion.acreate(
                model=variant.model_config["model"],
                messages=[{"role": "user", "content": prompt_text}],
                max_tokens=1000,
                temperature=variant.model_config["temperature"]
            )
            
            output_text = response.choices[0].message.content.strip()
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Оценка качества результата
            quality_score = self._evaluate_output_quality(output_text, dialog)
            
            # Создание результата
            result = TestResult(
                variant_name=variant_name,
                input_text=dialog,
                output_text=output_text,
                quality_score=quality_score,
                processing_time=processing_time,
                timestamp=datetime.now(),
                metadata=context
            )
            
            # Сохранение результата
            self.test_results.append(result)
            
            # Обновление истории производительности
            if variant_name not in self.performance_history:
                self.performance_history[variant_name] = []
            self.performance_history[variant_name].append(quality_score)
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка обработки с вариантом {variant_name}: {e}")
            # Возвращаем пустой результат
            return TestResult(
                variant_name=variant_name,
                input_text=dialog,
                output_text="",
                quality_score=0.0,
                processing_time=(datetime.now() - start_time).total_seconds(),
                timestamp=datetime.now(),
                metadata=context
            )
    
    def _assess_complexity(self, dialog: str) -> str:
        """Оценка сложности диалога"""
        length = len(dialog)
        sentences = dialog.count('.') + dialog.count('!') + dialog.count('?')
        
        if length < 100 or sentences < 3:
            return "low"
        elif length < 500 or sentences < 10:
            return "medium"
        else:
            return "high"
    
    def _evaluate_output_quality(self, output: str, input_dialog: str) -> float:
        """Оценка качества выходного результата"""
        if not output:
            return 0.0
        
        score = 0.0
        
        # Проверка на JSON формат
        try:
            data = json.loads(output)
            score += 0.3
        except:
            pass
        
        # Проверка на наличие всех полей
        if isinstance(data, dict):
            required_fields = ['problems', 'ideas', 'barriers', 'quotes']
            present_fields = sum(1 for field in required_fields if field in data)
            score += (present_fields / len(required_fields)) * 0.3
        
        # Проверка на качество цитат
        if 'quotes' in data and isinstance(data['quotes'], list):
            quote_quality = self._evaluate_quotes_quality(data['quotes'])
            score += quote_quality * 0.4
        
        return min(1.0, score)
    
    def _evaluate_quotes_quality(self, quotes: List[str]) -> float:
        """Оценка качества цитат"""
        if not quotes:
            return 0.0
        
        total_score = 0.0
        
        for quote in quotes:
            quote_score = 0.0
            
            # Длина цитаты
            if 15 <= len(quote) <= 200:
                quote_score += 0.3
            
            # Отсутствие мусора
            garbage_words = ['угу', 'ага', 'да', 'нет', 'хм', 'эм']
            if not any(word in quote.lower() for word in garbage_words):
                quote_score += 0.3
            
            # Наличие ключевых слов
            keywords = ['доставка', 'заказ', 'курьер', 'адрес', 'время', 'оплата']
            if any(keyword in quote.lower() for keyword in keywords):
                quote_score += 0.4
            
            total_score += quote_score
        
        return total_score / len(quotes)
    
    def analyze_test_results(self, test_name: str) -> Dict[str, Any]:
        """Анализ результатов A/B теста"""
        if test_name not in self.active_tests:
            return {"error": "Тест не найден"}
        
        test = self.active_tests[test_name]
        variant_names = [v.name for v in test.variants]
        
        # Фильтруем результаты по тесту
        test_results = [r for r in self.test_results 
                       if r.variant_name in variant_names]
        
        if not test_results:
            return {"error": "Нет результатов для анализа"}
        
        # Группируем по вариантам
        results_by_variant = {}
        for variant_name in variant_names:
            variant_results = [r for r in test_results if r.variant_name == variant_name]
            if variant_results:
                results_by_variant[variant_name] = variant_results
        
        # Анализ статистики
        analysis = {
            "test_name": test_name,
            "total_samples": len(test_results),
            "variants": {}
        }
        
        for variant_name, results in results_by_variant.items():
            quality_scores = [r.quality_score for r in results]
            processing_times = [r.processing_time for r in results]
            
            analysis["variants"][variant_name] = {
                "sample_size": len(results),
                "avg_quality": statistics.mean(quality_scores),
                "std_quality": statistics.stdev(quality_scores) if len(quality_scores) > 1 else 0,
                "avg_processing_time": statistics.mean(processing_times),
                "min_quality": min(quality_scores),
                "max_quality": max(quality_scores)
            }
        
        # Статистическое сравнение
        if len(results_by_variant) >= 2:
            variant_names_list = list(results_by_variant.keys())
            baseline = variant_names_list[0]
            
            for variant_name in variant_names_list[1:]:
                baseline_scores = [r.quality_score for r in results_by_variant[baseline]]
                variant_scores = [r.quality_score for r in results_by_variant[variant_name]]
                
                try:
                    # t-test
                    t_stat, p_value = stats.ttest_ind(baseline_scores, variant_scores)
                    
                    analysis["variants"][variant_name]["statistical_comparison"] = {
                        "baseline": baseline,
                        "t_statistic": t_stat,
                        "p_value": p_value,
                        "significant": p_value < (1 - test.confidence_level),
                        "improvement": statistics.mean(variant_scores) - statistics.mean(baseline_scores)
                    }
                except Exception as e:
                    logger.warning(f"Ошибка статистического анализа: {e}")
        
        return analysis
    
    def get_best_variant(self, test_name: str) -> Optional[str]:
        """Получение лучшего варианта по результатам теста"""
        analysis = self.analyze_test_results(test_name)
        
        if "error" in analysis:
            return None
        
        best_variant = None
        best_score = -1
        
        for variant_name, stats in analysis["variants"].items():
            if stats["avg_quality"] > best_score:
                best_score = stats["avg_quality"]
                best_variant = variant_name
        
        return best_variant
    
    def auto_switch_to_best(self, test_name: str):
        """Автоматическое переключение на лучший вариант"""
        best_variant = self.get_best_variant(test_name)
        
        if best_variant and test_name in self.active_tests:
            test = self.active_tests[test_name]
            
            # Деактивируем все варианты
            for variant in test.variants:
                variant.is_active = False
            
            # Активируем лучший
            for variant in test.variants:
                if variant.name == best_variant:
                    variant.is_active = True
                    variant.weight = 1.0
                    break
            
            logger.info(f"Автоматически переключились на лучший вариант: {best_variant}")
    
    def get_performance_dashboard(self) -> Dict[str, Any]:
        """Получение дашборда производительности"""
        dashboard = {
            "total_tests": len(self.active_tests),
            "total_results": len(self.test_results),
            "variants_performance": {},
            "recent_trends": {}
        }
        
        # Производительность по вариантам
        for variant_name, scores in self.performance_history.items():
            if scores:
                dashboard["variants_performance"][variant_name] = {
                    "avg_quality": statistics.mean(scores),
                    "total_samples": len(scores),
                    "trend": self._calculate_trend(scores[-10:]) if len(scores) >= 10 else "insufficient_data"
                }
        
        # Недавние тренды
        recent_results = [r for r in self.test_results 
                         if r.timestamp > datetime.now() - timedelta(hours=24)]
        
        if recent_results:
            dashboard["recent_trends"] = {
                "last_24h_samples": len(recent_results),
                "avg_quality_24h": statistics.mean([r.quality_score for r in recent_results]),
                "best_variant_24h": max(set([r.variant_name for r in recent_results]), 
                                      key=[r.variant_name for r in recent_results].count)
            }
        
        return dashboard
    
    def _calculate_trend(self, scores: List[float]) -> str:
        """Вычисление тренда производительности"""
        if len(scores) < 2:
            return "insufficient_data"
        
        first_half = scores[:len(scores)//2]
        second_half = scores[len(scores)//2:]
        
        first_avg = statistics.mean(first_half)
        second_avg = statistics.mean(second_half)
        
        if second_avg > first_avg * 1.05:
            return "improving"
        elif second_avg < first_avg * 0.95:
            return "declining"
        else:
            return "stable"
    
    def save_test_data(self, filepath: str):
        """Сохранение данных тестирования"""
        data = {
            "variants": {name: asdict(variant) for name, variant in self.variants.items()},
            "test_results": [asdict(result) for result in self.test_results],
            "active_tests": {name: asdict(test) for name, test in self.active_tests.items()},
            "performance_history": self.performance_history
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    def load_test_data(self, filepath: str):
        """Загрузка данных тестирования"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Восстанавливаем варианты
        self.variants = {}
        for name, variant_data in data.get("variants", {}).items():
            variant_data["created_at"] = datetime.fromisoformat(variant_data["created_at"])
            self.variants[name] = PromptVariant(**variant_data)
        
        # Восстанавливаем результаты тестов
        self.test_results = []
        for result_data in data.get("test_results", []):
            result_data["timestamp"] = datetime.fromisoformat(result_data["timestamp"])
            self.test_results.append(TestResult(**result_data))
        
        # Восстанавливаем активные тесты
        self.active_tests = {}
        for name, test_data in data.get("active_tests", {}).items():
            variants = [PromptVariant(**v) for v in test_data["variants"]]
            test_data["variants"] = variants
            self.active_tests[name] = ABTestConfig(**test_data)
        
        self.performance_history = data.get("performance_history", {})

# Пример использования
if __name__ == "__main__":
    # Тестирование системы адаптивных промптов
    config = {
        'openai_api_key': 'your-api-key'
    }
    
    system = AdaptivePromptSystem(config)
    
    # Создаем A/B тест
    test_config = system.create_ab_test(
        test_name="quote_quality_test",
        variants=["base", "detailed", "contextual"],
        traffic_split={"base": 0.3, "detailed": 0.4, "contextual": 0.3}
    )
    
    print("=== Система адаптивных промптов ===")
    print(f"Создан тест: {test_config.test_name}")
    print(f"Варианты: {[v.name for v in test_config.variants]}")
    
    # Тестируем выбор вариантов
    for i in range(10):
        variant = system.select_variant("quote_quality_test")
        print(f"Выбран вариант {i+1}: {variant}")
    
    # Анализ результатов
    analysis = system.analyze_test_results("quote_quality_test")
    print(f"\nАнализ результатов: {analysis}")
    
    # Дашборд производительности
    dashboard = system.get_performance_dashboard()
    print(f"\nДашборд: {dashboard}")
