#!/usr/bin/env python3
"""
Integrated Enhanced Quality System
Интегрированная система улучшенного качества анализа диалогов
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

# Импортируем все компоненты системы
from .quality_autocorrection import QualityAutoCorrector, QualityMonitor
from .adaptive_prompts import AdaptivePromptSystem
from .continuous_learning import ContinuousLearningSystem
from .quality_monitoring import QualityMonitor as SystemMonitor
from .scaling_optimizer import ScalingOptimizer

logger = logging.getLogger(__name__)

@dataclass
class ProcessingConfig:
    """Конфигурация обработки"""
    enable_autocorrection: bool = True
    enable_adaptive_prompts: bool = True
    enable_continuous_learning: bool = True
    enable_monitoring: bool = True
    enable_scaling: bool = True
    max_dialogs_per_batch: int = 1000
    quality_threshold: float = 0.7
    auto_save_results: bool = True
    output_directory: str = "enhanced_results"

@dataclass
class SystemStatus:
    """Статус системы"""
    is_initialized: bool = False
    components_loaded: List[str] = None
    last_processing_time: Optional[datetime] = None
    total_dialogs_processed: int = 0
    avg_quality_score: float = 0.0
    system_health: str = "unknown"
    
    def __post_init__(self):
        if self.components_loaded is None:
            self.components_loaded = []

class IntegratedQualitySystem:
    """Интегрированная система улучшенного качества"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.status = SystemStatus()
        
        # Компоненты системы
        self.quality_corrector: Optional[QualityAutoCorrector] = None
        self.adaptive_prompts: Optional[AdaptivePromptSystem] = None
        self.learning_system: Optional[ContinuousLearningSystem] = None
        self.monitor: Optional[SystemMonitor] = None
        self.scaling_optimizer: Optional[ScalingOptimizer] = None
        
        # Конфигурация обработки
        self.processing_config = ProcessingConfig(**config.get('processing', {}))
        
        # Инициализация системы
        self._initialize_system()
    
    def _initialize_system(self):
        """Инициализация всех компонентов системы"""
        logger.info("Инициализация интегрированной системы качества...")
        
        try:
            # 1. Система автокоррекции качества
            if self.processing_config.enable_autocorrection:
                self.quality_corrector = QualityAutoCorrector(self.config)
                self.status.components_loaded.append("quality_autocorrection")
                logger.info("✓ Система автокоррекции качества загружена")
            
            # 2. Адаптивные промпты
            if self.processing_config.enable_adaptive_prompts:
                self.adaptive_prompts = AdaptivePromptSystem(self.config)
                # Создаем A/B тест по умолчанию
                self.adaptive_prompts.create_ab_test(
                    test_name="default_quality_test",
                    variants=["base", "detailed", "contextual"],
                    traffic_split={"base": 0.3, "detailed": 0.4, "contextual": 0.3}
                )
                self.status.components_loaded.append("adaptive_prompts")
                logger.info("✓ Система адаптивных промптов загружена")
            
            # 3. Непрерывное обучение
            if self.processing_config.enable_continuous_learning:
                self.learning_system = ContinuousLearningSystem(self.config)
                self.status.components_loaded.append("continuous_learning")
                logger.info("✓ Система непрерывного обучения загружена")
            
            # 4. Мониторинг качества
            if self.processing_config.enable_monitoring:
                self.monitor = SystemMonitor(self.config)
                self.status.components_loaded.append("quality_monitoring")
                logger.info("✓ Система мониторинга качества загружена")
            
            # 5. Масштабирование
            if self.processing_config.enable_scaling:
                self.scaling_optimizer = ScalingOptimizer(self.config)
                self.status.components_loaded.append("scaling_optimizer")
                logger.info("✓ Система масштабирования загружена")
            
            self.status.is_initialized = True
            self.status.system_health = "healthy"
            
            logger.info(f"Система инициализирована успешно. Загружено компонентов: {len(self.status.components_loaded)}")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации системы: {e}")
            self.status.system_health = "error"
            raise
    
    async def process_dialogs_enhanced(self, dialogs: List[str], 
                                     context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Улучшенная обработка диалогов с использованием всех компонентов"""
        
        if not self.status.is_initialized:
            raise RuntimeError("Система не инициализирована")
        
        logger.info(f"Начинаем улучшенную обработку {len(dialogs)} диалогов")
        start_time = datetime.now()
        
        # Создаем директорию для результатов
        output_dir = Path(self.processing_config.output_directory)
        output_dir.mkdir(exist_ok=True)
        
        # Определяем стратегию обработки
        if len(dialogs) > self.processing_config.max_dialogs_per_batch:
            # Большой объем - используем масштабирование
            results = await self._process_large_volume(dialogs, context)
        else:
            # Обычный объем - стандартная обработка
            results = await self._process_standard_volume(dialogs, context)
        
        # Обновляем статус системы
        self.status.last_processing_time = datetime.now()
        self.status.total_dialogs_processed += len(dialogs)
        
        # Вычисляем среднее качество
        if results:
            quality_scores = [r.get('quality_score', 0) for r in results if r.get('quality_score')]
            if quality_scores:
                self.status.avg_quality_score = np.mean(quality_scores)
        
        # Сохраняем результаты
        if self.processing_config.auto_save_results:
            self._save_processing_results(results, output_dir)
        
        # Генерируем отчет
        self._generate_processing_report(results, output_dir)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Обработка завершена за {processing_time:.2f} секунд")
        
        return results
    
    async def _process_standard_volume(self, dialogs: List[str], 
                                     context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Стандартная обработка диалогов"""
        results = []
        
        for i, dialog in enumerate(dialogs):
            try:
                # 1. Выбираем лучший промпт (если включены адаптивные промпты)
                prompt_variant = "base"
                if self.adaptive_prompts:
                    prompt_variant = self.adaptive_prompts.select_variant("default_quality_test")
                
                # 2. Обрабатываем диалог
                if self.adaptive_prompts:
                    # Используем адаптивные промпты
                    processing_result = await self.adaptive_prompts.process_with_variant(
                        dialog, prompt_variant, context
                    )
                    
                    # Парсим результат
                    try:
                        extracted_entities = json.loads(processing_result.output_text)
                    except:
                        extracted_entities = {"problems": [], "ideas": [], "barriers": [], "quotes": []}
                    
                    quality_score = processing_result.quality_score
                else:
                    # Базовая обработка (заглушка)
                    extracted_entities = self._basic_entity_extraction(dialog)
                    quality_score = 0.5
                
                # 3. Автокоррекция качества (если включена)
                if self.quality_corrector and quality_score < self.processing_config.quality_threshold:
                    corrected_entities = self._apply_quality_correction(extracted_entities, dialog)
                    extracted_entities = corrected_entities
                    quality_score = self._calculate_quality_score(corrected_entities)
                
                # 4. Непрерывное обучение (если включено)
                if self.learning_system:
                    self.learning_system.add_learning_example(
                        dialog=dialog,
                        extracted_entities=extracted_entities,
                        quality_score=quality_score,
                        source="enhanced_processing"
                    )
                
                # 5. Мониторинг (если включен)
                if self.monitor:
                    self.monitor.record_processing_result(
                        dialog=dialog,
                        extracted_entities=extracted_entities,
                        quality_score=quality_score,
                        processing_time=0.1,  # Заглушка
                        prompt_variant=prompt_variant
                    )
                
                # Создаем результат
                result = {
                    'dialog_id': i,
                    'dialog': dialog,
                    'extracted_entities': extracted_entities,
                    'quality_score': quality_score,
                    'prompt_variant': prompt_variant,
                    'processing_timestamp': datetime.now().isoformat(),
                    'corrections_applied': self._get_corrections_applied(extracted_entities)
                }
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Ошибка обработки диалога {i}: {e}")
                results.append({
                    'dialog_id': i,
                    'dialog': dialog,
                    'error': str(e),
                    'quality_score': 0.0
                })
        
        return results
    
    async def _process_large_volume(self, dialogs: List[str], 
                                  context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Обработка большого объема диалогов с масштабированием"""
        
        if not self.scaling_optimizer:
            # Fallback к стандартной обработке
            return await self._process_standard_volume(dialogs, context)
        
        # Создаем функцию обработки для масштабирования
        def processing_function(dialog: str) -> Dict[str, Any]:
            # Синхронная версия обработки
            try:
                # Базовая обработка
                extracted_entities = self._basic_entity_extraction(dialog)
                quality_score = self._calculate_quality_score(extracted_entities)
                
                # Автокоррекция
                if self.quality_corrector and quality_score < self.processing_config.quality_threshold:
                    extracted_entities = self._apply_quality_correction(extracted_entities, dialog)
                    quality_score = self._calculate_quality_score(extracted_entities)
                
                return {
                    'extracted_entities': extracted_entities,
                    'quality_score': quality_score
                }
            except Exception as e:
                logger.error(f"Ошибка обработки: {e}")
                return {
                    'extracted_entities': {"problems": [], "ideas": [], "barriers": [], "quotes": []},
                    'quality_score': 0.0,
                    'error': str(e)
                }
        
        # Обрабатываем с масштабированием
        scaling_results = self.scaling_optimizer.process_dialogs_batch(
            dialogs=dialogs,
            processing_function=processing_function
        )
        
        # Конвертируем результаты
        results = []
        for i, scaling_result in enumerate(scaling_results):
            if scaling_result.success:
                result = {
                    'dialog_id': i,
                    'dialog': dialogs[i],
                    'extracted_entities': scaling_result.result['extracted_entities'],
                    'quality_score': scaling_result.result['quality_score'],
                    'processing_time': scaling_result.processing_time,
                    'worker_id': scaling_result.worker_id,
                    'processing_timestamp': scaling_result.timestamp.isoformat()
                }
            else:
                result = {
                    'dialog_id': i,
                    'dialog': dialogs[i],
                    'error': scaling_result.error,
                    'quality_score': 0.0
                }
            
            results.append(result)
        
        return results
    
    def _basic_entity_extraction(self, dialog: str) -> Dict[str, List[str]]:
        """Базовая извлечение сущностей (заглушка)"""
        # Простая эвристика для демонстрации
        problems = []
        ideas = []
        barriers = []
        quotes = []
        
        # Ищем проблемы
        problem_keywords = ['проблема', 'жалоба', 'ошибка', 'задержка', 'недовольство']
        for keyword in problem_keywords:
            if keyword in dialog.lower():
                problems.append(f"Обнаружена проблема: {keyword}")
        
        # Ищем идеи
        idea_keywords = ['предложение', 'идея', 'рекомендация', 'улучшение']
        for keyword in idea_keywords:
            if keyword in dialog.lower():
                ideas.append(f"Предложение: {keyword}")
        
        # Ищем барьеры
        barrier_keywords = ['барьер', 'препятствие', 'сложность', 'трудность']
        for keyword in barrier_keywords:
            if keyword in dialog.lower():
                barriers.append(f"Барьер: {keyword}")
        
        # Извлекаем цитаты (простая эвристика)
        sentences = dialog.split('.')
        for sentence in sentences:
            if len(sentence.strip()) > 20 and any(kw in sentence.lower() for kw in ['доставка', 'заказ', 'курьер']):
                quotes.append(sentence.strip())
        
        return {
            'problems': problems,
            'ideas': ideas,
            'barriers': barriers,
            'quotes': quotes
        }
    
    def _apply_quality_correction(self, entities: Dict[str, List[str]], dialog: str) -> Dict[str, List[str]]:
        """Применение автокоррекции качества"""
        if not self.quality_corrector:
            return entities
        
        corrected_entities = entities.copy()
        
        # Корректируем цитаты
        if 'quotes' in corrected_entities:
            corrected_quotes = []
            for quote in corrected_entities['quotes']:
                correction_result = self.quality_corrector.correct_quote(quote, dialog)
                if correction_result.corrected and correction_result.quality_score > 0.5:
                    corrected_quotes.append(correction_result.corrected)
            
            # Удаляем дубликаты
            corrected_quotes = self.quality_corrector.remove_duplicates(corrected_quotes)
            corrected_entities['quotes'] = corrected_quotes
        
        return corrected_entities
    
    def _calculate_quality_score(self, entities: Dict[str, List[str]]) -> float:
        """Вычисление оценки качества"""
        score = 0.0
        
        # Базовый счет
        total_entities = sum(len(v) for v in entities.values() if isinstance(v, list))
        if total_entities > 0:
            score += 0.3
        
        # Качество цитат
        quotes = entities.get('quotes', [])
        if quotes:
            quote_scores = []
            for quote in quotes:
                if len(quote) > 20 and not any(word in quote.lower() for word in ['угу', 'ага', 'да', 'нет']):
                    quote_scores.append(1.0)
                else:
                    quote_scores.append(0.3)
            
            if quote_scores:
                score += np.mean(quote_scores) * 0.4
        
        # Наличие всех типов сущностей
        entity_types = ['problems', 'ideas', 'barriers', 'quotes']
        present_types = sum(1 for et in entity_types if entities.get(et))
        score += (present_types / len(entity_types)) * 0.3
        
        return min(1.0, score)
    
    def _get_corrections_applied(self, entities: Dict[str, List[str]]) -> List[str]:
        """Получение списка примененных исправлений"""
        corrections = []
        
        # Проверяем цитаты на мусор
        quotes = entities.get('quotes', [])
        garbage_quotes = sum(1 for q in quotes if any(word in q.lower() for word in ['угу', 'ага', 'да', 'нет']))
        if garbage_quotes > 0:
            corrections.append(f"Удалено {garbage_quotes} мусорных цитат")
        
        # Проверяем дубликаты
        if len(quotes) != len(set(quotes)):
            corrections.append("Удалены дубликаты цитат")
        
        return corrections
    
    def _save_processing_results(self, results: List[Dict[str, Any]], output_dir: Path):
        """Сохранение результатов обработки"""
        # Сохраняем основные результаты
        results_file = output_dir / "enhanced_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        # Сохраняем статистику
        stats = self.get_system_statistics()
        stats_file = output_dir / "processing_statistics.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Результаты сохранены в {output_dir}")
    
    def _generate_processing_report(self, results: List[Dict[str, Any]], output_dir: Path):
        """Генерация отчета об обработке"""
        if not self.monitor:
            return
        
        # Генерируем HTML дашборд
        dashboard_path = output_dir / "quality_dashboard.html"
        self.monitor.generate_html_dashboard(str(dashboard_path))
        
        # Создаем текстовый отчет
        report_path = output_dir / "processing_report.md"
        self._create_markdown_report(results, report_path)
    
    def _create_markdown_report(self, results: List[Dict[str, Any]], report_path: Path):
        """Создание Markdown отчета"""
        total_dialogs = len(results)
        successful_dialogs = sum(1 for r in results if r.get('quality_score', 0) > 0)
        avg_quality = np.mean([r.get('quality_score', 0) for r in results if r.get('quality_score', 0) > 0])
        
        # Статистика по сущностям
        total_problems = sum(len(r.get('extracted_entities', {}).get('problems', [])) for r in results)
        total_ideas = sum(len(r.get('extracted_entities', {}).get('ideas', [])) for r in results)
        total_barriers = sum(len(r.get('extracted_entities', {}).get('barriers', [])) for r in results)
        total_quotes = sum(len(r.get('extracted_entities', {}).get('quotes', [])) for r in results)
        
        report_content = f"""# Отчет об обработке диалогов

## Общая статистика
- **Всего диалогов**: {total_dialogs}
- **Успешно обработано**: {successful_dialogs} ({successful_dialogs/total_dialogs*100:.1f}%)
- **Среднее качество**: {avg_quality:.2f}

## Извлеченные сущности
- **Проблемы**: {total_problems}
- **Идеи**: {total_ideas}
- **Барьеры**: {total_barriers}
- **Цитаты**: {total_quotes}

## Качество по диалогам
"""
        
        # Добавляем детали по каждому диалогу
        for i, result in enumerate(results[:10]):  # Показываем первые 10
            quality = result.get('quality_score', 0)
            corrections = result.get('corrections_applied', [])
            
            report_content += f"""
### Диалог {i+1}
- **Качество**: {quality:.2f}
- **Исправления**: {', '.join(corrections) if corrections else 'Нет'}
- **Проблемы**: {len(result.get('extracted_entities', {}).get('problems', []))}
- **Идеи**: {len(result.get('extracted_entities', {}).get('ideas', []))}
- **Цитаты**: {len(result.get('extracted_entities', {}).get('quotes', []))}
"""
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Получение статистики системы"""
        stats = {
            'system_status': asdict(self.status),
            'processing_config': asdict(self.processing_config),
            'components_loaded': self.status.components_loaded
        }
        
        # Добавляем статистику компонентов
        if self.monitor:
            stats['monitoring_stats'] = self.monitor.get_processing_stats()
        
        if self.scaling_optimizer:
            stats['scaling_stats'] = self.scaling_optimizer.get_processing_stats()
        
        if self.learning_system:
            stats['learning_insights'] = self.learning_system.get_learning_insights()
        
        return stats
    
    def get_quality_dashboard(self) -> str:
        """Получение дашборда качества"""
        if not self.monitor:
            return "Мониторинг не включен"
        
        dashboard_data = self.monitor.get_quality_dashboard_data()
        return json.dumps(dashboard_data, ensure_ascii=False, indent=2, default=str)
    
    def optimize_system_for_volume(self, expected_dialogs: int) -> Dict[str, Any]:
        """Оптимизация системы для заданного объема"""
        if not self.scaling_optimizer:
            return {"error": "Масштабирование не включено"}
        
        return self.scaling_optimizer.optimize_for_volume(expected_dialogs)

# Пример использования
if __name__ == "__main__":
    # Конфигурация системы
    config = {
        'openai_api_key': 'your-api-key',
        'processing': {
            'enable_autocorrection': True,
            'enable_adaptive_prompts': True,
            'enable_continuous_learning': True,
            'enable_monitoring': True,
            'enable_scaling': True,
            'max_dialogs_per_batch': 1000,
            'quality_threshold': 0.7,
            'output_directory': 'enhanced_results'
        }
    }
    
    # Создаем систему
    system = IntegratedQualitySystem(config)
    
    # Тестовые диалоги
    test_dialogs = [
        "Клиент: Здравствуйте, у меня проблема с доставкой заказа. Менеджер: Расскажите подробнее, что именно произошло?",
        "Угу. Угу угу угу. Угу. Угу. Угу, ну угу",
        "Клиент: Доставка задерживается на 2 часа. Менеджер: Извините за неудобства, курьер в пути.",
        "Клиент: Предлагаю улучшить систему уведомлений. Менеджер: Отличная идея, передам в разработку."
    ]
    
    print("=== Интегрированная система качества ===")
    print(f"Статус системы: {system.status.system_health}")
    print(f"Загружено компонентов: {len(system.status.components_loaded)}")
    
    # Обрабатываем диалоги
    async def main():
        results = await system.process_dialogs_enhanced(test_dialogs)
        
        print(f"\nОбработано диалогов: {len(results)}")
        for i, result in enumerate(results):
            print(f"Диалог {i+1}: качество {result.get('quality_score', 0):.2f}")
        
        # Получаем статистику
        stats = system.get_system_statistics()
        print(f"\nСтатистика системы: {stats}")
    
    # Запускаем обработку
    asyncio.run(main())
