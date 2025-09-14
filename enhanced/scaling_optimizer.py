#!/usr/bin/env python3
"""
Scaling Optimizer for High-Volume Processing
Система масштабирования для обработки 10,000+ диалогов
"""

import asyncio
import aiohttp
import json
import logging
import multiprocessing as mp
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import redis
import psutil
import time
import queue
import threading
from tenacity import retry, stop_after_attempt, wait_exponential
import openai
from tqdm import tqdm
import pickle
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class ProcessingTask:
    """Задача обработки"""
    task_id: str
    dialog: str
    priority: int = 1  # 1-5, где 5 - высший приоритет
    created_at: datetime = None
    retry_count: int = 0
    max_retries: int = 3
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

@dataclass
class ProcessingResult:
    """Результат обработки"""
    task_id: str
    success: bool
    result: Dict[str, Any] = None
    error: str = None
    processing_time: float = 0.0
    worker_id: str = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

@dataclass
class WorkerStats:
    """Статистика воркера"""
    worker_id: str
    tasks_processed: int = 0
    total_processing_time: float = 0.0
    error_count: int = 0
    last_activity: datetime = None
    is_active: bool = True

class ScalingOptimizer:
    """Оптимизатор масштабирования для высоконагруженной обработки"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis_client = None
        self.task_queue = queue.PriorityQueue()
        self.result_queue = queue.Queue()
        self.workers: Dict[str, WorkerStats] = {}
        self.processing_stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'avg_processing_time': 0.0,
            'throughput_per_hour': 0.0
        }
        
        # Настройки масштабирования
        self.scaling_config = {
            'max_workers': min(32, mp.cpu_count() * 4),
            'batch_size': 100,
            'max_memory_usage': 0.8,  # 80% от доступной памяти
            'max_queue_size': 10000,
            'task_timeout': 300,  # 5 минут
            'retry_delay': 5,  # секунд
            'cache_ttl': 3600,  # 1 час
            'enable_caching': True,
            'enable_redis': False
        }
        
        # Кэш для результатов
        self.result_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Инициализация Redis (если включен)
        if self.scaling_config['enable_redis']:
            self._init_redis()
    
    def _init_redis(self):
        """Инициализация Redis для кэширования"""
        try:
            self.redis_client = redis.Redis(
                host=self.config.get('redis_host', 'localhost'),
                port=self.config.get('redis_port', 6379),
                db=self.config.get('redis_db', 0),
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Redis подключен успешно")
        except Exception as e:
            logger.warning(f"Не удалось подключиться к Redis: {e}")
            self.scaling_config['enable_redis'] = False
    
    def process_dialogs_batch(self, dialogs: List[str], 
                            processing_function: Callable,
                            progress_callback: Optional[Callable] = None) -> List[ProcessingResult]:
        """Обработка батча диалогов с оптимизацией"""
        
        logger.info(f"Начинаем обработку {len(dialogs)} диалогов")
        start_time = datetime.now()
        
        # Создаем задачи
        tasks = []
        for i, dialog in enumerate(dialogs):
            task_id = f"task_{i}_{int(time.time())}"
            task = ProcessingTask(
                task_id=task_id,
                dialog=dialog,
                priority=self._calculate_priority(dialog),
                metadata={'dialog_index': i}
            )
            tasks.append(task)
        
        # Обрабатываем задачи
        results = []
        
        if len(tasks) <= self.scaling_config['batch_size']:
            # Маленький батч - обрабатываем последовательно
            results = self._process_tasks_sequential(tasks, processing_function, progress_callback)
        else:
            # Большой батч - используем параллельную обработку
            results = self._process_tasks_parallel(tasks, processing_function, progress_callback)
        
        # Обновляем статистику
        self._update_processing_stats(results, start_time)
        
        logger.info(f"Обработка завершена: {len(results)} результатов за {(datetime.now() - start_time).total_seconds():.2f} сек")
        
        return results
    
    def _calculate_priority(self, dialog: str) -> int:
        """Вычисление приоритета задачи на основе сложности диалога"""
        # Простая эвристика приоритета
        length = len(dialog)
        complexity_score = 0
        
        # Длина диалога
        if length > 1000:
            complexity_score += 2
        elif length > 500:
            complexity_score += 1
        
        # Количество предложений
        sentence_count = dialog.count('.') + dialog.count('!') + dialog.count('?')
        if sentence_count > 10:
            complexity_score += 1
        
        # Наличие ключевых слов
        keywords = ['проблема', 'жалоба', 'доставка', 'заказ', 'курьер']
        if any(keyword in dialog.lower() for keyword in keywords):
            complexity_score += 1
        
        # Приоритет: 1-5 (5 - высший)
        return min(5, max(1, 5 - complexity_score))
    
    def _process_tasks_sequential(self, tasks: List[ProcessingTask], 
                                processing_function: Callable,
                                progress_callback: Optional[Callable] = None) -> List[ProcessingResult]:
        """Последовательная обработка задач"""
        results = []
        
        for i, task in enumerate(tasks):
            try:
                # Проверяем кэш
                cached_result = self._get_cached_result(task)
                if cached_result:
                    results.append(cached_result)
                    self.cache_hits += 1
                    continue
                
                # Обрабатываем задачу
                start_time = time.time()
                result_data = processing_function(task.dialog)
                processing_time = time.time() - start_time
                
                # Создаем результат
                result = ProcessingResult(
                    task_id=task.task_id,
                    success=True,
                    result=result_data,
                    processing_time=processing_time,
                    worker_id="sequential"
                )
                
                results.append(result)
                
                # Кэшируем результат
                self._cache_result(task, result)
                
                # Вызываем callback прогресса
                if progress_callback:
                    progress_callback(i + 1, len(tasks), result)
                
            except Exception as e:
                logger.error(f"Ошибка обработки задачи {task.task_id}: {e}")
                result = ProcessingResult(
                    task_id=task.task_id,
                    success=False,
                    error=str(e),
                    worker_id="sequential"
                )
                results.append(result)
        
        return results
    
    def _process_tasks_parallel(self, tasks: List[ProcessingTask], 
                              processing_function: Callable,
                              progress_callback: Optional[Callable] = None) -> List[ProcessingResult]:
        """Параллельная обработка задач"""
        
        # Определяем оптимальное количество воркеров
        optimal_workers = self._calculate_optimal_workers(len(tasks))
        
        logger.info(f"Используем {optimal_workers} воркеров для обработки {len(tasks)} задач")
        
        # Разделяем задачи на чанки
        chunk_size = max(1, len(tasks) // optimal_workers)
        task_chunks = [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]
        
        results = []
        completed_tasks = 0
        
        # Обрабатываем чанки параллельно
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            # Отправляем задачи
            future_to_chunk = {
                executor.submit(self._process_task_chunk, chunk, processing_function): chunk 
                for chunk in task_chunks
            }
            
            # Собираем результаты
            for future in as_completed(future_to_chunk):
                try:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    completed_tasks += len(chunk_results)
                    
                    # Вызываем callback прогресса
                    if progress_callback:
                        progress_callback(completed_tasks, len(tasks), None)
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки чанка: {e}")
        
        return results
    
    def _process_task_chunk(self, tasks: List[ProcessingTask], 
                           processing_function: Callable) -> List[ProcessingResult]:
        """Обработка чанка задач"""
        results = []
        worker_id = f"worker_{threading.current_thread().ident}"
        
        for task in tasks:
            try:
                # Проверяем кэш
                cached_result = self._get_cached_result(task)
                if cached_result:
                    cached_result.worker_id = worker_id
                    results.append(cached_result)
                    self.cache_hits += 1
                    continue
                
                # Обрабатываем задачу
                start_time = time.time()
                result_data = processing_function(task.dialog)
                processing_time = time.time() - start_time
                
                # Создаем результат
                result = ProcessingResult(
                    task_id=task.task_id,
                    success=True,
                    result=result_data,
                    processing_time=processing_time,
                    worker_id=worker_id
                )
                
                results.append(result)
                
                # Кэшируем результат
                self._cache_result(task, result)
                
            except Exception as e:
                logger.error(f"Ошибка обработки задачи {task.task_id}: {e}")
                result = ProcessingResult(
                    task_id=task.task_id,
                    success=False,
                    error=str(e),
                    worker_id=worker_id
                )
                results.append(result)
        
        return results
    
    def _calculate_optimal_workers(self, task_count: int) -> int:
        """Вычисление оптимального количества воркеров"""
        # Базовое количество воркеров
        base_workers = min(self.scaling_config['max_workers'], mp.cpu_count() * 2)
        
        # Учитываем количество задач
        if task_count < 100:
            return min(4, base_workers)
        elif task_count < 1000:
            return min(8, base_workers)
        else:
            return base_workers
    
    def _get_cached_result(self, task: ProcessingTask) -> Optional[ProcessingResult]:
        """Получение результата из кэша"""
        if not self.scaling_config['enable_caching']:
            return None
        
        # Создаем ключ кэша
        cache_key = self._generate_cache_key(task.dialog)
        
        # Проверяем локальный кэш
        if cache_key in self.result_cache:
            cached_data = self.result_cache[cache_key]
            if time.time() - cached_data['timestamp'] < self.scaling_config['cache_ttl']:
                return cached_data['result']
            else:
                del self.result_cache[cache_key]
        
        # Проверяем Redis кэш
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    result_data = json.loads(cached_data)
                    return ProcessingResult(**result_data)
            except Exception as e:
                logger.warning(f"Ошибка чтения из Redis: {e}")
        
        return None
    
    def _cache_result(self, task: ProcessingTask, result: ProcessingResult):
        """Кэширование результата"""
        if not self.scaling_config['enable_caching']:
            return
        
        cache_key = self._generate_cache_key(task.dialog)
        cache_data = {
            'result': asdict(result),
            'timestamp': time.time()
        }
        
        # Сохраняем в локальный кэш
        self.result_cache[cache_key] = cache_data
        
        # Очищаем старые записи из кэша
        if len(self.result_cache) > 1000:
            self._cleanup_cache()
        
        # Сохраняем в Redis
        if self.redis_client:
            try:
                self.redis_client.setex(
                    cache_key, 
                    self.scaling_config['cache_ttl'],
                    json.dumps(asdict(result), default=str)
                )
            except Exception as e:
                logger.warning(f"Ошибка записи в Redis: {e}")
    
    def _generate_cache_key(self, dialog: str) -> str:
        """Генерация ключа кэша"""
        # Создаем хэш от диалога
        dialog_hash = hashlib.md5(dialog.encode('utf-8')).hexdigest()
        return f"dialog_result_{dialog_hash}"
    
    def _cleanup_cache(self):
        """Очистка кэша от старых записей"""
        current_time = time.time()
        expired_keys = []
        
        for key, data in self.result_cache.items():
            if current_time - data['timestamp'] > self.scaling_config['cache_ttl']:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.result_cache[key]
    
    def _update_processing_stats(self, results: List[ProcessingResult], start_time: datetime):
        """Обновление статистики обработки"""
        self.processing_stats['total_tasks'] += len(results)
        self.processing_stats['completed_tasks'] += sum(1 for r in results if r.success)
        self.processing_stats['failed_tasks'] += sum(1 for r in results if not r.success)
        
        # Вычисляем среднее время обработки
        successful_results = [r for r in results if r.success and r.processing_time > 0]
        if successful_results:
            avg_time = np.mean([r.processing_time for r in successful_results])
            self.processing_stats['avg_processing_time'] = avg_time
        
        # Вычисляем пропускную способность
        total_time = (datetime.now() - start_time).total_seconds()
        if total_time > 0:
            self.processing_stats['throughput_per_hour'] = (len(results) / total_time) * 3600
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Получение статистики обработки"""
        return {
            **self.processing_stats,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0,
            'memory_usage': psutil.virtual_memory().percent,
            'cpu_usage': psutil.cpu_percent(),
            'active_workers': len([w for w in self.workers.values() if w.is_active])
        }
    
    def optimize_for_volume(self, expected_dialogs: int) -> Dict[str, Any]:
        """Оптимизация системы для заданного объема диалогов"""
        recommendations = []
        
        # Анализируем текущую производительность
        current_throughput = self.processing_stats.get('throughput_per_hour', 0)
        if current_throughput == 0:
            current_throughput = 100  # Предполагаем базовую производительность
        
        # Вычисляем время обработки
        estimated_hours = expected_dialogs / current_throughput
        
        # Рекомендации по масштабированию
        if expected_dialogs > 10000:
            recommendations.append("Рекомендуется использовать Redis для кэширования")
            recommendations.append("Увеличьте количество воркеров до 16-32")
            recommendations.append("Рассмотрите использование GPU для эмбеддингов")
        
        if estimated_hours > 24:
            recommendations.append("Обработка займет более 24 часов. Рассмотрите батчевую обработку")
            recommendations.append("Используйте checkpointing для сохранения прогресса")
        
        if expected_dialogs > 50000:
            recommendations.append("Для очень больших объемов рассмотрите распределенную обработку")
            recommendations.append("Используйте Apache Spark или Dask для масштабирования")
        
        # Оптимизация конфигурации
        optimal_config = self.scaling_config.copy()
        
        if expected_dialogs > 5000:
            optimal_config['max_workers'] = min(32, mp.cpu_count() * 4)
            optimal_config['batch_size'] = 200
            optimal_config['enable_caching'] = True
            optimal_config['enable_redis'] = True
        
        return {
            'expected_dialogs': expected_dialogs,
            'estimated_processing_time_hours': estimated_hours,
            'current_throughput': current_throughput,
            'recommendations': recommendations,
            'optimal_config': optimal_config
        }
    
    def create_progress_tracker(self, total_tasks: int) -> Callable:
        """Создание трекера прогресса"""
        progress_bar = tqdm(total=total_tasks, desc="Обработка диалогов", unit="диалог")
        
        def progress_callback(completed: int, total: int, result: ProcessingResult = None):
            progress_bar.update(1)
            
            if result and not result.success:
                progress_bar.set_postfix({
                    'Ошибки': f"{self.processing_stats['failed_tasks']}/{completed}",
                    'Время': f"{self.processing_stats['avg_processing_time']:.2f}с"
                })
            else:
                progress_bar.set_postfix({
                    'Успешно': f"{self.processing_stats['completed_tasks']}/{completed}",
                    'Время': f"{self.processing_stats['avg_processing_time']:.2f}с"
                })
        
        return progress_callback
    
    def save_processing_results(self, results: List[ProcessingResult], filepath: str):
        """Сохранение результатов обработки"""
        results_data = [asdict(result) for result in results]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Результаты сохранены: {filepath}")
    
    def load_processing_results(self, filepath: str) -> List[ProcessingResult]:
        """Загрузка результатов обработки"""
        with open(filepath, 'r', encoding='utf-8') as f:
            results_data = json.load(f)
        
        results = []
        for result_data in results_data:
            result_data['timestamp'] = datetime.fromisoformat(result_data['timestamp'])
            results.append(ProcessingResult(**result_data))
        
        return results

# Пример использования
if __name__ == "__main__":
    # Тестирование системы масштабирования
    config = {
        'redis_host': 'localhost',
        'redis_port': 6379,
        'redis_db': 0
    }
    
    optimizer = ScalingOptimizer(config)
    
    # Тестовая функция обработки
    def test_processing_function(dialog: str) -> Dict[str, Any]:
        """Тестовая функция обработки диалога"""
        time.sleep(0.1)  # Симулируем обработку
        
        return {
            'dialog_length': len(dialog),
            'word_count': len(dialog.split()),
            'has_delivery_keywords': any(kw in dialog.lower() for kw in ['доставка', 'заказ', 'курьер']),
            'processed_at': datetime.now().isoformat()
        }
    
    # Создаем тестовые диалоги
    test_dialogs = [
        f"Тестовый диалог {i}: Клиент звонит по поводу доставки заказа номер {i}. Менеджер отвечает на вопросы."
        for i in range(100)
    ]
    
    print("=== Тестирование системы масштабирования ===")
    
    # Создаем трекер прогресса
    progress_callback = optimizer.create_progress_tracker(len(test_dialogs))
    
    # Обрабатываем диалоги
    results = optimizer.process_dialogs_batch(
        dialogs=test_dialogs,
        processing_function=test_processing_function,
        progress_callback=progress_callback
    )
    
    # Получаем статистику
    stats = optimizer.get_processing_stats()
    print(f"\nСтатистика обработки: {stats}")
    
    # Анализируем оптимизацию для 10,000 диалогов
    optimization = optimizer.optimize_for_volume(10000)
    print(f"\nОптимизация для 10,000 диалогов: {optimization}")
    
    # Сохраняем результаты
    optimizer.save_processing_results(results, "test_results.json")
    print("Результаты сохранены в test_results.json")
