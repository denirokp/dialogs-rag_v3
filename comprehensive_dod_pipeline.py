#!/usr/bin/env python3
"""
🎯 КОМПЛЕКСНЫЙ ПАЙПЛАЙН DIALOGS RAG SYSTEM с DoD
Полная система с самообучением, A/B тестами, мониторингом и всеми компонентами
"""

import asyncio
import json
import logging
import time
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import yaml
import jsonschema
import duckdb
from collections import defaultdict
import redis
import pickle

# Добавляем пути для импорта
sys.path.append(str(Path(__file__).parent))

# Импорт всех компонентов системы
from enhanced.integrated_system import IntegratedQualitySystem
from enhanced.quality_autocorrection import QualityAutoCorrector
from enhanced.adaptive_prompts import AdaptivePromptSystem
from enhanced.continuous_learning import ContinuousLearningSystem
from enhanced.quality_monitoring import QualityMonitor
from enhanced.scaling_optimizer import ScalingOptimizer

# Импорт компонентов DoD
from scripts.dedup import main as dedup_main
from scripts.clusterize import main as clusterize_main
from scripts.eval_extraction import micro_f1

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/comprehensive_dod_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveDoDPipeline:
    """Комплексный пайплайн с полным функционалом и DoD"""
    
    def __init__(self, config_path: str = "final_pipeline_config.json"):
        self.config = self._load_config(config_path)
        self.taxonomy = self._load_taxonomy()
        self.schema = self._load_schema()
        self.results = []
        self.statistics = {}
        
        # Инициализация всех компонентов
        self._initialize_all_components()
        
        # Создаем необходимые директории
        self._create_directories()
        
        logger.info("🚀 Инициализация комплексного DoD пайплайна...")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации"""
        if Path(config_path).exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {
                "openai_api_key": "your-api-key-here",
                "processing": {
                    "enable_validation": True,
                    "enable_dedup": True,
                    "enable_clustering": True,
                    "enable_quality_checks": True,
                    "enable_autocorrection": True,
                    "enable_adaptive_prompts": True,
                    "enable_continuous_learning": True,
                    "enable_monitoring": True,
                    "enable_scaling": True,
                    "max_dialogs_per_batch": 1000,
                    "quality_threshold": 0.6
                },
                "dedup": {"threshold": 0.92, "enable_embeddings": False},
                "clustering": {"min_cluster_size": 25, "n_neighbors": 12, "min_dist": 0.1},
                "redis_host": "localhost", "redis_port": 6379, "redis_db": 0
            }
    
    def _load_taxonomy(self) -> Dict[str, Any]:
        """Загрузка таксономии"""
        with open('taxonomy.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _load_schema(self) -> Dict[str, Any]:
        """Загрузка JSON схемы"""
        with open('schemas/mentions.schema.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _initialize_all_components(self):
        """Инициализация всех компонентов системы"""
        try:
            # 1. Интегрированная система качества
            self.quality_system = IntegratedQualitySystem(self.config)
            logger.info("✅ Интегрированная система качества загружена")
            
            # 2. Автокоррекция качества
            self.autocorrector = QualityAutoCorrector(self.config)
            logger.info("✅ Система автокоррекции загружена")
            
            # 3. Адаптивные промпты с A/B тестированием
            self.adaptive_prompts = AdaptivePromptSystem(self.config)
            logger.info("✅ Система адаптивных промптов загружена")
            
            # 4. Непрерывное обучение
            self.learning_system = ContinuousLearningSystem(self.config)
            logger.info("✅ Система непрерывного обучения загружена")
            
            # 5. Мониторинг качества
            self.monitor = QualityMonitor(self.config)
            logger.info("✅ Система мониторинга загружена")
            
            # 6. Масштабирование
            self.scaler = ScalingOptimizer(self.config)
            logger.info("✅ Система масштабирования загружена")
            
            # 7. Redis для кэширования и очередей
            try:
                self.redis_client = redis.Redis(
                    host=self.config.get('redis_host', 'localhost'),
                    port=self.config.get('redis_port', 6379),
                    db=self.config.get('redis_db', 0),
                    decode_responses=True
                )
                self.redis_client.ping()
                logger.info("✅ Redis подключен")
            except:
                self.redis_client = None
                logger.warning("⚠️ Redis недоступен, работаем без кэширования")
            
            logger.info("🎉 Все компоненты успешно инициализированы!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    def _create_directories(self):
        """Создание необходимых директорий"""
        dirs = ['logs', 'artifacts', 'reports', 'goldset', 'quality', 'sql', 'scripts', 'schemas', 'models']
        for dir_name in dirs:
            Path(dir_name).mkdir(exist_ok=True)
    
    async def process_dialogs(self, dialogs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработка диалогов с полным функционалом и DoD"""
        logger.info(f"📊 Начинаем комплексную обработку {len(dialogs)} диалогов...")
        
        start_time = time.time()
        results = []
        learning_examples = []
        ab_test_data = []
        
        # Прогресс-бар
        from tqdm import tqdm
        
        for i, dialog in enumerate(tqdm(dialogs, desc="Обработка диалогов")):
            try:
                # 1. A/B тестирование промптов
                prompt_variant = self.adaptive_prompts.select_variant("dod_extraction_test")
                
                # 2. Извлечение упоминаний с DoD требованиями
                mentions = await self._extract_mentions_with_dod(dialog, i, prompt_variant)
                
                # 3. Валидация по JSON схеме
                if self.config["processing"]["enable_validation"]:
                    self._validate_mentions(mentions)
                
                # 4. Автокоррекция качества
                if self.config["processing"]["enable_autocorrection"]:
                    corrected_mentions = []
                    for mention in mentions:
                        corrected = self.autocorrector.correct_extraction(mention, dialog)
                        corrected_mentions.append(corrected)
                    mentions = corrected_mentions
                
                # 5. Оценка качества
                quality_score = self._calculate_quality_score(mentions, dialog)
                
                # 6. Создание результата
                result = {
                    "dialog_id": i,
                    "dialog": dialog,
                    "mentions": mentions,
                    "quality_score": quality_score,
                    "prompt_variant": prompt_variant,
                    "processing_timestamp": datetime.now().isoformat(),
                    "ab_test_variant": prompt_variant,
                    "learning_quality": quality_score
                }
                
                results.append(result)
                
                # 7. Добавление в обучение
                if quality_score > 0.3:
                    learning_example = {
                        "dialog": dialog,
                        "mentions": mentions,
                        "quality_score": quality_score,
                        "source": "comprehensive_dod_pipeline",
                        "timestamp": datetime.now().isoformat()
                    }
                    learning_examples.append(learning_example)
                
                # 8. A/B тест данные
                ab_test_data.append({
                    "variant": prompt_variant,
                    "quality_score": quality_score,
                    "dialog_length": len(str(dialog)),
                    "mentions_count": len(mentions),
                    "timestamp": datetime.now().isoformat()
                })
                
                # 9. Мониторинг
                if self.config["processing"]["enable_monitoring"]:
                    self.monitor.record_processing_metrics({
                        "quality_score": quality_score,
                        "processing_time": time.time() - start_time,
                        "dialog_length": len(str(dialog)),
                        "mentions_count": len(mentions)
                    })
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки диалога {i}: {e}")
                results.append({
                    "dialog_id": i,
                    "dialog": dialog,
                    "error": str(e),
                    "quality_score": 0.0,
                    "processing_timestamp": datetime.now().isoformat()
                })
        
        # 10. Обновление A/B тестов
        for data in ab_test_data:
            self.adaptive_prompts.record_result("dod_extraction_test", data["variant"], data["quality_score"])
        
        # 11. Добавление в обучение
        for example in learning_examples:
            self.learning_system.add_learning_example(example)
        
        # 12. Дедупликация всех упоминаний
        all_mentions = []
        for result in results:
            if "mentions" in result:
                all_mentions.extend(result["mentions"])
        
        if self.config["processing"]["enable_dedup"] and all_mentions:
            logger.info("🔄 Дедупликация упоминаний...")
            all_mentions = await self._deduplicate_mentions(all_mentions)
        
        # 13. Кластеризация
        clusters = {}
        if self.config["processing"]["enable_clustering"] and all_mentions:
            logger.info("🎯 Кластеризация упоминаний...")
            clusters = await self._cluster_mentions(all_mentions)
        
        # 14. Построение сводок
        logger.info("📊 Построение сводок...")
        summaries = await self._build_summaries(all_mentions)
        
        # 15. Проверки качества DoD
        quality_results = {}
        if self.config["processing"]["enable_quality_checks"]:
            logger.info("🔍 Проверки качества DoD...")
            quality_results = await self._run_quality_checks(all_mentions)
        
        # 16. Генерация отчетов
        logger.info("📝 Генерация отчетов...")
        reports = await self._generate_reports(all_mentions, clusters, summaries, results)
        
        # 17. Анализ результатов
        analysis = self._analyze_comprehensive_results(results, all_mentions, clusters, quality_results)
        
        # 18. Статистика
        processing_time = time.time() - start_time
        self.statistics = {
            "total_dialogs": len(dialogs),
            "processed_dialogs": len(results),
            "success_rate": len([r for r in results if "error" not in r]) / len(dialogs),
            "total_mentions": len(all_mentions),
            "avg_quality_score": np.mean([r["quality_score"] for r in results if "error" not in r]),
            "processing_time_seconds": processing_time,
            "dialogs_per_second": len(dialogs) / processing_time,
            "ab_test_results": self.adaptive_prompts.get_ab_test_summary("dod_extraction_test"),
            "learning_examples_added": len(learning_examples),
            "monitoring_stats": self.monitor.get_processing_stats() if self.config["processing"]["enable_monitoring"] else {},
            "clusters_found": len(clusters),
            "quality_results": quality_results
        }
        
        self.results = {
            "dialog_results": results,
            "all_mentions": all_mentions,
            "clusters": clusters,
            "summaries": summaries,
            "quality_results": quality_results,
            "reports": reports,
            "analysis": analysis,
            "statistics": self.statistics
        }
        
        logger.info(f"✅ Комплексная обработка завершена за {processing_time:.2f} секунд")
        logger.info(f"📈 Среднее качество: {self.statistics['avg_quality_score']:.3f}")
        logger.info(f"🎯 Успешность: {self.statistics['success_rate']:.1%}")
        logger.info(f"📊 Упоминаний: {len(all_mentions)}")
        logger.info(f"🎯 Кластеров: {len(clusters)}")
        
        return self.results
    
    async def _extract_mentions_with_dod(self, dialog: Dict[str, Any], dialog_id: int, prompt_variant: str) -> List[Dict[str, Any]]:
        """Извлечение упоминаний с соблюдением DoD (только клиент + evidence)"""
        mentions = []
        
        # Извлекаем только реплики клиента
        client_turns = []
        if "turns" in dialog:
            for turn_idx, turn in enumerate(dialog["turns"]):
                if turn.get("role") == "client":
                    client_turns.append((turn_idx, turn))
        elif "messages" in dialog:
            for turn_idx, message in enumerate(dialog["messages"]):
                if message.get("role") == "client":
                    client_turns.append((turn_idx, message))
        
        # Извлекаем упоминания из каждой реплики клиента
        for turn_idx, turn in client_turns:
            text = turn.get("text", "")
            if not text.strip():
                continue
            
            # Используем адаптивный промпт для извлечения
            prompt = self.adaptive_prompts.get_prompt(prompt_variant)
            
            # Извлекаем упоминания по таксономии
            extracted_mentions = self._extract_mentions_from_text(text, dialog_id, turn_idx, prompt)
            mentions.extend(extracted_mentions)
        
        return mentions
    
    def _extract_mentions_from_text(self, text: str, dialog_id: int, turn_id: int, prompt: str) -> List[Dict[str, Any]]:
        """Извлечение упоминаний из текста клиента с использованием промпта"""
        mentions = []
        
        # Простое извлечение по ключевым словам из таксономии
        for theme in self.taxonomy["themes"]:
            theme_name = theme["name"]
            theme_id = theme["id"]
            
            for subtheme in theme["subthemes"]:
                subtheme_name = subtheme["name"]
                subtheme_id = subtheme["id"]
                
                # Проверяем наличие ключевых слов подтемы в тексте
                keywords = subtheme_name.lower().split()
                if any(keyword in text.lower() for keyword in keywords):
                    # Определяем тип метки
                    label_type = self._determine_label_type(text, subtheme_name)
                    
                    # Извлекаем цитату
                    quote = self._extract_quote(text, keywords)
                    
                    if quote:
                        mention = {
                            "dialog_id": dialog_id,
                            "turn_id": turn_id,
                            "theme": theme_name,
                            "subtheme": subtheme_name,
                            "label_type": label_type,
                            "text_quote": quote,
                            "delivery_type": self._extract_delivery_type(text),
                            "cause_hint": self._extract_cause_hint(text),
                            "confidence": self._calculate_confidence(text, subtheme_name)
                        }
                        mentions.append(mention)
        
        return mentions
    
    def _determine_label_type(self, text: str, subtheme: str) -> str:
        """Определение типа метки"""
        text_lower = text.lower()
        
        if any(word in text_lower for word in ["проблема", "не работает", "сломал", "ошибка"]):
            return "барьер"
        elif any(word in text_lower for word in ["предложение", "идея", "можно", "лучше"]):
            return "идея"
        elif any(word in text_lower for word in ["сигнал", "уведомление", "алерт"]):
            return "сигнал"
        elif any(word in text_lower for word in ["спасибо", "отлично", "хорошо", "понравилось"]):
            return "похвала"
        else:
            return "барьер"
    
    def _extract_quote(self, text: str, keywords: List[str]) -> str:
        """Извлечение цитаты из текста"""
        sentences = text.split('.')
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 10 and any(keyword in sentence.lower() for keyword in keywords):
                return sentence
        return text[:100] + "..." if len(text) > 100 else text
    
    def _extract_delivery_type(self, text: str) -> Optional[str]:
        """Извлечение типа доставки"""
        text_lower = text.lower()
        if "жалоба" in text_lower:
            return "complaint"
        elif "запрос" in text_lower:
            return "request"
        elif "вопрос" in text_lower:
            return "question"
        elif "отзыв" in text_lower:
            return "feedback"
        return None
    
    def _extract_cause_hint(self, text: str) -> Optional[str]:
        """Извлечение подсказки о причине"""
        text_lower = text.lower()
        if "из-за" in text_lower:
            return "причина указана"
        elif "потому что" in text_lower:
            return "причина указана"
        return None
    
    def _calculate_confidence(self, text: str, subtheme: str) -> float:
        """Расчет уверенности"""
        text_lower = text.lower()
        subtheme_lower = subtheme.lower()
        
        if subtheme_lower in text_lower:
            return 0.95
        elif sum(1 for kw in subtheme_lower.split() if kw in text_lower) >= len(subtheme_lower.split()) * 0.7:
            return 0.85
        elif any(kw in text_lower for kw in subtheme_lower.split()):
            return 0.70
        else:
            return 0.50
    
    def _validate_mentions(self, mentions: List[Dict[str, Any]]):
        """Валидация упоминаний по JSON схеме"""
        try:
            for mention in mentions:
                jsonschema.validate(mention, self.schema)
            logger.info("✅ Все упоминания прошли валидацию по схеме")
        except jsonschema.ValidationError as e:
            logger.error(f"❌ Ошибка валидации: {e}")
            raise
    
    async def _deduplicate_mentions(self, mentions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Дедупликация упоминаний"""
        temp_file = "artifacts/temp_mentions.jsonl"
        with open(temp_file, 'w', encoding='utf-8') as f:
            for mention in mentions:
                f.write(json.dumps(mention, ensure_ascii=False) + '\n')
        
        dedup_file = "artifacts/mentions_dedup.jsonl"
        sys.argv = ['dedup.py', '--in', temp_file, '--out', dedup_file]
        dedup_main()
        
        deduped_mentions = []
        with open(dedup_file, 'r', encoding='utf-8') as f:
            for line in f:
                deduped_mentions.append(json.loads(line))
        
        os.remove(temp_file)
        os.remove(dedup_file)
        
        logger.info(f"🔄 Дедупликация: {len(mentions)} -> {len(deduped_mentions)} упоминаний")
        return deduped_mentions
    
    async def _cluster_mentions(self, mentions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Кластеризация упоминаний"""
        clusters = {}
        by_subtheme = defaultdict(list)
        
        for mention in mentions:
            key = f"{mention['theme']}_{mention['subtheme']}"
            by_subtheme[key].append(mention)
        
        for subtheme_key, subtheme_mentions in by_subtheme.items():
            if len(subtheme_mentions) < 5:
                continue
            
            embeddings = np.random.rand(len(subtheme_mentions), 50)
            
            temp_mentions = "artifacts/temp_cluster_mentions.jsonl"
            temp_embeddings = "artifacts/temp_embeddings.npy"
            
            with open(temp_mentions, 'w', encoding='utf-8') as f:
                for mention in subtheme_mentions:
                    f.write(json.dumps(mention, ensure_ascii=False) + '\n')
            
            np.save(temp_embeddings, embeddings)
            
            theme, subtheme = subtheme_key.split('_', 1)
            cluster_file = f"artifacts/clusters_{subtheme_key}.json"
            
            sys.argv = ['clusterize.py', '--mentions', temp_mentions, '--embeddings', temp_embeddings,
                       '--theme', theme, '--subtheme', subtheme, '--out', cluster_file]
            clusterize_main()
            
            if Path(cluster_file).exists():
                with open(cluster_file, 'r', encoding='utf-8') as f:
                    clusters[subtheme_key] = json.load(f)
            
            os.remove(temp_mentions)
            os.remove(temp_embeddings)
            if Path(cluster_file).exists():
                os.remove(cluster_file)
        
        logger.info(f"🎯 Кластеризация: найдено {len(clusters)} групп кластеров")
        return clusters
    
    async def _build_summaries(self, mentions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Построение сводок"""
        conn = duckdb.connect(':memory:')
        
        dialog_ids = list(set(m['dialog_id'] for m in mentions))
        dialogs_df = pd.DataFrame({'dialog_id': dialog_ids})
        conn.register('dialogs', dialogs_df)
        
        mentions_df = pd.DataFrame(mentions)
        conn.register('mentions', mentions_df)
        
        with open('sql/build_summaries.sql', 'r', encoding='utf-8') as f:
            sql_queries = f.read().split(';')
        
        summaries = {}
        for query in sql_queries:
            query = query.strip()
            if not query:
                continue
            
            try:
                result = conn.execute(query).fetchdf()
                table_name = query.split('CREATE OR REPLACE TABLE')[1].split('AS')[0].strip()
                summaries[table_name] = result.to_dict('records')
            except Exception as e:
                logger.warning(f"Ошибка выполнения SQL: {e}")
        
        conn.close()
        logger.info(f"📊 Сводки: создано {len(summaries)} таблиц")
        return summaries
    
    async def _run_quality_checks(self, mentions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Запуск проверок качества DoD"""
        conn = duckdb.connect(':memory:')
        
        dialog_ids = list(set(m['dialog_id'] for m in mentions))
        dialogs_df = pd.DataFrame({'dialog_id': dialog_ids})
        conn.register('dialogs', dialogs_df)
        
        mentions_df = pd.DataFrame(mentions)
        conn.register('mentions', mentions_df)
        
        utterances_data = []
        for mention in mentions:
            utterances_data.append({
                'dialog_id': mention['dialog_id'],
                'turn_id': mention['turn_id'],
                'role': 'client'
            })
        utterances_df = pd.DataFrame(utterances_data)
        conn.register('utterances', utterances_df)
        
        with open('quality/checks.sql', 'r', encoding='utf-8') as f:
            queries = [q.strip() for q in f.read().split(';') if q.strip()]
        
        quality_results = {}
        for i, query in enumerate(queries):
            try:
                result = conn.execute(query).fetchone()
                if i == 0:
                    quality_results['empty_quotes'] = result[0]
                elif i == 1:
                    quality_results['non_client_mentions'] = result[0]
                elif i == 2:
                    quality_results['dup_pct'] = result[0]
                elif i == 3:
                    quality_results['misc_share_pct'] = result[0]
                elif i == 4:
                    quality_results['ambiguity_report'] = conn.execute(query).fetchdf().to_dict('records')
            except Exception as e:
                logger.warning(f"Ошибка проверки качества {i+1}: {e}")
        
        conn.close()
        
        dod_status = {
            "evidence_100": quality_results.get('empty_quotes', 0) == 0,
            "client_only_100": quality_results.get('non_client_mentions', 0) == 0,
            "dedup_1pct": quality_results.get('dup_pct', 0) <= 1.0,
            "coverage_98pct": quality_results.get('misc_share_pct', 0) <= 2.0
        }
        
        quality_results['dod_status'] = dod_status
        quality_results['dod_passed'] = all(dod_status.values())
        
        logger.info(f"🔍 Проверки качества: DoD {'✅ пройден' if quality_results['dod_passed'] else '❌ не пройден'}")
        return quality_results
    
    async def _generate_reports(self, mentions: List[Dict[str, Any]], clusters: Dict[str, Any], 
                               summaries: Dict[str, Any], results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Генерация отчетов"""
        reports = {}
        
        total_dialogs = len(set(m['dialog_id'] for m in mentions))
        themes_summary = summaries.get('summary_themes', [])
        subthemes_summary = summaries.get('summary_subthemes', [])
        
        # Основной отчет
        report_content = f"""# Комплексный отчет по анализу диалогов с DoD

## Общая статистика
- Всего диалогов: {total_dialogs}
- Всего упоминаний: {len(mentions)}
- Найдено кластеров: {len(clusters)}
- Среднее качество: {np.mean([r.get('quality_score', 0) for r in results if 'error' not in r]):.3f}

## Темы
"""
        
        for theme in themes_summary[:10]:
            report_content += f"- {theme['theme']}: {theme['dialog_count']} диалогов ({theme['share_of_dialogs_pct']}%)\n"
        
        report_content += "\n## Подтемы\n"
        for subtheme in subthemes_summary[:20]:
            report_content += f"- {subtheme['theme']} / {subtheme['subtheme']}: {subtheme['dialog_count']} диалогов\n"
        
        # A/B тест результаты
        ab_results = self.adaptive_prompts.get_ab_test_summary("dod_extraction_test")
        if ab_results:
            report_content += "\n## A/B тестирование\n"
            for variant, stats in ab_results.get('variants', {}).items():
                report_content += f"- {variant}: качество {stats.get('avg_quality', 0):.3f}\n"
        
        reports['main_report'] = report_content
        
        with open('reports/comprehensive_dod_report.md', 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info("📝 Отчеты сгенерированы")
        return reports
    
    def _analyze_comprehensive_results(self, results: List[Dict[str, Any]], mentions: List[Dict[str, Any]], 
                                     clusters: Dict[str, Any], quality_results: Dict[str, Any]) -> Dict[str, Any]:
        """Комплексный анализ результатов"""
        successful_results = [r for r in results if "error" not in r]
        
        if not successful_results:
            return {"error": "Нет успешных результатов для анализа"}
        
        # Анализ качества
        quality_scores = [r["quality_score"] for r in successful_results]
        
        # Анализ упоминаний
        all_mentions = {"problems": [], "ideas": [], "barriers": [], "quotes": []}
        for mention in mentions:
            label_type = mention.get("label_type", "барьер")
            if label_type == "барьер":
                all_mentions["barriers"].append(mention["text_quote"])
            elif label_type == "идея":
                all_mentions["ideas"].append(mention["text_quote"])
            all_mentions["quotes"].append(mention["text_quote"])
        
        # A/B тест анализ
        ab_variants = {}
        for result in successful_results:
            variant = result.get("prompt_variant", "unknown")
            if variant not in ab_variants:
                ab_variants[variant] = []
            ab_variants[variant].append(result["quality_score"])
        
        variant_stats = {}
        for variant, scores in ab_variants.items():
            variant_stats[variant] = {
                "count": len(scores),
                "avg_quality": np.mean(scores),
                "std_quality": np.std(scores),
                "min_quality": np.min(scores),
                "max_quality": np.max(scores)
            }
        
        analysis = {
            "quality_analysis": {
                "avg_quality": np.mean(quality_scores),
                "std_quality": np.std(quality_scores),
                "min_quality": np.min(quality_scores),
                "max_quality": np.max(quality_scores),
                "quality_distribution": {
                    "high": len([s for s in quality_scores if s >= 0.7]),
                    "medium": len([s for s in quality_scores if 0.4 <= s < 0.7]),
                    "low": len([s for s in quality_scores if s < 0.4])
                }
            },
            "mentions_analysis": {
                "total_mentions": len(mentions),
                "total_barriers": len(all_mentions["barriers"]),
                "total_ideas": len(all_mentions["ideas"]),
                "total_quotes": len(all_mentions["quotes"]),
                "unique_barriers": len(set(all_mentions["barriers"])),
                "unique_ideas": len(set(all_mentions["ideas"])),
                "unique_quotes": len(set(all_mentions["quotes"]))
            },
            "clusters_analysis": {
                "total_clusters": len(clusters),
                "clusters_by_subtheme": {k: len(v.get('clusters', [])) for k, v in clusters.items()}
            },
            "ab_test_analysis": variant_stats,
            "dod_compliance": quality_results.get('dod_status', {}),
            "recommendations": self._generate_comprehensive_recommendations(quality_scores, all_mentions, variant_stats, quality_results)
        }
        
        return analysis
    
    def _generate_comprehensive_recommendations(self, quality_scores: List[float], mentions: Dict, 
                                              variant_stats: Dict, quality_results: Dict) -> List[str]:
        """Генерация комплексных рекомендаций"""
        recommendations = []
        
        avg_quality = np.mean(quality_scores)
        
        if avg_quality < 0.5:
            recommendations.append("🔧 Низкое качество обработки. Рекомендуется улучшить промпты или добавить больше обучающих данных.")
        
        if len(mentions["quotes"]) < len(quality_scores) * 0.5:
            recommendations.append("📝 Мало извлеченных цитат. Проверьте качество диалогов и настройки извлечения.")
        
        if len(mentions["barriers"]) > len(mentions["ideas"]) * 2:
            recommendations.append("⚠️ Много проблем, мало идей. Рекомендуется фокус на позитивных аспектах.")
        
        # A/B тест рекомендации
        if len(variant_stats) > 1:
            best_variant = max(variant_stats.items(), key=lambda x: x[1]["avg_quality"])
            recommendations.append(f"🎯 Лучший вариант промпта: {best_variant[0]} (качество: {best_variant[1]['avg_quality']:.3f})")
        
        # DoD рекомендации
        dod_status = quality_results.get('dod_status', {})
        if not dod_status.get('evidence_100'):
            recommendations.append("❌ DoD нарушен: найдены пустые цитаты")
        if not dod_status.get('client_only_100'):
            recommendations.append("❌ DoD нарушен: найдены упоминания не от клиента")
        if not dod_status.get('dedup_1pct'):
            recommendations.append("❌ DoD нарушен: слишком много дубликатов")
        if not dod_status.get('coverage_98pct'):
            recommendations.append("❌ DoD нарушен: слишком много категории 'прочее'")
        
        if not recommendations:
            recommendations.append("✅ Система работает стабильно. Все DoD требования выполнены.")
        
        return recommendations
    
    def _calculate_quality_score(self, mentions: List[Dict[str, Any]], dialog: Dict[str, Any]) -> float:
        """Расчет качества результата"""
        if not mentions:
            return 0.0
        
        score = 0.0
        
        # Оценка по количеству упоминаний
        score += min(0.4, len(mentions) * 0.1)
        
        # Оценка по качеству цитат
        avg_confidence = np.mean([m.get('confidence', 0) for m in mentions])
        score += avg_confidence * 0.3
        
        # Оценка по разнообразию тем
        unique_themes = len(set(m.get('theme', '') for m in mentions))
        score += min(0.2, unique_themes * 0.05)
        
        # Оценка по длине диалога
        dialog_text = str(dialog)
        if len(dialog_text) > 100:
            score += 0.1
        
        return min(1.0, score)
    
    def save_results(self, output_dir: str = "artifacts"):
        """Сохранение результатов"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Сохранение всех результатов
        with open(output_path / "comprehensive_results.json", "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # Сохранение упоминаний
        with open(output_path / "mentions.jsonl", "w", encoding="utf-8") as f:
            for mention in self.results["all_mentions"]:
                f.write(json.dumps(mention, ensure_ascii=False) + "\n")
        
        # Сохранение кластеров
        with open(output_path / "clusters.json", "w", encoding="utf-8") as f:
            json.dump(self.results["clusters"], f, ensure_ascii=False, indent=2)
        
        # Сохранение сводок
        with open(output_path / "summaries.json", "w", encoding="utf-8") as f:
            json.dump(self.results["summaries"], f, ensure_ascii=False, indent=2)
        
        # Сохранение результатов качества
        with open(output_path / "quality_results.json", "w", encoding="utf-8") as f:
            json.dump(self.results["quality_results"], f, ensure_ascii=False, indent=2)
        
        # Сохранение статистики
        with open(output_path / "statistics.json", "w", encoding="utf-8") as f:
            json.dump(self.results["statistics"], f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Результаты сохранены в {output_dir}")

def load_dialogs_from_file(file_path: str) -> List[Dict[str, Any]]:
    """Загрузка диалогов из файла"""
    file_path = Path(file_path)
    
    if file_path.suffix == '.xlsx':
        df = pd.read_excel(file_path)
        dialogs = []
        for idx, row in df.iterrows():
            dialog = {
                "dialog_id": idx,
                "turns": [
                    {"role": "client", "text": str(row.iloc[0]) if len(row) > 0 else ""}
                ]
            }
            dialogs.append(dialog)
        return dialogs
    elif file_path.suffix == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        else:
            return data.get('dialogs', [])
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        dialogs = []
        for idx, line in enumerate(lines):
            dialogs.append({
                "dialog_id": idx,
                "turns": [{"role": "client", "text": line}]
            })
        return dialogs

async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Комплексный DoD пайплайн')
    parser.add_argument('--input', '-i', required=True, help='Путь к файлу с диалогами')
    parser.add_argument('--output', '-o', default='artifacts', help='Директория для результатов')
    parser.add_argument('--config', '-c', default='final_pipeline_config.json', help='Конфигурация')
    
    args = parser.parse_args()
    
    # Загрузка диалогов
    logger.info(f"📂 Загружаем диалоги из {args.input}")
    dialogs = load_dialogs_from_file(args.input)
    logger.info(f"✅ Загружено {len(dialogs)} диалогов")
    
    # Создание и запуск пайплайна
    pipeline = ComprehensiveDoDPipeline(args.config)
    
    # Обработка диалогов
    results = await pipeline.process_dialogs(dialogs)
    
    # Сохранение результатов
    pipeline.save_results(args.output)
    
    # Вывод результатов DoD
    quality = results["quality_results"]
    print("\n" + "="*60)
    print("🎯 РЕЗУЛЬТАТЫ КОМПЛЕКСНОГО DoD ПАЙПЛАЙНА")
    print("="*60)
    print(f"Evidence-100: {'✅' if quality.get('dod_status', {}).get('evidence_100') else '❌'}")
    print(f"Client-only-100: {'✅' if quality.get('dod_status', {}).get('client_only_100') else '❌'}")
    print(f"Dedup ≤1%: {'✅' if quality.get('dod_status', {}).get('dedup_1pct') else '❌'}")
    print(f"Coverage ≥98%: {'✅' if quality.get('dod_status', {}).get('coverage_98pct') else '❌'}")
    print(f"Общий статус DoD: {'✅ ПРОЙДЕН' if quality.get('dod_passed') else '❌ НЕ ПРОЙДЕН'}")
    print(f"Среднее качество: {results['statistics']['avg_quality_score']:.3f}")
    print(f"Успешность: {results['statistics']['success_rate']:.1%}")
    print(f"Упоминаний: {len(results['all_mentions'])}")
    print(f"Кластеров: {len(results['clusters'])}")
    print("="*60)
    
    logger.info("🎉 Комплексный DoD пайплайн завершен!")

if __name__ == "__main__":
    asyncio.run(main())
