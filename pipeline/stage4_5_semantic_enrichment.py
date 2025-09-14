#!/usr/bin/env python3
"""
Stage 4.5: Семантическое обогащение
Обогащение кластеров дополнительной семантической информацией
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import re
from collections import Counter

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_cluster_description(cluster: Dict[str, Any]) -> str:
    """Генерация описания кластера"""
    
    variants = cluster.get("variants", [])
    if not variants:
        return "Пустой кластер"
    
    # Анализируем варианты
    texts = [v["text"] for v in variants]
    
    # Извлекаем ключевые слова
    words = []
    for text in texts:
        text_words = re.findall(r'\b[а-яё]+\b', text.lower())
        words.extend([w for w in text_words if len(w) > 2])
    
    word_counts = Counter(words)
    top_words = [word for word, count in word_counts.most_common(5)]
    
    if top_words:
        return f"Кластер связан с: {', '.join(top_words)}"
    else:
        return "Описание недоступно"

def calculate_cluster_priority(cluster: Dict[str, Any]) -> str:
    """Вычисление приоритета кластера"""
    
    variants = cluster.get("variants", [])
    if not variants:
        return "низкий"
    
    # Ключевые слова для определения приоритета
    high_priority_keywords = [
        "критично", "срочно", "блокирует", "не работает", "ошибка",
        "не понимаю", "как сделать", "помогите", "высокая"
    ]
    
    medium_priority_keywords = [
        "проблема", "сложно", "неудобно", "долго", "дорого", "средняя"
    ]
    
    texts = [v["text"].lower() for v in variants]
    
    for keyword in high_priority_keywords:
        if any(keyword in text for text in texts):
            return "высокий"
    
    for keyword in medium_priority_keywords:
        if any(keyword in text for text in texts):
            return "средний"
    
    return "низкий"

def generate_solution_suggestions(cluster: Dict[str, Any]) -> List[str]:
    """Генерация предложений по решению"""
    
    cluster_name = cluster.get("name", "").lower()
    suggestions = []
    
    # Предложения на основе названия кластера
    if "непонимание" in cluster_name or "процесс" in cluster_name:
        suggestions.extend([
            "Создать пошаговую инструкцию",
            "Добавить интерактивные подсказки",
            "Улучшить UX интерфейса"
        ])
    
    if "настройки" in cluster_name or "сбои" in cluster_name:
        suggestions.extend([
            "Упростить процесс настройки",
            "Добавить автоматическую диагностику",
            "Улучшить техническую поддержку"
        ])
    
    if "стоимость" in cluster_name or "дорого" in cluster_name:
        suggestions.extend([
            "Предоставить калькулятор стоимости",
            "Добавить варианты экономичной доставки",
            "Внедрить динамическое ценообразование"
        ])
    
    if "пвз" in cluster_name or "пункты" in cluster_name:
        suggestions.extend([
            "Расширить сеть пунктов выдачи",
            "Улучшить карту с ПВЗ",
            "Добавить фильтры по удобству"
        ])
    
    # Если нет специфических предложений, добавляем общие
    if not suggestions:
        suggestions = [
            "Провести дополнительное исследование",
            "Собрать обратную связь от пользователей",
            "Рассмотреть возможность улучшения процесса"
        ]
    
    return suggestions[:3]  # Максимум 3 предложения

def calculate_impact_metrics(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """Вычисление метрик влияния кластера"""
    
    variants = cluster.get("variants", [])
    total_mentions = sum(v.get("count", 1) for v in variants)
    
    # Базовые метрики
    metrics = {
        "total_mentions": total_mentions,
        "unique_variants": len(variants),
        "avg_mentions_per_variant": total_mentions / len(variants) if variants else 0
    }
    
    # Анализ семантической связности
    if cluster.get("semantic_coherence"):
        metrics["semantic_coherence"] = cluster["semantic_coherence"]
    
    # Анализ приоритета
    priority = cluster.get("priority", "низкий")
    priority_scores = {"низкий": 1, "средний": 2, "высокий": 3}
    metrics["priority_score"] = priority_scores.get(priority, 1)
    
    # Общий балл влияния
    impact_score = (
        metrics["total_mentions"] * 0.4 +
        metrics["priority_score"] * 0.3 +
        metrics.get("semantic_coherence", 0.5) * 0.3
    )
    metrics["impact_score"] = impact_score
    
    return metrics

def analyze_cluster_trends(clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализ трендов в кластерах"""
    
    # Анализ распределения приоритетов
    priority_distribution = Counter()
    for cluster in clusters:
        priority = cluster.get("priority", "низкий")
        priority_distribution[priority] += 1
    
    # Анализ семантической связности
    coherence_scores = [cluster.get("semantic_coherence", 0) for cluster in clusters if cluster.get("semantic_coherence")]
    avg_coherence = sum(coherence_scores) / len(coherence_scores) if coherence_scores else 0
    
    # Анализ метрик влияния
    impact_scores = [cluster.get("impact_metrics", {}).get("impact_score", 0) for cluster in clusters]
    avg_impact = sum(impact_scores) / len(impact_scores) if impact_scores else 0
    
    return {
        "priority_distribution": dict(priority_distribution),
        "avg_semantic_coherence": avg_coherence,
        "avg_impact_score": avg_impact,
        "high_impact_clusters": len([c for c in clusters if c.get("impact_metrics", {}).get("impact_score", 0) > 2.0])
    }

def semantic_enrichment(clusters: Dict[str, Any]) -> Dict[str, Any]:
    """Обогащение кластеров семантической информацией"""
    
    logger.info("🔍 Запуск семантического обогащения...")
    
    enriched_clusters = {}
    
    for cluster_type, cluster_list in clusters.items():
        if cluster_type in ["barriers", "ideas", "signals"]:
            enriched_list = []
            
            for cluster in cluster_list:
                # Обогащаем кластер
                enriched_cluster = cluster.copy()
                
                # Добавляем описание
                enriched_cluster["description"] = generate_cluster_description(cluster)
                
                # Добавляем приоритет
                enriched_cluster["priority"] = calculate_cluster_priority(cluster)
                
                # Добавляем предложения по решению
                enriched_cluster["solutions"] = generate_solution_suggestions(cluster)
                
                # Добавляем метрики влияния
                enriched_cluster["impact_metrics"] = calculate_impact_metrics(cluster)
                
                # Добавляем временные метки
                enriched_cluster["enriched_at"] = pd.Timestamp.now().isoformat()
                
                enriched_list.append(enriched_cluster)
            
            enriched_clusters[cluster_type] = enriched_list
    
    # Добавляем анализ трендов
    all_clusters = []
    for cluster_list in enriched_clusters.values():
        if isinstance(cluster_list, list):
            all_clusters.extend(cluster_list)
    
    enriched_clusters["trend_analysis"] = analyze_cluster_trends(all_clusters)
    
    # Добавляем общие метрики
    enriched_clusters["enrichment_metrics"] = {
        "total_clusters": sum(len(clusters) for clusters in enriched_clusters.values() if isinstance(clusters, list)),
        "enrichment_timestamp": pd.Timestamp.now().isoformat()
    }
    
    logger.info("✅ Семантическое обогащение завершено")
    
    return enriched_clusters

def main():
    """Основная функция"""
    logger.info("🚀 Stage 4.5: Семантическое обогащение")
    
    # Загружаем данные
    input_file = "artifacts/stage4_clusters_enhanced.json"
    if not Path(input_file).exists():
        logger.error(f"Файл {input_file} не найден. Запустите Stage 4 Enhanced сначала.")
        return
    
    with open(input_file, 'r', encoding='utf-8') as f:
        clusters = json.load(f)
    
    logger.info(f"Загружено кластеров: {len(clusters.get('barriers', [])) + len(clusters.get('ideas', [])) + len(clusters.get('signals', []))}")
    
    # Семантическое обогащение
    enriched_clusters = semantic_enrichment(clusters)
    
    # Сохраняем результаты
    output_file = "artifacts/stage4_5_semantic_enrichment.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_clusters, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Результаты сохранены в {output_file}")

if __name__ == "__main__":
    main()
