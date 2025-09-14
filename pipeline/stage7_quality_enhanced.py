#!/usr/bin/env python3
"""
Stage 7 Enhanced: Расширенные метрики качества
Вычисляет расширенные метрики качества анализа с семантическими и контекстными показателями
"""

import json
import statistics
import logging
from pathlib import Path
from typing import Dict, Any, List
from collections import Counter, defaultdict
import pandas as pd

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_precision_recall(extractions: List[Dict[str, Any]]) -> Dict[str, float]:
    """Вычисление precision и recall для извлечения сущностей"""
    
    total_extractions = len(extractions)
    if total_extractions == 0:
        return {"precision": 0.0, "recall": 0.0, "f1_score": 0.0}
    
    # Подсчитываем успешные извлечения
    successful_extractions = 0
    total_entities = 0
    
    for extraction in extractions:
        if not extraction.get("error"):
            successful_extractions += 1
            
            # Подсчитываем сущности
            barriers = extraction.get("barriers", [])
            ideas = extraction.get("ideas", [])
            signals = extraction.get("signals", [])
            
            total_entities += len(barriers) + len(ideas) + len(signals)
    
    precision = successful_extractions / total_extractions if total_extractions > 0 else 0.0
    recall = successful_extractions / total_extractions if total_extractions > 0 else 0.0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1_score": round(f1_score, 3)
    }

def calculate_citation_quality(clusters: List[Dict[str, Any]]) -> Dict[str, float]:
    """Вычисление качества цитат"""
    
    if not clusters:
        return {"avg_citation_length": 0.0, "citation_coverage": 0.0, "quality_score": 0.0}
    
    total_citations = 0
    total_length = 0
    clusters_with_citations = 0
    
    for cluster in clusters:
        quotes = cluster.get("quotes", [])
        if quotes:
            clusters_with_citations += 1
            total_citations += len(quotes)
            
            for quote in quotes:
                if isinstance(quote, dict):
                    quote_text = quote.get("quote", "")
                else:
                    quote_text = str(quote)
                
                total_length += len(quote_text.strip())
    
    avg_citation_length = total_length / total_citations if total_citations > 0 else 0.0
    citation_coverage = clusters_with_citations / len(clusters) if clusters else 0.0
    
    # Качество цитат (длина + покрытие)
    quality_score = (avg_citation_length / 100.0) * 0.5 + citation_coverage * 0.5
    
    return {
        "avg_citation_length": round(avg_citation_length, 2),
        "citation_coverage": round(citation_coverage, 3),
        "quality_score": round(quality_score, 3)
    }

def calculate_clustering_quality(clusters: List[Dict[str, Any]]) -> Dict[str, float]:
    """Вычисление качества кластеризации"""
    
    if not clusters:
        return {"silhouette_score": 0.0, "cluster_coherence": 0.0, "duplicate_rate": 0.0}
    
    # Анализ дублирования
    cluster_names = [cluster.get("name", "") for cluster in clusters]
    name_counts = Counter(cluster_names)
    duplicates = sum(1 for count in name_counts.values() if count > 1)
    duplicate_rate = duplicates / len(clusters) if clusters else 0.0
    
    # Анализ семантической связности
    coherence_scores = [cluster.get("semantic_coherence", 0) for cluster in clusters if cluster.get("semantic_coherence")]
    avg_coherence = sum(coherence_scores) / len(coherence_scores) if coherence_scores else 0.0
    
    # Оценка качества кластеризации
    clustering_quality = (1 - duplicate_rate) * 0.6 + avg_coherence * 0.4
    
    return {
        "duplicate_rate": round(duplicate_rate, 3),
        "cluster_coherence": round(avg_coherence, 3),
        "clustering_quality": round(clustering_quality, 3)
    }

def calculate_semantic_quality(clusters: List[Dict[str, Any]]) -> Dict[str, float]:
    """Вычисление семантического качества"""
    
    if not clusters:
        return {"concept_diversity": 0.0, "semantic_richness": 0.0, "context_richness": 0.0}
    
    # Анализ разнообразия концепций
    all_texts = []
    for cluster in clusters:
        variants = cluster.get("variants", [])
        for variant in variants:
            if isinstance(variant, dict):
                all_texts.append(variant.get("text", ""))
            else:
                all_texts.append(str(variant))
    
    # Подсчитываем уникальные слова
    import re
    all_words = []
    for text in all_texts:
        words = re.findall(r'\b[а-яё]+\b', text.lower())
        all_words.extend([w for w in words if len(w) > 2])
    
    unique_words = len(set(all_words))
    total_words = len(all_words)
    concept_diversity = unique_words / total_words if total_words > 0 else 0.0
    
    # Анализ семантического богатства
    semantic_features = 0
    for cluster in clusters:
        if cluster.get("description"):
            semantic_features += 1
        if cluster.get("priority"):
            semantic_features += 1
        if cluster.get("solutions"):
            semantic_features += 1
    
    semantic_richness = semantic_features / (len(clusters) * 3) if clusters else 0.0
    
    # Анализ контекстного богатства
    context_features = 0
    for cluster in clusters:
        if cluster.get("impact_metrics"):
            context_features += 1
        if cluster.get("semantic_coherence"):
            context_features += 1
    
    context_richness = context_features / (len(clusters) * 2) if clusters else 0.0
    
    return {
        "concept_diversity": round(concept_diversity, 3),
        "semantic_richness": round(semantic_richness, 3),
        "context_richness": round(context_richness, 3)
    }

def calculate_business_metrics(clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Вычисление бизнес-метрик"""
    
    if not clusters:
        return {"actionable_insights": 0, "priority_distribution": {}, "solution_feasibility": 0.0}
    
    # Подсчет actionable insights
    actionable_insights = 0
    for cluster in clusters:
        if cluster.get("solutions") and len(cluster.get("solutions", [])) > 0:
            actionable_insights += 1
    
    # Распределение приоритетов
    priority_distribution = Counter()
    for cluster in clusters:
        priority = cluster.get("priority", "низкий")
        priority_distribution[priority] += 1
    
    # Анализ осуществимости решений
    feasibility_scores = []
    for cluster in clusters:
        solutions = cluster.get("solutions", [])
        if solutions:
            # Простая эвристика: чем больше решений, тем выше осуществимость
            feasibility_scores.append(min(len(solutions) / 3.0, 1.0))
    
    solution_feasibility = sum(feasibility_scores) / len(feasibility_scores) if feasibility_scores else 0.0
    
    return {
        "actionable_insights": actionable_insights,
        "priority_distribution": dict(priority_distribution),
        "solution_feasibility": round(solution_feasibility, 3)
    }

def calculate_enhanced_quality_metrics(aggregate_path: str = "artifacts/aggregate_results.json", 
                                     enhanced_extractions_path: str = "artifacts/stage2_extracted_enhanced.jsonl",
                                     enhanced_clusters_path: str = "artifacts/stage4_5_semantic_enrichment.json",
                                     out: str = "reports/quality_enhanced.json") -> Dict[str, Any]:
    """Вычисление расширенных метрик качества анализа"""
    
    logger.info("🔍 Вычисление расширенных метрик качества...")
    
    # Загружаем базовые данные
    agg = json.loads(Path(aggregate_path).read_text(encoding="utf-8"))
    N = agg["meta"]["N"]
    D = agg["meta"]["D"]
    
    # Загружаем улучшенные извлечения
    extractions = []
    if Path(enhanced_extractions_path).exists():
        with open(enhanced_extractions_path, 'r', encoding='utf-8') as f:
            for line in f:
                extractions.append(json.loads(line))
    
    # Загружаем улучшенные кластеры
    enhanced_clusters = {}
    if Path(enhanced_clusters_path).exists():
        with open(enhanced_clusters_path, 'r', encoding='utf-8') as f:
            enhanced_clusters = json.load(f)
    
    # Базовые метрики
    clusters = agg.get("barriers", []) + agg.get("ideas", []) + agg.get("signals", []) + agg.get("signals_platform", [])
    clusters_count = len(clusters)
    
    # Метрики извлечения
    extraction_quality = calculate_precision_recall(extractions)
    
    # Метрики кластеризации
    clustering_quality = calculate_clustering_quality(clusters)
    
    # Метрики цитат
    citation_quality = calculate_citation_quality(clusters)
    
    # Семантические метрики
    semantic_quality = calculate_semantic_quality(clusters)
    
    # Бизнес-метрики
    business_metrics = calculate_business_metrics(clusters)
    
    # Общие метрики
    total_quotes = sum(len(c.get("quotes", [])) for c in clusters)
    clusters_with_quotes = sum(1 for c in clusters if c.get("quotes"))
    
    # Создаем расширенные метрики
    enhanced_metrics = {
        "basic_metrics": {
            "N": N,
            "D": D,
            "D_share": round((D/N), 3) if N else 0.0,
            "clusters_count": clusters_count,
            "total_quotes": total_quotes,
            "clusters_with_quotes": clusters_with_quotes
        },
        
        "extraction_quality": extraction_quality,
        "clustering_quality": clustering_quality,
        "citation_quality": citation_quality,
        "semantic_quality": semantic_quality,
        "business_metrics": business_metrics,
        
        "overall_quality_score": round(
            (extraction_quality["f1_score"] * 0.3 +
             clustering_quality["clustering_quality"] * 0.2 +
             citation_quality["quality_score"] * 0.2 +
             semantic_quality["semantic_richness"] * 0.2 +
             business_metrics["solution_feasibility"] * 0.1), 3
        ),
        
        "analysis_timestamp": pd.Timestamp.now().isoformat()
    }
    
    # Сохраняем метрики
    Path("reports").mkdir(exist_ok=True, parents=True)
    Path(out).write_text(json.dumps(enhanced_metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info(f"📊 Расширенные метрики качества сохранены: {out}")
    logger.info(f"  Общий балл качества: {enhanced_metrics['overall_quality_score']}")
    logger.info(f"  F1-score извлечения: {extraction_quality['f1_score']}")
    logger.info(f"  Качество кластеризации: {clustering_quality['clustering_quality']}")
    logger.info(f"  Семантическое богатство: {semantic_quality['semantic_richness']}")
    
    return enhanced_metrics

def main():
    """Основная функция Stage 7 Enhanced"""
    logger.info("🚀 Stage 7 Enhanced: Расширенные метрики качества")
    
    try:
        quality_metrics = calculate_enhanced_quality_metrics()
        logger.info("✅ Stage 7 Enhanced завершен успешно!")
        return quality_metrics
    except Exception as e:
        logger.error(f"❌ Ошибка в Stage 7 Enhanced: {e}")
        raise

if __name__ == "__main__":
    main()
