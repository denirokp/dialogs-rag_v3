#!/usr/bin/env python3
"""
Stage 4 Enhanced: Улучшенная кластеризация с семантическим анализом
Группирует похожие барьеры и идеи с использованием embeddings
"""

import json
import logging
import re
import pandas as pd
from pathlib import Path
from collections import Counter, defaultdict
from typing import List, Dict, Any, Set, Tuple
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import silhouette_score

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from models.validation import DialogExtraction, Cluster, ClusterVariant, ClusterSlices, ClustersData
from prompts import STAGE_CONFIG, CLUSTERING_STOPWORDS

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def semantic_cluster(texts: List[str], threshold: float = 0.7, min_samples: int = 2) -> Dict[int, List[str]]:
    """Семантическая кластеризация с использованием TF-IDF и DBSCAN"""
    
    if len(texts) < 2:
        return {0: texts}
    
    # Создаем TF-IDF векторы
    vectorizer = TfidfVectorizer(
        max_features=1000,
        stop_words=list(CLUSTERING_STOPWORDS),
        ngram_range=(1, 2),
        min_df=1
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform(texts)
        
        # Используем DBSCAN для кластеризации
        clustering = DBSCAN(
            eps=1-threshold,
            min_samples=min_samples,
            metric='cosine'
        )
        
        cluster_labels = clustering.fit_predict(tfidf_matrix)
        
        # Группируем по кластерам
        clusters = {}
        for i, label in enumerate(cluster_labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(texts[i])
        
        return clusters
        
    except Exception as e:
        logger.warning(f"Ошибка семантической кластеризации: {e}")
        # Fallback к простой группировке
        return {0: texts}

def merge_semantic_clusters(clusters: Dict[int, List[str]]) -> List[Dict[str, Any]]:
    """Объединение семантически похожих кластеров"""
    
    merged = []
    
    for cluster_id, texts in clusters.items():
        if cluster_id == -1:  # Шум в DBSCAN
            continue
            
        if not texts:
            continue
        
        # Подсчитываем частоту
        text_counts = Counter(texts)
        
        # Создаем варианты
        variants = []
        for text, count in text_counts.most_common():
            variants.append({
                "text": text,
                "count": count
            })
        
        # Генерируем название кластера
        cluster_name = generate_cluster_name(texts)
        
        # Создаем кластер
        cluster = {
            "name": cluster_name,
            "variants": variants,
            "dialog_ids": [],  # Будет заполнено позже
            "slices": {
                "regions": {},
                "segments": {},
                "product_categories": {},
                "delivery_types": {},
                "sentiment": {}
            },
            "semantic_coherence": calculate_semantic_coherence(texts),
            "priority": calculate_cluster_priority(texts),
            "description": generate_cluster_description(texts)
        }
        
        merged.append(cluster)
    
    return merged

def generate_cluster_name(texts: List[str]) -> str:
    """Генерация названия кластера на основе текстов"""
    
    # Извлекаем ключевые слова
    words = []
    for text in texts:
        # Простая токенизация
        text_words = re.findall(r'\b[а-яё]+\b', text.lower())
        words.extend([w for w in text_words if len(w) > 2 and w not in CLUSTERING_STOPWORDS])
    
    # Находим наиболее частые слова
    word_counts = Counter(words)
    top_words = [word for word, count in word_counts.most_common(3)]
    
    if top_words:
        return " ".join(top_words)
    else:
        return "Неопределенный кластер"

def calculate_semantic_coherence(texts: List[str]) -> float:
    """Вычисление семантической связности кластера"""
    
    if len(texts) < 2:
        return 1.0
    
    try:
        # Создаем TF-IDF векторы
        vectorizer = TfidfVectorizer(
            max_features=500,
            stop_words=list(CLUSTERING_STOPWORDS),
            ngram_range=(1, 2)
        )
        
        tfidf_matrix = vectorizer.fit_transform(texts)
        
        # Вычисляем косинусное сходство
        similarity_matrix = cosine_similarity(tfidf_matrix)
        
        # Убираем диагональ (сходство с самим собой)
        mask = np.ones(similarity_matrix.shape, dtype=bool)
        np.fill_diagonal(mask, False)
        
        # Среднее сходство
        mean_similarity = similarity_matrix[mask].mean()
        
        return float(mean_similarity)
        
    except Exception as e:
        logger.warning(f"Ошибка вычисления связности: {e}")
        return 0.5

def calculate_cluster_priority(texts: List[str]) -> str:
    """Вычисление приоритета кластера"""
    
    # Ключевые слова для определения приоритета
    high_priority_keywords = [
        "критично", "срочно", "блокирует", "не работает", "ошибка",
        "не понимаю", "как сделать", "помогите"
    ]
    
    medium_priority_keywords = [
        "проблема", "сложно", "неудобно", "долго", "дорого"
    ]
    
    texts_lower = [text.lower() for text in texts]
    
    for keyword in high_priority_keywords:
        if any(keyword in text for text in texts_lower):
            return "высокий"
    
    for keyword in medium_priority_keywords:
        if any(keyword in text for text in texts_lower):
            return "средний"
    
    return "низкий"

def generate_cluster_description(texts: List[str]) -> str:
    """Генерация описания кластера"""
    
    if not texts:
        return "Пустой кластер"
    
    # Анализируем общие паттерны
    common_words = []
    for text in texts:
        words = re.findall(r'\b[а-яё]+\b', text.lower())
        common_words.extend([w for w in words if len(w) > 2])
    
    word_counts = Counter(common_words)
    top_words = [word for word, count in word_counts.most_common(5)]
    
    if top_words:
        return f"Кластер связан с: {', '.join(top_words)}"
    else:
        return "Описание недоступно"

def enhanced_clustering(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Улучшенная кластеризация с семантическим анализом"""
    
    logger.info("🔍 Запуск улучшенной кластеризации...")
    
    # Извлекаем тексты по категориям
    barriers = []
    ideas = []
    signals = []
    
    for extraction in extractions:
        # Барьеры
        for barrier in extraction.get("barriers", []):
            if isinstance(barrier, dict):
                barriers.append(barrier["text"])
            else:
                barriers.append(barrier)
        
        # Идеи
        for idea in extraction.get("ideas", []):
            if isinstance(idea, dict):
                ideas.append(idea["text"])
            else:
                ideas.append(idea)
        
        # Сигналы
        for signal in extraction.get("signals", []):
            if isinstance(signal, dict):
                signals.append(signal["text"])
            else:
                signals.append(signal)
    
    logger.info(f"Найдено: {len(barriers)} барьеров, {len(ideas)} идей, {len(signals)} сигналов")
    
    # Кластеризация
    barrier_clusters = semantic_cluster(barriers, threshold=0.6)
    idea_clusters = semantic_cluster(ideas, threshold=0.7)
    signal_clusters = semantic_cluster(signals, threshold=0.8)
    
    # Объединение
    merged_barriers = merge_semantic_clusters(barrier_clusters)
    merged_ideas = merge_semantic_clusters(idea_clusters)
    merged_signals = merge_semantic_clusters(signal_clusters)
    
    # Добавляем метаданные
    for cluster in merged_barriers + merged_ideas + merged_signals:
        cluster["cluster_type"] = "barrier" if cluster in merged_barriers else "idea" if cluster in merged_ideas else "signal"
        cluster["created_at"] = pd.Timestamp.now().isoformat()
    
    result = {
        "barriers": merged_barriers,
        "ideas": merged_ideas,
        "signals": merged_signals,
        "clustering_metrics": {
            "barrier_clusters": len(merged_barriers),
            "idea_clusters": len(merged_ideas),
            "signal_clusters": len(merged_signals),
            "total_clusters": len(merged_barriers) + len(merged_ideas) + len(merged_signals)
        }
    }
    
    logger.info(f"✅ Создано кластеров: {result['clustering_metrics']['total_clusters']}")
    
    return result

def main():
    """Основная функция"""
    logger.info("🚀 Stage 4 Enhanced: Улучшенная кластеризация")
    
    # Загружаем данные
    input_file = "artifacts/stage2_extracted_enhanced.jsonl"
    if not Path(input_file).exists():
        logger.error(f"Файл {input_file} не найден. Запустите Stage 2 Enhanced сначала.")
        return
    
    extractions = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            extractions.append(json.loads(line))
    
    logger.info(f"Загружено {len(extractions)} извлечений")
    
    # Кластеризация
    clusters = enhanced_clustering(extractions)
    
    # Сохраняем результаты
    output_file = "artifacts/stage4_clusters_enhanced.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clusters, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Результаты сохранены в {output_file}")

if __name__ == "__main__":
    main()
