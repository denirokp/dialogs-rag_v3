#!/usr/bin/env python3
"""
Stage 4: Кластеризация формулировок
Группирует похожие барьеры и идеи в кластеры
"""

import json
import logging
import re
from pathlib import Path
from collections import Counter, defaultdict
from typing import List, Dict, Any, Set, Tuple
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from models.validation import DialogExtraction, Cluster, ClusterVariant, ClusterSlices, ClustersData
from prompts import STAGE_CONFIG, CLUSTERING_STOPWORDS

# Константы для авто-лейблов
TRASH = {"вопрос", "проблема", "предложение", "клиентов", "клиента", "платформы"}
BAD = re.compile(r"(пизд|хер|блин|черт|хрень)", re.IGNORECASE)
WORD_RX = re.compile(r"[A-Za-zА-Яа-я0-9\-]+")
QUESTION_RX = re.compile(r"\b(как|почему|где|что делать|что дальше)\b", re.IGNORECASE)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def top_terms(texts: List[str], topn: int = 5) -> List[str]:
    """Извлечение топ-N терминов из текстов"""
    cnt = Counter()
    for t in texts:
        for w in WORD_RX.findall(t.lower()):
            if len(w) < 3: 
                continue
            if w in TRASH:
                continue
            cnt[w] += 1
    return [w for w, _ in cnt.most_common(topn)]


def norm(s: str) -> str:
    """Нормализация текста"""
    s = s.replace("ё", "е").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def short_label(variants: List[str]) -> str:
    """Короткий и осмысленный лейбл кластера"""
    cand = [re.sub(r"\s+", " ", v.strip().lower()) for v in variants if v.strip()]
    cnt = Counter(cand)
    
    # 1) взять частую фразу до 5 слов, без мусора и мата
    for v, _ in cnt.most_common():
        if BAD.search(v): 
            continue
        if any(t in v for t in TRASH): 
            continue
        if len(v.split()) <= 5:
            return v.capitalize()
    
    # 2) fallback по топ-термам
    terms = top_terms(cand, topn=3)
    return (" ".join(terms)).capitalize() if terms else "Кластер"


def promote_questions_to_barriers(item):
    """Промо вопросов в барьеры"""
    txt = (item.get("evidence_span") or "").lower()
    if QUESTION_RX.search(txt):
        labs = set(item.get("labels", {}).get("barrier", []))
        labs.update({"непонимание процесса доставки"})
        item.setdefault("labels", {}).setdefault("barrier", [])
        item["labels"]["barrier"] = list(labs)
    return item


def auto_label_cluster(variants: List[str]) -> str:
    """Обратная совместимость - использует новую функцию"""
    return short_label(variants)


def auto_label(variants: List[str]) -> str:
    """Обратная совместимость - использует новую функцию"""
    return auto_label_cluster(variants)


def cluster_texts(texts: List[str]) -> List[int]:
    """Кластеризация текстов с дроблением больших кластеров"""
    if len(texts) < 2:
        return [0] * len(texts)
    
    # TF-IDF векторизация
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2), 
        max_df=0.6, 
        min_df=2,
        stop_words=None
    )
    X = vectorizer.fit_transform(texts)
    
    # Эвристика для количества кластеров
    k = min(max(2, len(texts) // 8), STAGE_CONFIG["max_clusters"])
    
    # KMeans кластеризация
    km = KMeans(n_clusters=k, n_init="auto", random_state=42)
    labels = km.fit_predict(X)
    
    # Фильтрация по минимальному размеру кластера
    MIN_CLUSTER = STAGE_CONFIG["min_cluster_size"]
    clusters = {i: [] for i in range(k)}
    for text, label in zip(texts, labels):
        clusters[label].append(text)
    
    # Разделение на плотные и редкие кластеры
    dense_clusters = [c for c in clusters.values() if len(c) >= MIN_CLUSTER]
    rare_cases = [c for c in clusters.values() if len(c) < MIN_CLUSTER]
    
    # Пересчет лейблов
    new_labels = []
    label_map = {}
    new_label = 0
    
    for i, text in enumerate(texts):
        old_label = labels[i]
        if old_label not in label_map:
            if len(clusters[old_label]) >= MIN_CLUSTER:
                label_map[old_label] = new_label
                new_label += 1
            else:
                # Редкие случаи объединяем в один кластер
                label_map[old_label] = -1
        new_labels.append(label_map[old_label])
    
    return new_labels


def create_embeddings(texts: List[str]) -> np.ndarray:
    """Создание эмбеддингов для текстов"""
    if not texts:
        return np.array([])
    
    # Используем TF-IDF для простоты
    vectorizer = TfidfVectorizer(
        max_features=1000,
        stop_words=None,  # Не убираем стоп-слова для русского языка
        ngram_range=(1, 2)
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform(texts)
        return tfidf_matrix.toarray()
    except Exception as e:
        logger.error(f"Ошибка создания эмбеддингов: {e}")
        return np.array([])


def cluster_texts(texts: List[str], min_clusters: int = 2, max_clusters: int = 10) -> List[int]:
    """Кластеризация текстов"""
    if len(texts) < 2:
        return [0] * len(texts)
    
    embeddings = create_embeddings(texts)
    if embeddings.size == 0:
        return [0] * len(texts)
    
    # Определяем оптимальное количество кластеров
    n_clusters = min(max(min_clusters, len(texts) // 3), max_clusters, len(texts))
    
    try:
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(embeddings)
        return cluster_labels.tolist()
    except Exception as e:
        logger.error(f"Ошибка кластеризации: {e}")
        return [0] * len(texts)


def create_cluster_variants(texts: List[str], counts: List[int]) -> List[ClusterVariant]:
    """Создание вариантов формулировок для кластера"""
    # Группируем по тексту и суммируем счетчики
    text_counts = defaultdict(int)
    for text, count in zip(texts, counts):
        text_counts[text] += count
    
    # Сортируем по количеству упоминаний
    sorted_variants = sorted(text_counts.items(), key=lambda x: x[1], reverse=True)
    
    return [ClusterVariant(text=text, count=count) for text, count in sorted_variants]


def create_cluster_slices(dialogs: List[DialogExtraction], cluster_texts: List[str]) -> ClusterSlices:
    """Создание срезов данных для кластера"""
    regions = Counter()
    segments = Counter()
    product_categories = Counter()
    delivery_types = Counter()
    sentiment = Counter()
    
    for dialog in dialogs:
        # Проверяем, есть ли тексты кластера в диалоге
        dialog_texts = dialog.barriers + dialog.ideas + dialog.signals
        if any(text in dialog_texts for text in cluster_texts):
            if dialog.region:
                regions[dialog.region] += 1
            if dialog.segment:
                segments[dialog.segment] += 1
            if dialog.product_category:
                product_categories[dialog.product_category] += 1
            for delivery_type in dialog.delivery_types:
                delivery_types[delivery_type] += 1
            if dialog.sentiment:
                sentiment[dialog.sentiment] += 1
    
    return ClusterSlices(
        regions=dict(regions),
        segments=dict(segments),
        product_categories=dict(product_categories),
        delivery_types=dict(delivery_types),
        sentiment=dict(sentiment)
    )


def cluster_category(dialogs: List[DialogExtraction], category: str) -> List[Cluster]:
    """Кластеризация по категории (барьеры, идеи, сигналы)"""
    # Собираем все тексты и их счетчики
    text_counts = Counter()
    text_to_dialogs = defaultdict(list)
    
    for dialog in dialogs:
        texts = getattr(dialog, category, [])
        for text in texts:
            text_counts[text] += 1
            text_to_dialogs[text].append(dialog)
    
    if not text_counts:
        return []
    
    # Подготавливаем данные для кластеризации
    texts = list(text_counts.keys())
    counts = [text_counts[text] for text in texts]
    
    # Кластеризация
    cluster_labels = cluster_texts(texts)
    
    # Группируем по кластерам
    clusters = defaultdict(list)
    for text, label in zip(texts, cluster_labels):
        clusters[label].append(text)
    
    # Создаем объекты кластеров
    result_clusters = []
    
    for cluster_id, cluster_texts_list in clusters.items():
        if not cluster_texts_list:
            continue
        
        # Создаем варианты формулировок
        cluster_counts = [text_counts[text] for text in cluster_texts_list]
        variants = create_cluster_variants(cluster_texts_list, cluster_counts)
        
        # Собираем ID диалогов
        dialog_ids = set()
        for text in cluster_texts_list:
            for dialog in text_to_dialogs[text]:
                dialog_ids.add(dialog.dialog_id)
        
        # Создаем срезы данных
        cluster_dialogs = [dialog for dialog in dialogs if dialog.dialog_id in dialog_ids]
        slices = create_cluster_slices(cluster_dialogs, cluster_texts_list)
        
        # Название кластера - автоматическое определение
        cluster_name = auto_label(cluster_texts_list)
        
        cluster = Cluster(
            name=cluster_name,
            variants=variants,
            dialog_ids=list(dialog_ids),
            slices=slices
        )
        
        result_clusters.append(cluster)
    
    # Сортируем по общему количеству упоминаний
    result_clusters.sort(key=lambda c: sum(v.count for v in c.variants), reverse=True)
    
    return result_clusters


def load_normalized_dialogs() -> List[DialogExtraction]:
    """Загрузка нормализованных диалогов"""
    normalized_file = Path("artifacts/stage3_normalized.jsonl")
    
    if not normalized_file.exists():
        logger.error(f"❌ Файл {normalized_file} не найден. Запустите Stage 3 сначала.")
        return []
    
    dialogs = []
    
    with open(normalized_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                dialog = DialogExtraction(**data)
                dialogs.append(dialog)
            except Exception as e:
                logger.error(f"Ошибка чтения диалога: {e}")
                continue
    
    return dialogs


def main():
    """Главная функция Stage 4"""
    logger.info("🚀 Stage 4: Кластеризация формулировок")
    
    # Создание папки artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Загрузка нормализованных диалогов
    dialogs = load_normalized_dialogs()
    logger.info(f"📊 Загружено {len(dialogs)} диалогов для кластеризации")
    
    if not dialogs:
        logger.error("❌ Нет диалогов для кластеризации")
        return
    
    # Кластеризация по категориям
    logger.info("🔄 Кластеризация барьеров...")
    barrier_clusters = cluster_category(dialogs, "barriers")
    
    logger.info("🔄 Кластеризация идей...")
    idea_clusters = cluster_category(dialogs, "ideas")
    
    logger.info("🔄 Кластеризация сигналов...")
    signal_clusters = cluster_category(dialogs, "signals")
    
    # Создание результата
    clusters_data = ClustersData(
        barriers=barrier_clusters,
        ideas=idea_clusters,
        signals=signal_clusters
    )
    
    # Сохранение результатов
    output_file = artifacts_dir / "stage4_clusters.json"
    logger.info(f"💾 Сохранение результатов в {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clusters_data.dict(), f, ensure_ascii=False, indent=2)
    
    # Статистика
    total_barrier_mentions = sum(sum(v.count for v in c.variants) for c in barrier_clusters)
    total_idea_mentions = sum(sum(v.count for v in c.variants) for c in idea_clusters)
    total_signal_mentions = sum(sum(v.count for v in c.variants) for c in signal_clusters)
    
    logger.info("📊 Статистика кластеризации:")
    logger.info(f"  Кластеров барьеров: {len(barrier_clusters)}")
    logger.info(f"  Кластеров идей: {len(idea_clusters)}")
    logger.info(f"  Кластеров сигналов: {len(signal_clusters)}")
    logger.info(f"  Всего упоминаний барьеров: {total_barrier_mentions}")
    logger.info(f"  Всего упоминаний идей: {total_idea_mentions}")
    logger.info(f"  Всего упоминаний сигналов: {total_signal_mentions}")
    
    # Показываем топ-кластеры
    if barrier_clusters:
        logger.info("🔍 Топ-3 кластера барьеров:")
        for i, cluster in enumerate(barrier_clusters[:3]):
            total_mentions = sum(v.count for v in cluster.variants)
            logger.info(f"  {i+1}. {cluster.name} ({total_mentions} упоминаний)")
    
    if idea_clusters:
        logger.info("🔍 Топ-3 кластера идей:")
        for i, cluster in enumerate(idea_clusters[:3]):
            total_mentions = sum(v.count for v in cluster.variants)
            logger.info(f"  {i+1}. {cluster.name} ({total_mentions} упоминаний)")
    
    logger.info("✅ Stage 4 завершен успешно!")


if __name__ == "__main__":
    main()
