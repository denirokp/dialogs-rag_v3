#!/usr/bin/env python3
"""
Stage 5: Агрегация метрик
Подсчитывает метрики N/D, частоты и проценты по кластерам
"""

import json
import logging
import pandas as pd
from pathlib import Path
from collections import Counter, defaultdict
from typing import List, Dict, Any, Tuple

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from models.validation import DeliveryDetection, ClustersData, AggregateResults
from prompts import STAGE_CONFIG, DELIVERY_KEYWORDS, PLATFORM_KEYWORDS

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Загрузка JSONL файла"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data.append(json.loads(line.strip()))
            except Exception as e:
                logger.error(f"Ошибка чтения строки: {e}")
                continue
    return data


def unique_count_and_ids(records_for_cluster):
    """Подсчет уникальных диалогов и их ID"""
    ids = {r["dialog_id"] for r in records_for_cluster}
    return len(ids), sorted(ids)


def pct_of_D(x, D):
    """Процент от D"""
    return round(100.0 * x / D, 1) if D else 0.0


def split_ideas_by_role(records):
    """Разделение идей по ролям: клиентские vs операторские"""
    client_side, operator_side = [], []
    for r in records:
        ci = [i for i in r.get("ideas", []) if i.get("source_role") == "client"]
        oi = [i for i in r.get("ideas", []) if i.get("source_role") == "operator"]
        if ci:
            rr = dict(r)
            rr["ideas"] = ci
            client_side.append(rr)
        if oi:
            rr = dict(r)
            rr["ideas"] = oi
            operator_side.append(rr)
    return client_side, operator_side


def promote_questions_to_barriers(record):
    """Эвристика: клиентские вопросы/непонимание → в Барьеры"""
    new_barriers = record.get("barriers", [])[:]
    for i in record.get("ideas", []):
        txt = (i.get("text") or "").lower()
        if any(k in txt for k in ["как ", "почему", "не подключено", "где кнопка", "не работает"]):
            new_barriers.append("непонимание/неверная настройка доставки")
    record["barriers"] = list({b.strip() for b in new_barriers})
    return record


def split_sections(records, require_client_role_for_ideas=True):
    """Разделение записей по секциям"""
    barriers, ideas_client, signals, operator_recos = [], [], [], []
    for r in records:
        src = r.get("source_role", "client")
        labs = r.get("labels", {})
        if labs.get("barrier"):
            barriers.append(r)
        elif src == "client" and labs.get("idea"):
            ideas_client.append(r)
        elif labs.get("signal"):
            signals.append(r)
        elif src == "operator":
            operator_recos.append(r)
    if not require_client_role_for_ideas:
        # можно объединить по желанию
        pass
    return barriers, ideas_client, signals, operator_recos


def cluster_mentions(cluster_records):
    """Подсчет упоминаний по уникальным dialog_id"""
    return len({r["dialog_id"] for r in cluster_records})


def classify_cluster(records_for_cluster):
    """Классификация кластера по доле доставочных диалогов"""
    total = len({r["dialog_id"] for r in records_for_cluster})
    delivery_ids = {r["dialog_id"] for r in records_for_cluster if r.get("delivery_discussed")}
    share = len(delivery_ids) / total if total else 0.0
    if share >= 0.6:  # порог можно вынести в конфиг
        return "delivery"
    return "platform_signals"


def classify_cluster_by_dialogs(cluster: Dict[str, Any]) -> str:
    """Классификация кластера по доле доставочных диалогов"""
    dialog_ids = cluster.get("dialog_ids", [])
    if not dialog_ids:
        return "platform_signals"
    
    # Загружаем данные о доставке для этих диалогов
    detections_data = load_jsonl("artifacts/stage1_delivery.jsonl")
    delivery_dialog_ids = {d["dialog_id"] for d in detections_data if d.get("delivery_discussed")}
    
    # Подсчитываем долю доставочных диалогов в кластере
    delivery_count = sum(1 for dialog_id in dialog_ids if dialog_id in delivery_dialog_ids)
    total_count = len(dialog_ids)
    share = delivery_count / total_count if total_count else 0.0
    
    if share >= 0.6:  # порог можно вынести в конфиг
        return "delivery"
    return "platform_signals"


def separate_platform_signals(clusters: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Разделение сигналов на доставку и платформу по доле доставочных диалогов"""
    delivery_signals = []
    platform_signals = []
    
    for cluster in clusters:
        # Классифицируем кластер по доле доставочных диалогов
        cluster_type = classify_cluster_by_dialogs(cluster)
        
        if cluster_type == "delivery":
            delivery_signals.append(cluster)
        else:
            platform_signals.append(cluster)
    
    return delivery_signals, platform_signals


def load_delivery_detections() -> List[DeliveryDetection]:
    """Загрузка результатов детекции доставки"""
    delivery_file = Path("artifacts/stage1_delivery.jsonl")
    
    if not delivery_file.exists():
        logger.error(f"❌ Файл {delivery_file} не найден. Запустите Stage 1 сначала.")
        return []
    
    detections = []
    
    with open(delivery_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                detection = DeliveryDetection(**data)
                detections.append(detection)
            except Exception as e:
                logger.error(f"Ошибка чтения детекции: {e}")
                continue
    
    return detections


def load_clusters() -> ClustersData:
    """Загрузка результатов кластеризации"""
    clusters_file = Path("artifacts/stage4_clusters.json")
    
    if not clusters_file.exists():
        logger.error(f"❌ Файл {clusters_file} не найден. Запустите Stage 4 сначала.")
        return ClustersData()
    
    with open(clusters_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return ClustersData(**data)


def calculate_quality_metrics(detections: List[DeliveryDetection], clusters: ClustersData) -> Dict[str, int]:
    """Расчет метрик качества пайплайна"""
    # N = общее количество диалогов
    total_dialogs = len(detections)
    
    # D = количество диалогов с доставкой
    delivery_dialogs = sum(1 for d in detections if d.delivery_discussed)
    
    # X = количество диалогов с доставкой, но без сущностей
    delivery_dialog_ids = {d.dialog_id for d in detections if d.delivery_discussed}
    
    # Собираем все ID диалогов с сущностями
    entity_dialog_ids = set()
    for cluster in clusters.barriers + clusters.ideas + clusters.signals:
        entity_dialog_ids.update(cluster.dialog_ids)
    
    # X = диалоги с доставкой, но без сущностей
    delivery_without_entities = len(delivery_dialog_ids - entity_dialog_ids)
    
    # Y = диалоги без доставки, но с сущностями
    non_delivery_dialog_ids = {d.dialog_id for d in detections if not d.delivery_discussed}
    non_delivery_with_entities = len(non_delivery_dialog_ids & entity_dialog_ids)
    
    return {
        "total_dialogs": total_dialogs,
        "delivery_dialogs": delivery_dialogs,
        "delivery_without_entities": delivery_without_entities,
        "non_delivery_with_entities": non_delivery_with_entities
    }


def per_dialog_counts(records_for_cluster):
    """Подсчет метрик пер-диалог для кластера"""
    unique_ids = sorted({r["dialog_id"] for r in records_for_cluster})
    
    # Варианты = по диалогам
    variant_to_ids = defaultdict(set)
    for r in records_for_cluster:
        seen = set()
        for v in (r.get("variants_in_this_cluster") or r.get("barriers") or []):
            nv = v.strip().lower()
            if nv in seen: 
                continue
            variant_to_ids[nv].add(r["dialog_id"])
            seen.add(nv)
    variants = [{"text": v, "count_abs": len(ids), "dialog_ids": sorted(ids)} for v, ids in variant_to_ids.items()]

    # Срезы
    regions = defaultdict(set)
    sentiments = defaultdict(set)
    delivery_types = defaultdict(set)
    segments = defaultdict(set)
    categories = defaultdict(set)
    
    for r in records_for_cluster:
        did = r["dialog_id"]
        if r.get("region"): 
            regions[r["region"]].add(did)
        if r.get("segment"): 
            segments[r["segment"]].add(did)
        if r.get("product_category"): 
            categories[r["product_category"]].add(did)
        for t in r.get("delivery_types", []): 
            delivery_types[t].add(did)
        if r.get("sentiment"): 
            sentiments[r["sentiment"]].add(did)

    # Собираем цитаты из всех записей кластера
    quotes = []
    low_evidence_count = 0
    for r in records_for_cluster:
        if r.get("citations"):
            for citation in r["citations"]:
                if isinstance(citation, dict) and citation.get("quote"):
                    quotes.append({
                        "quote": citation["quote"],
                        "dialog_id": r["dialog_id"],
                        "speaker": citation.get("speaker", "Клиент")
                    })
        if r.get("extras", {}).get("low_evidence"):
            low_evidence_count += 1
    
    return {
        "mentions_abs": len(unique_ids),
        "dialog_ids": unique_ids,
        "variants": variants,
        "quotes": quotes,
        "low_evidence_share": low_evidence_count / len(unique_ids) if unique_ids else 0.0,
        "slices": {
            "regions": {k: len(v) for k, v in regions.items()},
            "segments": {k: len(v) for k, v in segments.items()},
            "product_categories": {k: len(v) for k, v in categories.items()},
            "delivery_types": {k: len(v) for k, v in delivery_types.items()},
            "sentiment": {k: len(v) for k, v in sentiments.items()},
        }
    }


def calculate_cluster_metrics(clusters: List[Any], D: int, extractions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Расчет метрик для кластеров с учетом уникальных диалогов"""
    cluster_metrics = []
    
    for i, cluster in enumerate(clusters):
        # Получаем записи для этого кластера из данных извлечения
        cluster_dialog_ids = sorted(set(cluster.dialog_ids))
        records_for_cluster = [ext for ext in extractions if ext.get("dialog_id") in cluster_dialog_ids]
        
        # Используем новую функцию per_dialog_counts
        cluster_data = per_dialog_counts(records_for_cluster)
        
        # Обновляем mentions_abs и dialog_ids
        mentions_abs = cluster_data["mentions_abs"]
        cluster_dialog_ids = cluster_data["dialog_ids"]
        
        # Создаем варианты с подсчетом по диалогам
        variants_metrics = []
        for variant in cluster_data["variants"]:
            variants_metrics.append({
                "text": variant["text"],
                "count_abs": variant["count_abs"],
                "count_pct_of_D": pct_of_D(variant["count_abs"], D),
                "dialog_ids": variant["dialog_ids"]
            })
        
        # Используем срезы из per_dialog_counts
        slices_metrics = cluster_data["slices"]
        
        cluster_metric = {
            "cluster_id": i + 1,
            "name": cluster.name,
            "mentions_abs": mentions_abs,
            "mentions_pct_of_D": pct_of_D(mentions_abs, D),
            "dialog_ids": cluster_dialog_ids,
            "variants": variants_metrics,
            "quotes": cluster_data.get("quotes", []),
            "low_evidence_share": cluster_data.get("low_evidence_share", 0.0),
            "slices": slices_metrics
        }
        
        cluster_metrics.append(cluster_metric)
    
    return cluster_metrics


def save_csv_reports(barrier_metrics: List[Dict], idea_metrics: List[Dict], signal_metrics: List[Dict]):
    """Сохранение CSV отчетов"""
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Барьеры
    if barrier_metrics:
        barrier_df = pd.DataFrame(barrier_metrics)
        barrier_df.to_csv(artifacts_dir / "barriers.csv", index=False, encoding='utf-8')
        logger.info(f"💾 Сохранен отчет barriers.csv с {len(barrier_metrics)} записями")
    
    # Идеи
    if idea_metrics:
        idea_df = pd.DataFrame(idea_metrics)
        idea_df.to_csv(artifacts_dir / "ideas.csv", index=False, encoding='utf-8')
        logger.info(f"💾 Сохранен отчет ideas.csv с {len(idea_metrics)} записями")
    
    # Сигналы
    if signal_metrics:
        signal_df = pd.DataFrame(signal_metrics)
        signal_df.to_csv(artifacts_dir / "signals.csv", index=False, encoding='utf-8')
        logger.info(f"💾 Сохранен отчет signals.csv с {len(signal_metrics)} записями")


def main():
    """Главная функция Stage 5"""
    logger.info("🚀 Stage 5: Агрегация метрик")
    
    # Создание папки artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Загрузка данных
    logger.info("📁 Загрузка результатов детекции доставки...")
    detections = load_delivery_detections()
    
    logger.info("📁 Загрузка результатов извлечения сущностей...")
    extractions = load_jsonl("artifacts/stage2_extracted.jsonl")
    
    logger.info("📁 Загрузка результатов кластеризации...")
    clusters = load_clusters()
    
    if not detections:
        logger.error("❌ Нет данных детекции доставки")
        return
    
    if not clusters.barriers and not clusters.ideas and not clusters.signals:
        logger.error("❌ Нет данных кластеризации")
        return
    
    # Расчет базовых метрик N и D
    N = len(detections)  # total_calls_from_excel
    detections_data = load_jsonl("artifacts/stage1_delivery.jsonl")
    D = sum(1 for d in detections_data if d.get("delivery_discussed") is True)
    
    logger.info(f"📊 N (всего диалогов): {N}")
    logger.info(f"📊 D (с доставкой): {D}")
    
    # Расчет метрик качества
    quality_metrics = calculate_quality_metrics(detections, clusters)
    
    # Расчет метрик кластеров
    logger.info("🔄 Расчет метрик кластеров...")
    barrier_metrics = calculate_cluster_metrics(clusters.barriers, D, extractions)
    idea_metrics = calculate_cluster_metrics(clusters.ideas, D, extractions)
    signal_metrics = calculate_cluster_metrics(clusters.signals, D, extractions)
    
    # Разделение сигналов на доставку и платформу
    logger.info("🔄 Разделение сигналов на доставку и платформу...")
    delivery_signals, platform_signals = separate_platform_signals(signal_metrics)
    
    # Создание результата агрегации
    aggregate_result = {
        "barriers": barrier_metrics,
        "ideas": idea_metrics,
        "signals": delivery_signals,
        "signals_platform": platform_signals,
        "meta": {"N": N, "D": D}
    }
    
    # Сохранение результатов
    output_file = artifacts_dir / "aggregate_results.json"
    logger.info(f"💾 Сохранение результатов в {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(aggregate_result, f, ensure_ascii=False, indent=2)
    
    # Сохранение CSV отчетов
    save_csv_reports(barrier_metrics, idea_metrics, signal_metrics)
    
    # Статистика
    logger.info("📊 Статистика агрегации:")
    logger.info(f"  Всего диалогов (N): {N}")
    logger.info(f"  С доставкой (D): {D} ({pct_of_D(D, N)}%)")
    logger.info(f"  Кластеров барьеров: {len(barrier_metrics)}")
    logger.info(f"  Кластеров идей: {len(idea_metrics)}")
    logger.info(f"  Кластеров сигналов доставки: {len(delivery_signals)}")
    logger.info(f"  Кластеров сигналов платформы: {len(platform_signals)}")
    
    # Метрики качества
    logger.info("🔍 Метрики качества пайплайна:")
    logger.info(f"  С доставкой, но без сущностей (X): {quality_metrics['delivery_without_entities']}")
    logger.info(f"  Без доставки, но с сущностями (Y): {quality_metrics['non_delivery_with_entities']}")
    
    # Топ-кластеры
    if barrier_metrics:
        logger.info("🔍 Топ-3 кластера барьеров:")
        for cluster in barrier_metrics[:3]:
            logger.info(f"  {cluster['cluster_id']}. {cluster['name']} - {cluster['mentions_abs']} упоминаний ({cluster['mentions_pct_of_D']}%)")
    
    if idea_metrics:
        logger.info("🔍 Топ-3 кластера идей:")
        for cluster in idea_metrics[:3]:
            logger.info(f"  {cluster['cluster_id']}. {cluster['name']} - {cluster['mentions_abs']} упоминаний ({cluster['mentions_pct_of_D']}%)")
    
    if platform_signals:
        logger.info("🔍 Топ-3 кластера сигналов платформы:")
        for cluster in platform_signals[:3]:
            logger.info(f"  {cluster['cluster_id']}. {cluster['name']} - {cluster['mentions_abs']} упоминаний ({cluster['mentions_pct_of_D']}%)")
    
    logger.info("✅ Stage 5 завершен успешно!")


if __name__ == "__main__":
    main()
