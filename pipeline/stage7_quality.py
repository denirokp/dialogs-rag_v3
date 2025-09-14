#!/usr/bin/env python3
"""
Stage 7: Метрики качества
Вычисляет метрики качества анализа и сохраняет их в reports/quality.json
"""

import json
import statistics
import logging
from pathlib import Path
from typing import Dict, Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def compute_quality(aggregate_path: str = "artifacts/aggregate_results.json", out: str = "reports/quality.json") -> Dict[str, Any]:
    """Вычисление метрик качества анализа"""
    logger.info("🔍 Вычисление метрик качества...")
    
    # Загружаем агрегированные результаты
    agg = json.loads(Path(aggregate_path).read_text(encoding="utf-8"))
    N = agg["meta"]["N"]
    D = agg["meta"]["D"]

    # Оценим на уровне raw (если есть) или по агрегатам
    clusters = agg.get("barriers", []) + agg.get("ideas", []) + agg.get("signals", []) + agg.get("signals_platform", [])
    clusters_count = len(clusters)
    mentions = [c["mentions_abs"] for c in clusters] or [0]
    avg_mentions = statistics.mean(mentions)

    # Если есть stage1.5/2 артефакты — подтяни:
    valid_rate = None
    try:
        s15_path = Path("artifacts/stage1_5_sampling.jsonl")
        if s15_path.exists():
            s15 = [json.loads(l) for l in s15_path.read_text(encoding="utf-8").splitlines()]
            valid_samples = sum(1 for x in s15 if x.get("valid_sample"))
            valid_rate = (valid_samples / D) if D else 0.0
    except Exception as e:
        logger.warning(f"Не удалось загрузить stage1.5 данные: {e}")

    no_entities_rate = None
    avg_quotes_per_cluster = None
    try:
        s2_path = Path("artifacts/stage2_extracted.jsonl")
        if s2_path.exists():
            s2 = [json.loads(l) for l in s2_path.read_text(encoding="utf-8").splitlines()]
            no_entities = sum(1 for x in s2 if x.get("delivery_discussed") and not (x.get("barriers") or x.get("ideas") or x.get("signals")))
            no_entities_rate = (no_entities / D) if D else 0.0
            
            quotes_per_cluster = []
            for c in clusters:
                quotes_per_cluster.append(len(c.get("quotes", [])))
            avg_quotes_per_cluster = statistics.mean(quotes_per_cluster) if quotes_per_cluster else 0.0
    except Exception as e:
        logger.warning(f"Не удалось загрузить stage2 данные: {e}")

    # Вычисляем дополнительные метрики
    total_quotes = sum(len(c.get("quotes", [])) for c in clusters)
    clusters_with_quotes = sum(1 for c in clusters if c.get("quotes"))
    dialogs_with_quotes = sum(1 for c in clusters if (c.get("quotes") or []))
    low_evidence_clusters = sum(1 for c in clusters if c.get("low_evidence_share", 0) >= 0.5)
    
    # Распределение по типам кластеров
    barriers_count = len(agg.get("barriers", []))
    ideas_count = len(agg.get("ideas", []))
    signals_count = len(agg.get("signals", []))
    signals_platform_count = len(agg.get("signals_platform", []))

    out_data = {
        "N": N,
        "D": D, 
        "D_share": round((D/N), 3) if N else 0.0,
        "valid_rate": round(valid_rate, 3),
        "no_entities_rate": round(no_entities_rate, 3),
        "clusters_count": clusters_count,
        "avg_mentions_per_cluster": round(avg_mentions, 2),
        "avg_quotes_per_cluster": round(avg_quotes_per_cluster, 2) if avg_quotes_per_cluster is not None else None,
        "total_quotes": total_quotes,
        "clusters_with_quotes": clusters_with_quotes,
        "quotes_coverage": round((dialogs_with_quotes / (D or 1)), 3),
        "low_evidence_clusters": low_evidence_clusters,
        "clusters_by_type": {
            "barriers": barriers_count,
            "ideas": ideas_count,
            "signals": signals_count,
            "signals_platform": signals_platform_count
        }
    }
    
    # Создаем папку reports если не существует
    Path("reports").mkdir(exist_ok=True, parents=True)
    
    # Сохраняем метрики
    Path(out).write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info(f"📊 Метрики качества сохранены: {out}")
    logger.info(f"  Кластеров: {clusters_count}")
    logger.info(f"  Среднее упоминаний на кластер: {avg_mentions:.2f}")
    logger.info(f"  Всего цитат: {total_quotes}")
    logger.info(f"  Покрытие цитатами: {out_data['quotes_coverage']:.2%}")
    
    return out_data


def main():
    """Основная функция Stage 7"""
    logger.info("🚀 Stage 7: Метрики качества")
    
    try:
        quality_metrics = compute_quality()
        logger.info("✅ Stage 7 завершен успешно!")
        return quality_metrics
    except Exception as e:
        logger.error(f"❌ Ошибка в Stage 7: {e}")
        raise


if __name__ == "__main__":
    main()
