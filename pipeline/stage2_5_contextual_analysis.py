#!/usr/bin/env python3
"""
Stage 2.5: Контекстный анализ
Анализ контекста, паттернов и эмоциональной динамики
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from collections import Counter, defaultdict
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_problem_sequences(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализ последовательности проблем в диалогах"""
    
    sequences = []
    
    for extraction in extractions:
        barriers = extraction.get("barriers", [])
        if len(barriers) > 1:
            # Извлекаем тексты барьеров
            barrier_texts = []
            for barrier in barriers:
                if isinstance(barrier, dict):
                    barrier_texts.append(barrier["text"])
                else:
                    barrier_texts.append(barrier)
            
            sequences.append({
                "dialog_id": extraction.get("dialog_id", ""),
                "barriers": barrier_texts,
                "sequence_length": len(barrier_texts)
            })
    
    # Анализируем частые последовательности
    common_sequences = Counter()
    for seq in sequences:
        if len(seq["barriers"]) >= 2:
            # Создаем пары последовательных барьеров
            for i in range(len(seq["barriers"]) - 1):
                pair = (seq["barriers"][i], seq["barriers"][i + 1])
                common_sequences[pair] += 1
    
    return {
        "total_sequences": len(sequences),
        "common_pairs": {str(k): v for k, v in common_sequences.most_common(10)},
        "avg_sequence_length": sum(s["sequence_length"] for s in sequences) / len(sequences) if sequences else 0
    }

def analyze_emotional_dynamics(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализ эмоциональной динамики"""
    
    emotional_states = []
    sentiment_changes = []
    
    for extraction in extractions:
        emotional_state = extraction.get("emotional_state", "нейтрально")
        sentiment = extraction.get("sentiment", "нейтрально")
        
        emotional_states.append(emotional_state)
        
        # Анализируем изменения тональности
        if "previous_sentiment" in extraction:
            if extraction["previous_sentiment"] != sentiment:
                sentiment_changes.append({
                    "from": extraction["previous_sentiment"],
                    "to": sentiment,
                    "dialog_id": extraction.get("dialog_id", "")
                })
    
    # Статистика эмоциональных состояний
    state_counts = Counter(emotional_states)
    
    return {
        "emotional_distribution": dict(state_counts),
        "sentiment_changes": sentiment_changes,
        "most_common_state": state_counts.most_common(1)[0][0] if state_counts else "нейтрально"
    }

def analyze_decision_impact(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализ влияния на решение о покупке"""
    
    impact_counts = Counter()
    blocking_issues = []
    
    for extraction in extractions:
        decision_impact = extraction.get("decision_impact", "не влияет")
        impact_counts[decision_impact] += 1
        
        if decision_impact == "блокирует покупку":
            barriers = extraction.get("barriers", [])
            for barrier in barriers:
                if isinstance(barrier, dict):
                    blocking_issues.append({
                        "barrier": barrier["text"],
                        "severity": barrier.get("severity", "средняя"),
                        "dialog_id": extraction.get("dialog_id", "")
                    })
    
    return {
        "impact_distribution": dict(impact_counts),
        "blocking_issues": blocking_issues,
        "blocking_rate": impact_counts["блокирует покупку"] / len(extractions) if extractions else 0
    }

def identify_root_causes(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Выявление корневых причин проблем"""
    
    # Ключевые слова для определения корневых причин
    root_cause_keywords = {
        "технические_проблемы": ["не работает", "ошибка", "сбой", "не понимаю", "как настроить"],
        "пользовательский_опыт": ["сложно", "неудобно", "непонятно", "запутанно"],
        "логистические_проблемы": ["не доставляют", "долго", "не в мой город", "мало пвз"],
        "финансовые_проблемы": ["дорого", "скрытые платежи", "неожиданные расходы"]
    }
    
    root_causes = defaultdict(int)
    
    for extraction in extractions:
        barriers = extraction.get("barriers", [])
        
        for barrier in barriers:
            barrier_text = barrier["text"] if isinstance(barrier, dict) else barrier
            barrier_text_lower = barrier_text.lower()
            
            for cause_type, keywords in root_cause_keywords.items():
                if any(keyword in barrier_text_lower for keyword in keywords):
                    root_causes[cause_type] += 1
                    break
    
    return {
        "root_causes": dict(root_causes),
        "most_common_cause": max(root_causes.items(), key=lambda x: x[1])[0] if root_causes else "неизвестно"
    }

def analyze_expertise_patterns(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализ паттернов экспертности пользователей"""
    
    expertise_levels = []
    expertise_barriers = defaultdict(list)
    
    for extraction in extractions:
        expertise_level = extraction.get("expertise_level", "средний")
        expertise_levels.append(expertise_level)
        
        barriers = extraction.get("barriers", [])
        for barrier in barriers:
            barrier_text = barrier["text"] if isinstance(barrier, dict) else barrier
            expertise_barriers[expertise_level].append(barrier_text)
    
    # Анализируем типы проблем по уровням экспертности
    expertise_analysis = {}
    for level, barriers in expertise_barriers.items():
        if barriers:
            # Находим наиболее частые проблемы для каждого уровня
            barrier_counts = Counter(barriers)
            most_common = barrier_counts.most_common(5)
            expertise_analysis[level] = {
                "total_barriers": len(barriers),
                "unique_barriers": len(set(barriers)),
                "most_common": {str(k): v for k, v in most_common}
            }
    
    return {
        "expertise_distribution": dict(Counter(expertise_levels)),
        "expertise_analysis": expertise_analysis
    }

def contextual_analysis(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Основная функция контекстного анализа"""
    
    logger.info("🔍 Запуск контекстного анализа...")
    
    # Анализируем различные аспекты
    problem_sequences = analyze_problem_sequences(extractions)
    emotional_dynamics = analyze_emotional_dynamics(extractions)
    decision_impact = analyze_decision_impact(extractions)
    root_causes = identify_root_causes(extractions)
    expertise_patterns = analyze_expertise_patterns(extractions)
    
    result = {
        "problem_sequences": problem_sequences,
        "emotional_dynamics": emotional_dynamics,
        "decision_impact": decision_impact,
        "root_causes": root_causes,
        "expertise_patterns": expertise_patterns,
        "analysis_timestamp": pd.Timestamp.now().isoformat()
    }
    
    logger.info("✅ Контекстный анализ завершен")
    
    return result

def main():
    """Основная функция"""
    logger.info("🚀 Stage 2.5: Контекстный анализ")
    
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
    
    # Контекстный анализ
    context_analysis = contextual_analysis(extractions)
    
    # Сохраняем результаты
    output_file = "artifacts/stage2_5_contextual_analysis.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(context_analysis, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Результаты сохранены в {output_file}")

if __name__ == "__main__":
    main()
