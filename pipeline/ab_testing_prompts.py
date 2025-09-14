#!/usr/bin/env python3
"""
A/B тестирование промптов для извлечения сущностей
Сравнивает эффективность разных версий промптов
"""

import json
import logging
import pandas as pd
import random
from pathlib import Path
from typing import Dict, Any, List, Tuple
from collections import defaultdict
import statistics

import sys
sys.path.append(str(Path(__file__).parent.parent))

import openai
from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Варианты промптов для тестирования
PROMPT_VARIANTS = {
    "baseline": "prompts/extract_entities.txt",
    "enhanced": "prompts/extract_entities_enhanced.txt",
    "minimal": "prompts/extract_entities_minimal.txt",
    "detailed": "prompts/extract_entities_detailed.txt"
}

def load_prompt_variant(variant_name: str) -> str:
    """Загрузка варианта промпта"""
    
    prompt_file = PROMPT_VARIANTS.get(variant_name)
    if not prompt_file or not Path(prompt_file).exists():
        # Fallback к базовому промпту
        prompt_file = "prompts/extract_entities.txt"
    
    return Path(prompt_file).read_text(encoding="utf-8")

def create_minimal_prompt() -> str:
    """Создание минималистичного промпта"""
    
    return """
Роль: Аналитик диалогов.

Извлеки из диалога информацию о доставке в JSON:

1. БАРЬЕРЫ: проблемы с доставкой
2. ИДЕИ: предложения клиента  
3. СИГНАЛЫ: предпочтения клиента

Формат JSON:
{
  "delivery_related": true,
  "barriers": ["проблема1", "проблема2"],
  "ideas": ["идея1"],
  "signals": ["сигнал1"],
  "delivery_types": ["Avito Доставка"],
  "product_category": "электроника",
  "sentiment": "раздражение"
}
"""

def create_detailed_prompt() -> str:
    """Создание детального промпта"""
    
    return """
Роль: Эксперт-аналитик диалогов по доставке с глубоким пониманием пользовательского опыта и бизнес-контекста.

Цель: Провести комплексный многоуровневый анализ диалога и извлечь максимально структурированную и контекстную информацию для принятия продуктовых решений.

АНАЛИЗИРУЙ ВСЕ РЕПЛИКИ КЛИЕНТА с учетом контекста и подтекста:

1. БАРЬЕРЫ (проблемы клиента с доставкой):
   - Технические: "непонимание процесса", "проблемы с настройками", "сбои системы", "не работает интерфейс"
   - Финансовые: "высокая стоимость", "скрытые платежи", "неожиданные расходы", "дорого доставлять"
   - Логистические: "мало ПВЗ", "недоступность в регион", "долгие сроки", "плохая доставка"
   - Качественные: "повреждение товара", "неправильная упаковка", "потеря посылки", "некачественная доставка"
   - Процессные: "сложный возврат", "плохая коммуникация", "неясные условия", "неудобный процесс"

2. ИДЕИ (конкретные предложения клиента):
   - UX/UI: "единая кнопка выбора", "упростить интерфейс", "добавить подсказки", "улучшить навигацию"
   - Функциональные: "примерка перед покупкой", "предварительный расчет", "уведомления", "отслеживание"
   - Логистические: "больше ПВЗ", "экспресс-доставка", "выбор времени", "гибкие опции"
   - Финансовые: "субсидирование", "скидки за объем", "бесплатная доставка", "прозрачное ценообразование"

3. СИГНАЛЫ (предпочтения и паттерны поведения):
   - Предпочтения: "только Avito Доставка", "люблю самовывоз", "доверяю СДЭК", "предпочитаю курьера"
   - Паттерны: "часто заказываю", "редко пользуюсь", "только для дорогих товаров", "сезонные покупки"
   - Мотивации: "скорость важнее цены", "цена важнее скорости", "удобство превыше всего", "надежность"

4. КОНТЕКСТНЫЙ АНАЛИЗ:
   - Эмоциональное состояние: раздражение, фрустрация, сомнение, позитив, энтузиазм, безразличие, тревога
   - Уровень экспертности: новичок, средний, эксперт, продвинутый
   - Срочность: критично, важно, неважно, низкий приоритет
   - Влияние на решение: блокирует покупку, влияет на выбор, не влияет, улучшает опыт

5. ДОПОЛНИТЕЛЬНЫЕ МЕТАДАННЫЕ:
   - Типы доставки: Avito Доставка, СДЭК, Яндекс Доставка, курьерская, самовывоз, ПВЗ, Почта России
   - Категория товара: электроника, мебель, одежда, стройматериалы, продукты, спорт, другое
   - Регион: Москва, СПб, регионы, удаленные
   - Сегмент клиента: частный, корпоративный, оптовый, B2B

Формат ответа (строго JSON):
{
  "delivery_related": true,
  "barriers": [
    {
      "category": "технические",
      "text": "непонимание процесса доставки",
      "severity": "высокая",
      "context": "клиент не понимает, как подключить доставку",
      "impact": "блокирует покупку"
    }
  ],
  "ideas": [
    {
      "category": "UX/UI", 
      "text": "единая кнопка выбора доставки",
      "feasibility": "высокая",
      "impact": "средний",
      "user_value": "упрощение процесса"
    }
  ],
  "signals": [
    {
      "type": "предпочтения",
      "text": "предпочтение Avito Доставки",
      "confidence": "высокая",
      "context": "клиент явно предпочитает этот сервис",
      "frequency": "часто"
    }
  ],
  "emotional_state": "раздражение",
  "expertise_level": "новичок", 
  "urgency": "важно",
  "decision_impact": "блокирует покупку",
  "delivery_types": ["Avito Доставка"],
  "product_category": "электроника",
  "region": "Москва",
  "segment": "частный",
  "citations": [
    {
      "quote": "Как подключить доставку?",
      "speaker": "Клиент",
      "context": "вопрос о настройке",
      "timestamp": "00:01:30"
    }
  ],
  "confidence_score": 0.85,
  "extraction_notes": "Высокая уверенность в извлечении основных барьеров"
}
"""

def extract_entities_with_prompt(dialog_text: str, dialog_id: str, prompt_variant: str) -> Dict[str, Any]:
    """Извлечение сущностей с использованием конкретного варианта промпта"""
    
    try:
        # Загружаем промпт
        if prompt_variant == "minimal":
            prompt = create_minimal_prompt()
        elif prompt_variant == "detailed":
            prompt = create_detailed_prompt()
        else:
            prompt = load_prompt_variant(prompt_variant)
        
        # Формируем запрос
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Диалог ID: {dialog_id}\n\n{dialog_text}"}
        ]
        
        # Вызываем API
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        
        response = client.chat.completions.create(
            model=settings.model_extract,
            messages=messages,
            temperature=0.1,
            max_tokens=2000
        )
        
        # Парсим ответ
        content = response.choices[0].message.content.strip()
        
        # Извлекаем JSON
        import re
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if not json_match:
            raise ValueError("JSON не найден в ответе")
        
        extraction = json.loads(json_match.group())
        
        # Добавляем метаданные
        extraction["dialog_id"] = dialog_id
        extraction["prompt_variant"] = prompt_variant
        extraction["extraction_timestamp"] = pd.Timestamp.now().isoformat()
        
        return extraction
        
    except Exception as e:
        logger.error(f"Ошибка извлечения для диалога {dialog_id} с промптом {prompt_variant}: {e}")
        return {
            "dialog_id": dialog_id,
            "prompt_variant": prompt_variant,
            "delivery_related": False,
            "barriers": [],
            "ideas": [],
            "signals": [],
            "error": str(e)
        }

def evaluate_extraction_quality(extraction: Dict[str, Any]) -> Dict[str, float]:
    """Оценка качества извлечения"""
    
    quality_metrics = {
        "completeness": 0.0,
        "accuracy": 0.0,
        "consistency": 0.0,
        "overall_score": 0.0
    }
    
    # Полнота (completeness)
    has_barriers = len(extraction.get("barriers", [])) > 0
    has_ideas = len(extraction.get("ideas", [])) > 0
    has_signals = len(extraction.get("signals", [])) > 0
    has_metadata = bool(extraction.get("delivery_types") or extraction.get("product_category"))
    
    completeness = sum([has_barriers, has_ideas, has_signals, has_metadata]) / 4.0
    quality_metrics["completeness"] = completeness
    
    # Точность (accuracy) - проверяем структуру данных
    accuracy_score = 0.0
    accuracy_checks = 0
    
    # Проверяем барьеры
    barriers = extraction.get("barriers", [])
    if barriers:
        accuracy_checks += 1
        if all(isinstance(b, (str, dict)) for b in barriers):
            accuracy_score += 1
    
    # Проверяем идеи
    ideas = extraction.get("ideas", [])
    if ideas:
        accuracy_checks += 1
        if all(isinstance(i, (str, dict)) for i in ideas):
            accuracy_score += 1
    
    # Проверяем сигналы
    signals = extraction.get("signals", [])
    if signals:
        accuracy_checks += 1
        if all(isinstance(s, (str, dict)) for s in signals):
            accuracy_score += 1
    
    # Проверяем метаданные
    if extraction.get("delivery_related") is not None:
        accuracy_checks += 1
        accuracy_score += 1
    
    quality_metrics["accuracy"] = accuracy_score / accuracy_checks if accuracy_checks > 0 else 0.0
    
    # Согласованность (consistency) - проверяем внутреннюю логику
    consistency_score = 1.0
    
    # Если delivery_related = False, не должно быть сущностей
    if not extraction.get("delivery_related", True):
        if barriers or ideas or signals:
            consistency_score -= 0.5
    
    # Проверяем тональность
    sentiment = extraction.get("sentiment", "")
    if sentiment and sentiment not in ["раздражение", "нейтрально", "сомнение", "позитив"]:
        consistency_score -= 0.2
    
    quality_metrics["consistency"] = max(0.0, consistency_score)
    
    # Общий балл
    quality_metrics["overall_score"] = (
        quality_metrics["completeness"] * 0.4 +
        quality_metrics["accuracy"] * 0.4 +
        quality_metrics["consistency"] * 0.2
    )
    
    return quality_metrics

def run_ab_test(dialogs: List[Dict[str, Any]], 
                variants: List[str] = None,
                sample_size: int = 10) -> Dict[str, Any]:
    """Запуск A/B тестирования промптов"""
    
    logger.info("🧪 Запуск A/B тестирования промптов...")
    
    if variants is None:
        variants = list(PROMPT_VARIANTS.keys())
    
    # Создаем минимальный и детальный промпты если их нет
    create_additional_prompts()
    
    # Выбираем случайную выборку диалогов
    if len(dialogs) > sample_size:
        test_dialogs = random.sample(dialogs, sample_size)
    else:
        test_dialogs = dialogs
    
    logger.info(f"Тестируем {len(test_dialogs)} диалогов с {len(variants)} вариантами промптов")
    
    results = {}
    
    for variant in variants:
        logger.info(f"Тестирование варианта: {variant}")
        
        variant_results = []
        quality_scores = []
        
        for dialog in test_dialogs:
            dialog_id = dialog.get("dialog_id", f"dialog_{len(variant_results)}")
            dialog_text = dialog.get("text", "")
            
            if not dialog_text or len(dialog_text.strip()) < 10:
                continue
            
            # Извлекаем сущности
            extraction = extract_entities_with_prompt(dialog_text, dialog_id, variant)
            variant_results.append(extraction)
            
            # Оцениваем качество
            quality = evaluate_extraction_quality(extraction)
            quality_scores.append(quality)
        
        # Анализируем результаты
        if quality_scores:
            avg_quality = {
                "completeness": statistics.mean([q["completeness"] for q in quality_scores]),
                "accuracy": statistics.mean([q["accuracy"] for q in quality_scores]),
                "consistency": statistics.mean([q["consistency"] for q in quality_scores]),
                "overall_score": statistics.mean([q["overall_score"] for q in quality_scores])
            }
        else:
            avg_quality = {"completeness": 0.0, "accuracy": 0.0, "consistency": 0.0, "overall_score": 0.0}
        
        # Подсчитываем статистику
        total_entities = sum(
            len(extraction.get("barriers", [])) + 
            len(extraction.get("ideas", [])) + 
            len(extraction.get("signals", []))
            for extraction in variant_results
        )
        
        successful_extractions = len([e for e in variant_results if not e.get("error")])
        
        results[variant] = {
            "total_dialogs": len(variant_results),
            "successful_extractions": successful_extractions,
            "success_rate": successful_extractions / len(variant_results) if variant_results else 0.0,
            "total_entities": total_entities,
            "avg_entities_per_dialog": total_entities / len(variant_results) if variant_results else 0.0,
            "quality_metrics": avg_quality,
            "extractions": variant_results
        }
    
    # Определяем лучший вариант
    best_variant = max(results.keys(), key=lambda v: results[v]["quality_metrics"]["overall_score"])
    
    ab_test_results = {
        "test_metadata": {
            "total_dialogs": len(test_dialogs),
            "variants_tested": variants,
            "test_timestamp": pd.Timestamp.now().isoformat()
        },
        "results": results,
        "best_variant": best_variant,
        "recommendations": generate_ab_test_recommendations(results)
    }
    
    logger.info(f"✅ A/B тестирование завершено. Лучший вариант: {best_variant}")
    
    return ab_test_results

def create_additional_prompts():
    """Создание дополнительных промптов для тестирования"""
    
    # Создаем минимальный промпт
    minimal_prompt = create_minimal_prompt()
    Path("prompts/extract_entities_minimal.txt").write_text(minimal_prompt, encoding="utf-8")
    
    # Создаем детальный промпт
    detailed_prompt = create_detailed_prompt()
    Path("prompts/extract_entities_detailed.txt").write_text(detailed_prompt, encoding="utf-8")

def generate_ab_test_recommendations(results: Dict[str, Any]) -> List[str]:
    """Генерация рекомендаций на основе результатов A/B тестирования"""
    
    recommendations = []
    
    # Анализируем результаты
    best_variant = max(results.keys(), key=lambda v: results[v]["quality_metrics"]["overall_score"])
    worst_variant = min(results.keys(), key=lambda v: results[v]["quality_metrics"]["overall_score"])
    
    best_score = results[best_variant]["quality_metrics"]["overall_score"]
    worst_score = results[worst_variant]["quality_metrics"]["overall_score"]
    
    recommendations.append(f"🏆 Рекомендуется использовать вариант '{best_variant}' (балл: {best_score:.3f})")
    
    if best_score - worst_score > 0.2:
        recommendations.append(f"⚠️ Вариант '{worst_variant}' значительно хуже (балл: {worst_score:.3f})")
    
    # Анализируем конкретные метрики
    for variant, data in results.items():
        quality = data["quality_metrics"]
        
        if quality["completeness"] < 0.7:
            recommendations.append(f"📝 Вариант '{variant}' имеет низкую полноту ({quality['completeness']:.3f})")
        
        if quality["accuracy"] < 0.8:
            recommendations.append(f"🎯 Вариант '{variant}' имеет низкую точность ({quality['accuracy']:.3f})")
        
        if quality["consistency"] < 0.8:
            recommendations.append(f"🔄 Вариант '{variant}' имеет низкую согласованность ({quality['consistency']:.3f})")
    
    return recommendations

def main():
    """Основная функция"""
    logger.info("🚀 A/B тестирование промптов")
    
    # Загружаем результаты stage1
    stage1_file = "artifacts/stage1_delivery.jsonl"
    if not Path(stage1_file).exists():
        logger.error(f"Файл {stage1_file} не найден. Сначала запустите Stage 1.")
        return
    
    # Читаем результаты stage1
    delivery_dialogs = []
    with open(stage1_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line.strip())
            if data.get('delivery_discussed', False):
                delivery_dialogs.append(data)
    
    if not delivery_dialogs:
        logger.error("Не найдено диалогов с доставкой для тестирования")
        return
    
    # Загружаем исходные диалоги для получения текста
    input_file = "data/dialogs.xlsx"
    df = pd.read_excel(input_file, engine='openpyxl')
    original_dialogs = df.to_dict('records')
    
    # Создаем список диалогов для тестирования
    test_dialogs = []
    for dialog_data in delivery_dialogs:
        dialog_id = dialog_data.get('dialog_id', 'unknown')
        
        # Находим исходный диалог по ID
        original_dialog = next((d for d in original_dialogs if str(d.get('ID звонка', '')) == dialog_id), None)
        if original_dialog:
            test_dialogs.append({
                'dialog_id': dialog_id,
                'text': original_dialog.get('Текст транскрибации', ''),
                'delivery_discussed': True
            })
    
    if not test_dialogs:
        logger.error("Не найдено диалогов с доставкой для тестирования")
        return
    
    # Запускаем A/B тест
    ab_results = run_ab_test(test_dialogs, sample_size=5)
    
    # Сохраняем результаты
    output_file = "reports/ab_test_results.json"
    Path("reports").mkdir(exist_ok=True, parents=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(ab_results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Результаты A/B тестирования сохранены: {output_file}")

if __name__ == "__main__":
    main()
