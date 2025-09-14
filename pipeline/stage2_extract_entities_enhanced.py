#!/usr/bin/env python3
"""
Stage 2 Enhanced: Улучшенное извлечение сущностей
Извлекает барьеры, идеи, сигналы с расширенным контекстным анализом
"""

import json
import logging
import pandas as pd
import re
from pathlib import Path
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict, Any, Set

import sys
sys.path.append(str(Path(__file__).parent.parent))

import openai
from config import settings
from models.validation import DeliveryDetection, DialogExtraction, Citation
from prompts import STAGE_CONFIG, DELIVERY_KEYWORDS

# Расширенные типы тональности
ENHANCED_SENTIMENTS = {
    "раздражение": ["легкое_недовольство", "фрустрация", "гнев", "ярость"],
    "позитив": ["удовлетворение", "энтузиазм", "восторг", "благодарность"],
    "сомнение": ["неуверенность", "тревога", "подозрительность", "скептицизм"],
    "нейтрально": ["безразличие", "спокойствие", "деловитость", "формальность"]
}

# Уровни экспертности
EXPERTISE_LEVELS = ["новичок", "средний", "эксперт"]

# Уровни срочности
URGENCY_LEVELS = ["критично", "важно", "неважно"]

# Влияние на решение
DECISION_IMPACT = ["блокирует покупку", "влияет на выбор", "не влияет"]

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_enhanced_prompt() -> str:
    """Загрузка улучшенного промпта"""
    prompt_file = Path("prompts/extract_entities_enhanced.txt")
    if prompt_file.exists():
        return prompt_file.read_text(encoding="utf-8")
    else:
        # Fallback к базовому промпту
        return load_basic_prompt()

def load_basic_prompt() -> str:
    """Загрузка базового промпта"""
    prompt_file = Path("prompts/extract_entities.txt")
    return prompt_file.read_text(encoding="utf-8")

def normalize_enhanced_sentiment(sentiment: str) -> str:
    """Нормализация расширенной тональности"""
    if not sentiment:
        return "нейтрально"
    
    sentiment = sentiment.strip().lower()
    
    # Проверяем базовые типы
    for base_sentiment, variants in ENHANCED_SENTIMENTS.items():
        if sentiment == base_sentiment or sentiment in variants:
            return base_sentiment
    
    return "нейтрально"

def validate_enhanced_extraction(extraction: Dict[str, Any]) -> Dict[str, Any]:
    """Валидация расширенного извлечения"""
    errors = []
    
    # Валидация барьеров
    if "barriers" in extraction:
        for barrier in extraction["barriers"]:
            if isinstance(barrier, dict):
                required_fields = ["category", "text", "severity", "context"]
                for field in required_fields:
                    if field not in barrier:
                        errors.append(f"Отсутствует поле {field} в барьере")
            elif isinstance(barrier, str):
                # Конвертируем старый формат в новый
                extraction["barriers"] = [{
                    "category": "общие",
                    "text": barrier,
                    "severity": "средняя",
                    "context": "выявлено автоматически"
                }]
    
    # Валидация идей
    if "ideas" in extraction:
        for idea in extraction["ideas"]:
            if isinstance(idea, dict):
                required_fields = ["category", "text", "feasibility", "impact"]
                for field in required_fields:
                    if field not in idea:
                        errors.append(f"Отсутствует поле {field} в идее")
            elif isinstance(idea, str):
                # Конвертируем старый формат в новый
                extraction["ideas"] = [{
                    "category": "общие",
                    "text": idea,
                    "feasibility": "средняя",
                    "impact": "средний"
                }]
    
    # Валидация сигналов
    if "signals" in extraction:
        for signal in extraction["signals"]:
            if isinstance(signal, dict):
                required_fields = ["type", "text", "confidence", "context"]
                for field in required_fields:
                    if field not in signal:
                        errors.append(f"Отсутствует поле {field} в сигнале")
            elif isinstance(signal, str):
                # Конвертируем старый формат в новый
                extraction["signals"] = [{
                    "type": "предпочтения",
                    "text": signal,
                    "confidence": "средняя",
                    "context": "выявлено автоматически"
                }]
    
    # Валидация контекстных полей
    if "emotional_state" in extraction:
        extraction["emotional_state"] = normalize_enhanced_sentiment(extraction["emotional_state"])
    
    if "expertise_level" in extraction and extraction["expertise_level"] not in EXPERTISE_LEVELS:
        extraction["expertise_level"] = "средний"
    
    if "urgency" in extraction and extraction["urgency"] not in URGENCY_LEVELS:
        extraction["urgency"] = "важно"
    
    if "decision_impact" in extraction and extraction["decision_impact"] not in DECISION_IMPACT:
        extraction["decision_impact"] = "влияет на выбор"
    
    if errors:
        logger.warning(f"Ошибки валидации: {errors}")
    
    return extraction

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def extract_entities_enhanced(dialog_text: str, dialog_id: str) -> Dict[str, Any]:
    """Улучшенное извлечение сущностей из диалога"""
    
    try:
        # Загружаем улучшенный промпт
        prompt = load_enhanced_prompt()
        
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
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if not json_match:
            raise ValueError("JSON не найден в ответе")
        
        extraction = json.loads(json_match.group())
        
        # Валидируем и нормализуем
        extraction = validate_enhanced_extraction(extraction)
        
        # Добавляем метаданные
        extraction["dialog_id"] = dialog_id
        extraction["extraction_timestamp"] = pd.Timestamp.now().isoformat()
        extraction["model_used"] = settings.model_extract
        
        return extraction
        
    except Exception as e:
        logger.error(f"Ошибка извлечения для диалога {dialog_id}: {e}")
        return {
            "dialog_id": dialog_id,
            "delivery_related": False,
            "barriers": [],
            "ideas": [],
            "signals": [],
            "delivery_types": [],
            "product_category": "",
            "sentiment": "нейтрально",
            "emotional_state": "нейтрально",
            "expertise_level": "средний",
            "urgency": "важно",
            "decision_impact": "не влияет",
            "citations": [],
            "error": str(e)
        }

def process_dialogs_enhanced(input_file: str, output_file: str):
    """Обработка диалогов с улучшенным извлечением"""
    
    logger.info("🚀 Stage 2 Enhanced: Улучшенное извлечение сущностей")
    
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
    
    logger.info(f"Диалогов с доставкой: {len(delivery_dialogs)}")
    
    if not delivery_dialogs:
        logger.warning("Нет диалогов с доставкой для обработки")
        return
    
    # Загружаем исходные диалоги для получения текста
    original_df = pd.read_excel("data/dialogs.xlsx", engine='openpyxl')
    
    results = []
    
    for dialog_data in tqdm(delivery_dialogs, desc="Извлечение сущностей"):
        dialog_id = dialog_data.get('dialog_id', 'unknown')
        
        # Находим исходный диалог по ID
        original_dialog = original_df[original_df['ID звонка'].astype(str) == dialog_id]
        if original_dialog.empty:
            logger.warning(f"Диалог {dialog_id} не найден в исходных данных")
            continue
            
        dialog_text = str(original_dialog.iloc[0]['Текст транскрибации'])
        
        if not dialog_text or len(dialog_text.strip()) < 10:
            logger.warning(f"Пропуск диалога {dialog_id}: пустой текст")
            continue
        
        # Извлекаем сущности
        extraction = extract_entities_enhanced(dialog_text, dialog_id)
        results.append(extraction)
    
    # Сохраняем результаты
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for result in results:
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
    
    logger.info(f"✅ Сохранено {len(results)} результатов в {output_file}")
    
    # Статистика
    total_barriers = sum(len(r.get('barriers', [])) for r in results)
    total_ideas = sum(len(r.get('ideas', [])) for r in results)
    total_signals = sum(len(r.get('signals', [])) for r in results)
    
    logger.info(f"📊 Статистика:")
    logger.info(f"  Барьеров: {total_barriers}")
    logger.info(f"  Идей: {total_ideas}")
    logger.info(f"  Сигналов: {total_signals}")
    
    return results

def main():
    """Основная функция"""
    input_file = "artifacts/stage1_delivery.jsonl"
    output_file = "artifacts/stage2_extracted_enhanced.jsonl"
    
    process_dialogs_enhanced(input_file, output_file)

if __name__ == "__main__":
    main()
