#!/usr/bin/env python3
"""
Stage 1: Детекция доставки
Определяет, обсуждалась ли доставка в каждом диалоге
"""

import json
import logging
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict, Any

import sys
sys.path.append(str(Path(__file__).parent.parent))

import openai
from config import settings
from models.validation import DeliveryDetection
from prompts import DELIVERY_DETECTION_PROMPT, STAGE_CONFIG

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка OpenAI
if settings.openai_api_key:
    openai.api_key = settings.openai_api_key


@retry(
    stop=stop_after_attempt(settings.max_retries),
    wait=wait_exponential(multiplier=settings.retry_backoff_sec)
)
def second_pass_determine(text: str) -> bool:
    """Второй проход для определения доставки в серой зоне"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        
        # Более строгий промпт для второго прохода
        strict_prompt = """
        Ты анализируешь диалог. Определи ТОЛЬКО если четко обсуждается доставка товаров.
        
        Критерии (все должны быть выполнены):
        1. Есть упоминание доставки, ПВЗ, курьера, самовывоза
        2. Обсуждается конкретный способ получения товара
        3. Есть вопросы или проблемы с получением заказа
        
        Если не уверен - отвечай false.
        
        Ответь в формате JSON:
        {"delivery_discussed": true/false, "p_deliv": 0.0-1.0}
        """
        
        response = client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": strict_prompt},
                {"role": "user", "content": f"Диалог:\n{text}"}
            ],
            temperature=0.0,  # Максимальная детерминированность
            max_tokens=100
        )
        
        content = response.choices[0].message.content
        
        if not content or not content.strip():
            return False
        
        # Очищаем JSON от markdown блоков
        cleaned_content = content.strip()
        if cleaned_content.startswith("```json"):
            cleaned_content = cleaned_content[7:]
        if cleaned_content.endswith("```"):
            cleaned_content = cleaned_content[:-3]
        cleaned_content = cleaned_content.strip()
        
        try:
            result = json.loads(cleaned_content)
            return result.get("delivery_discussed", False)
        except json.JSONDecodeError:
            return False
            
    except Exception as e:
        logger.error(f"Ошибка второго прохода: {e}")
        return False


def detect_delivery_openai(text: str) -> Dict[str, Any]:
    """Детекция доставки через OpenAI API с ужесточенным фильтром"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        
        response = client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": DELIVERY_DETECTION_PROMPT},
                {"role": "user", "content": f"Диалог:\n{text}"}
            ],
            temperature=0.1,
            max_tokens=200
        )
        
        content = response.choices[0].message.content
        
        # Проверяем, что ответ не пустой
        if not content or not content.strip():
            logger.warning(f"Пустой ответ от OpenAI для диалога")
            return {"delivery_discussed": False, "p_deliv": 0.0}
        
        # Очищаем JSON от markdown блоков
        cleaned_content = content.strip()
        if cleaned_content.startswith("```json"):
            cleaned_content = cleaned_content[7:]  # Убираем ```json
        if cleaned_content.endswith("```"):
            cleaned_content = cleaned_content[:-3]  # Убираем ```
        cleaned_content = cleaned_content.strip()
        
        # Пытаемся парсить JSON
        try:
            result = json.loads(cleaned_content)
            p_deliv = result.get("p_deliv", 0.0)
            THRESH = settings.delivery_conf_threshold
            
            # Ужесточенный фильтр
            if p_deliv >= THRESH:
                delivery_discussed = True
            elif p_deliv <= (1 - THRESH):
                delivery_discussed = False
            else:
                # Серая зона - второй проход
                delivery_discussed = second_pass_determine(text)
                p_deliv = 0.8 if delivery_discussed else 0.2
            
            return {
                "delivery_discussed": delivery_discussed,
                "p_deliv": p_deliv
            }
            
        except json.JSONDecodeError as json_err:
            logger.error(f"Ошибка парсинга JSON: {json_err}. Ответ: {cleaned_content[:200]}...")
            # Возвращаем дефолтный результат вместо падения
            return {"delivery_discussed": False, "p_deliv": 0.0}
    except Exception as e:
        logger.error(f"Ошибка OpenAI API: {e}")
        # Возвращаем дефолтный результат вместо падения
        return {"delivery_discussed": False, "p_deliv": 0.0}


def detect_delivery_simple(text: str) -> Dict[str, Any]:
    """Простая детекция доставки по ключевым словам"""
    delivery_keywords = [
        "доставка", "пвз", "пункт выдачи", "курьер", "отправить", "привезти",
        "забрать", "логистика", "отправка", "постамат", "самовывоз"
    ]
    
    text_lower = text.lower()
    mentions = sum(1 for keyword in delivery_keywords if keyword in text_lower)
    
    delivery_discussed = mentions > 0
    p_deliv = min(mentions / 5.0, 1.0)  # Простая эвристика
    
    return {
        "delivery_discussed": delivery_discussed,
        "p_deliv": p_deliv
    }


def process_dialog_batch(dialogs: List[Dict[str, Any]]) -> List[DeliveryDetection]:
    """Обработка батча диалогов"""
    results = []
    
    for dialog in dialogs:
        try:
            dialog_id = str(dialog[settings.col_id])
            text = str(dialog[settings.col_text])
            
            # Детекция доставки
            if settings.use_openai and settings.openai_api_key:
                detection = detect_delivery_openai(text)
            else:
                detection = detect_delivery_simple(text)
            
            # Создание результата
            result = DeliveryDetection(
                dialog_id=dialog_id,
                delivery_discussed=detection["delivery_discussed"],
                p_deliv=detection["p_deliv"]
            )
            
            results.append(result)
            
        except Exception as e:
            logger.error(f"Ошибка обработки диалога {dialog.get(settings.col_id, 'unknown')}: {e}")
            continue
    
    return results


def main():
    """Главная функция Stage 1"""
    logger.info("🚀 Stage 1: Детекция доставки")
    
    # Создание папки artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Загрузка данных
    logger.info(f"📁 Загрузка данных из {settings.xlsx_path}")
    try:
        df = pd.read_excel(settings.xlsx_path)
        logger.info(f"✅ Загружено {len(df)} диалогов")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        return
    
    # Обработка батчами
    results = []
    total_dialogs = len(df)
    
    logger.info(f"🔄 Обработка {total_dialogs} диалогов батчами по {settings.batch_size}")
    
    for i in tqdm(range(0, total_dialogs, settings.batch_size), desc="Обработка батчей"):
        batch_df = df.iloc[i:i + settings.batch_size]
        batch_dialogs = batch_df.to_dict('records')
        
        batch_results = process_dialog_batch(batch_dialogs)
        results.extend(batch_results)
    
    # Сохранение результатов
    output_file = artifacts_dir / "stage1_delivery.jsonl"
    logger.info(f"💾 Сохранение результатов в {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for result in results:
            f.write(json.dumps(result.dict()) + '\n')
    
    # Статистика
    delivery_count = sum(1 for r in results if r.delivery_discussed)
    avg_confidence = sum(r.p_deliv for r in results) / len(results) if results else 0
    delivery_percentage = (delivery_count / len(results) * 100) if results else 0
    
    logger.info("📊 Статистика Stage 1:")
    logger.info(f"  Всего диалогов: {len(results)}")
    logger.info(f"  С доставкой: {delivery_count} ({delivery_percentage:.1f}%)")
    logger.info(f"  Средняя уверенность: {avg_confidence:.3f}")
    
    logger.info("✅ Stage 1 завершен успешно!")


if __name__ == "__main__":
    main()
