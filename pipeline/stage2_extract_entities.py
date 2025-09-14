#!/usr/bin/env python3
"""
Stage 2: Извлечение сущностей
Извлекает барьеры, идеи, сигналы и другие поля для диалогов с доставкой
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
from prompts import ENTITY_EXTRACTION_PROMPT, STAGE_CONFIG, DELIVERY_KEYWORDS

# Жесткий словарь тональностей
ALLOWED_SENTIMENTS = {"раздражение", "нейтрально", "сомнение", "позитив"}

# Маппинг для нормализации тональности
SENTIMENT_MAP = {
    "негативное": "раздражение",
    "негатив": "раздражение", 
    "позитивное": "позитив",
    "позитив": "позитив",
    "нейтральное": "нейтрально",
    "сомнения": "сомнение"
}

def normalize_sentiment(sentiment: str) -> str:
    """Нормализация тональности к разрешенным значениям"""
    if not sentiment:
        return "нейтрально"
    
    # Приводим к нижнему регистру и убираем пробелы
    sentiment = sentiment.strip().lower()
    
    # Применяем маппинг
    normalized = SENTIMENT_MAP.get(sentiment, sentiment)
    
    # Проверяем, что результат в разрешенном списке
    if normalized not in ALLOWED_SENTIMENTS:
        return "нейтрально"
    
    return normalized


# Словарь категорий товаров
PRODUCT_CATS = [
    ("шины|резин[аы]|колес", "шины"),
    ("стройматериал|цемент|гипсокартон|кирпич|саморез", "стройматериалы"),
    ("диван|кровать|шкаф|стол|стул", "мебель"),
    ("смартфон|телефон|ноутбук|планшет|телевизор|наушник", "электроника"),
    ("запчаст", "автозапчасти"),
]

# Улучшенные регексы для цитат
ROLE_RX = re.compile(r"^(клиент|оператор)\s*:\s*(.*)$", re.IGNORECASE)

KW_FOR_QUOTES = re.compile(
    r"(пвз|пункт выдачи|постамат\w*|курьер\w*|самовывоз\w*|сдэк|boxberry|боксберри|"
    r"доставк\w+|онлайн оплат\w+|возврат\s*24|субсидирован\w+|трек(?:-?номер)?)",
    re.IGNORECASE
)

# Регексы для определения барьеров
QUESTION_RE = re.compile(r"(как|что|куда|когда)\b.*\?$", re.IGNORECASE)


def split_turns(raw: str):
    """Устойчивый разбор диалога на реплики"""
    turns = []
    for line in raw.splitlines():
        line = line.strip()
        if not line: 
            continue
        m = ROLE_RX.match(line)
        if m:
            turns.append({"speaker": m.group(1).lower(), "text": m.group(2).strip()})
        elif turns:
            turns[-1]["text"] += " " + line
    return turns


def sanitize_quote(s: str) -> str:
    """Маскирование PII в цитатах"""
    s = re.sub(r'\b\d{10,11}\b','[masked-phone]', s)
    s = re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}','[masked-email]', s)
    return s.strip()


# Регексы для парсинга ролей и чистки цитат
ROLE_RX = re.compile(r"^(клиент|оператор)\s*:\s*(.*)$", re.IGNORECASE)
FILLERS = r"^(угу|ага|ну|вот|а|мм|э|ээ|так|короче|типа|как бы|просто)\b"
MULTISPACE_RX = re.compile(r"\s+")
REPEAT_RX = re.compile(r"\b(\w+)(\s+\1\b)+", re.IGNORECASE)  # «доставка доставка» → «доставка»


def split_turns(raw: str):
    """Парсинг диалога на реплики с ролями"""
    turns = []
    for line in raw.splitlines():
        s = line.strip()
        if not s: 
            continue
        m = ROLE_RX.match(s)
        if m:
            turns.append({"speaker": m.group(1).lower(), "text": m.group(2).strip()})
        elif turns:
            turns[-1]["text"] += " " + s
    return turns


def clean_sentence(s: str, max_len=180) -> str:
    """Очистка предложения от мусора с ограничением длины"""
    s = s.strip()
    # убрать стартовые междометия по нескольку раз
    s = re.sub(rf"(?:{FILLERS}[\s,–—-]*)+", "", s, flags=re.IGNORECASE)
    # схлопнуть повторы слов
    s = REPEAT_RX.sub(r"\1", s)
    # нормализовать пробелы и пунктуацию
    s = MULTISPACE_RX.sub(" ", s)
    # убрать точки и запятые в начале
    s = re.sub(r"^[.,\s]+", "", s)
    # укоротить очень длинные
    if len(s) > max_len:
        s = s[:max_len-1].rstrip() + "…"
    return s


def split_to_sentences(text: str) -> list[str]:
    """Грубая русская сегментация по .!? + переносам"""
    parts = re.split(r"(?<=[\.\?\!])\s+|\n+", text)
    return [p.strip() for p in parts if p.strip()]


def postprocess_quotes(quotes: list[dict], limit=3) -> list[dict]:
    """Постобработка цитат: чистка, дедупликация, сегментация"""
    out = []
    seen = set()
    for q in quotes:
        for sent in split_to_sentences(q["quote"]):
            sent = clean_sentence(sent)
            if len(sent) < 8:
                continue
            key = sent.lower()
            if key in seen:
                continue  # дедуп
            seen.add(key)
            out.append({"quote": sent, "speaker": "Клиент"})
            if len(out) >= limit:
                return out
    return out


def mark_source_role(items, speaker):
    """Разметка роли источника для элементов"""
    for it in items or []:
        it["source_role"] = "client" if speaker == "клиент" else "operator"


def pick_client_quotes(turns, limit=3):
    """Гарантированный отбор цитат клиента с улучшенной чисткой"""
    raw = []
    # 1) клиентские с доставочными терминами
    for t in turns:
        if t["speaker"] != "клиент": 
            continue
        tx = t["text"].strip()
        if KW_FOR_QUOTES.search(tx) and len(tx) >= 8:
            raw.append(mask_pii(tx))
    
    # 2) сегментация на предложения + очистка + дедуп
    out, seen = set(), []
    for q in raw:
        for sent in split_to_sentences(q):
            sent = clean_sentence(sent)
            if len(sent) < 10: 
                continue
            key = sent.lower()
            if key in out: 
                continue
            out.add(key)
            seen.append({"quote": sent, "speaker": "Клиент"})
            if len(seen) >= limit: 
                return seen
    return seen


def guess_product_category(text: str) -> str | None:
    """Определение категории товара по тексту диалога"""
    import re
    t = text.lower()
    for rx, cat in PRODUCT_CATS:
        if re.search(rx, t):
            return cat
    return None

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка OpenAI
if settings.openai_api_key:
    openai.api_key = settings.openai_api_key


def clean_quote(q: str) -> str:
    """Очистка цитаты от PII"""
    import re
    q = q.strip()
    # Простая защита PII
    q = re.sub(r'\b\d{11}\b', '[masked-phone]', q)
    q = re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', '[masked-email]', q)
    return q


def pick_client_quotes(turns, limit=3):
    """Извлечение цитат клиента с улучшенной логикой"""
    import re
    
    # Ключевые слова для поиска цитат
    KW_FOR_QUOTES = re.compile(
        r"(пвз|пункт выдачи|постамат\w*|курьер\w*|самовывоз\w*|доставк\w+|возврат 24|субсидирован\w+|онлайн оплат\w+)", 
        re.IGNORECASE
    )
    
    def sanitize_quote(s: str) -> str:
        s = re.sub(r'\b\d{10,11}\b', '[masked-phone]', s)
        s = re.sub(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', '[masked-email]', s)
        return s.strip()
    
    out = []
    
    # 1) Клиентские реплики с доставочными словами
    for t in turns:
        if t.get("speaker", "").lower() != "клиент":
            continue
        tx = t.get("text", "").strip()
        if KW_FOR_QUOTES.search(tx) and 8 <= len(tx) <= 280:
            out.append({"quote": sanitize_quote(tx), "speaker": "Клиент"})
            if len(out) >= limit:
                return out
    
    # 2) Соседние к операторским доставочным репликам
    for i, t in enumerate(turns):
        if t.get("speaker", "").lower() == "оператор" and KW_FOR_QUOTES.search(t.get("text", "")):
            for j in (i-1, i+1):
                if 0 <= j < len(turns) and turns[j].get("speaker", "").lower() == "клиент":
                    tx = turns[j].get("text", "").strip()
                    if 8 <= len(tx) <= 280:
                        out.append({"quote": sanitize_quote(tx), "speaker": "Клиент"})
                        if len(out) >= limit:
                            return out
    
    return out


def has_delivery_cue(citations):
    """Проверка наличия ключевых слов доставки в цитатах"""
    for citation in citations:
        quote_lower = citation.get("quote", "").lower()
        if any(keyword in quote_lower for keyword in DELIVERY_KEYWORDS):
            return True
    return False


@retry(
    stop=stop_after_attempt(settings.max_retries),
    wait=wait_exponential(multiplier=settings.retry_backoff_sec)
)
def extract_entities_openai(text: str, dialog_id: str) -> Dict[str, Any]:
    """Извлечение сущностей через OpenAI API с реальными цитатами клиента"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        
        # Парсим диалог для извлечения цитат клиента
        try:
            turns = split_turns(text)
            client_quotes = pick_client_quotes(turns, limit=3)
            low_evidence = (len(client_quotes) == 0)
        except:
            client_quotes = []
            low_evidence = True
        
        # Загружаем обновленный промпт
        with open("prompts/extract_entities.txt", "r", encoding="utf-8") as f:
            updated_prompt = f.read()
        
        # Параметры для разных моделей
        params = {
            "model": settings.model_extract,
            "messages": [
                {"role": "system", "content": updated_prompt},
                {"role": "user", "content": f"Проанализируй этот диалог:\n\n{text}"}
            ],
            "response_format": {"type": "json_object"},
            "max_completion_tokens": 1000
        }
        
        # Добавляем параметры только для поддерживающих моделей
        if "o3" not in settings.model_extract:
            params.update({
                "temperature": 0.2,
                "top_p": 0.9
            })
        
        try:
            response = client.chat.completions.create(**params)
        except Exception as e:
            # Fallback на gpt-4o-mini если o3-mini не работает
            if "o3" in settings.model_extract:
                params["model"] = "gpt-4o-mini"
                params.update({
                    "temperature": 0.2,
                    "top_p": 0.9
                })
                response = client.chat.completions.create(**params)
            else:
                raise e
        
        content = response.choices[0].message.content
        
        # Проверяем, что ответ не пустой
        if not content or not content.strip():
            logger.warning(f"Пустой ответ от OpenAI для диалога {dialog_id}")
            return {
                "delivery_types": [],
                "barriers": [],
                "ideas": [],
                "signals": [],
                "citations": client_quotes
            }
        
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
            
            # Заменяем сгенерированные цитаты на реальные
            result["citations"] = client_quotes
            result["low_evidence"] = low_evidence
            
            # Нормализуем тональность
            if "sentiment" in result:
                result["sentiment"] = normalize_sentiment(result["sentiment"])
            
            # Проверяем барьеры на наличие ключевых слов доставки
            if result.get("delivery_discussed") is True:
                has_delivery_cue_in_citations = has_delivery_cue(client_quotes)
                if not has_delivery_cue_in_citations and result.get("barriers"):
                    # Если нет явной клиентской реплики про доставку - убираем барьеры
                    result["barriers"] = []
            
            return result
        except json.JSONDecodeError as json_err:
            logger.error(f"Ошибка парсинга JSON для диалога {dialog_id}: {json_err}. Ответ: {cleaned_content[:200]}...")
            # Возвращаем дефолтный результат вместо падения
            return {
                "delivery_types": [],
                "barriers": [],
                "ideas": [],
                "signals": [],
                "citations": client_quotes
            }
    except Exception as e:
        logger.error(f"Ошибка OpenAI API для диалога {dialog_id}: {e}")
        # Возвращаем дефолтный результат вместо падения
        return {
            "delivery_types": [],
            "barriers": [],
            "ideas": [],
            "signals": [],
            "citations": []
        }


def extract_entities_simple(text: str, dialog_id: str) -> Dict[str, Any]:
    """Простое извлечение сущностей по ключевым словам"""
    text_lower = text.lower()
    
    # Простые эвристики для извлечения
    barriers = []
    ideas = []
    signals = []
    
    # Барьеры
    if "мало пвз" in text_lower or "нет пункта" in text_lower:
        barriers.append("мало пвз")
    if "дорого" in text_lower or "дорогая доставка" in text_lower:
        barriers.append("дорогая доставка")
    if "не поднимается" in text_lower or "не доходит" in text_lower:
        barriers.append("курьер не поднимается")
    
    # Идеи
    if "скидка" in text_lower or "дешевле" in text_lower:
        ideas.append("скидка на доставку")
    if "бесплатная доставка" in text_lower:
        ideas.append("бесплатная доставка")
    
    # Сигналы
    if "не понимаю" in text_lower or "не разобрался" in text_lower:
        signals.append("незнание")
    if "сложно" in text_lower or "запутанно" in text_lower:
        signals.append("сложность")
    
    # Типы доставки
    delivery_types = []
    if "пвз" in text_lower or "пункт выдачи" in text_lower:
        delivery_types.append("ПВЗ")
    if "курьер" in text_lower:
        delivery_types.append("курьерская")
    if "самовывоз" in text_lower or "заберу сам" in text_lower:
        delivery_types.append("самовывоз")
    
    return {
        "dialog_id": dialog_id,
        "delivery_discussed": True,
        "delivery_types": delivery_types,
        "barriers": barriers,
        "ideas": ideas,
        "signals": signals,
        "citations": [],
        "region": "",
        "segment": "",
        "product_category": "",
        "sentiment": "",
        "extras": {}
    }


def process_dialog_batch(dialogs: List[Dict[str, Any]], delivery_dialog_ids: Set[str]) -> List[DialogExtraction]:
    """Обработка батча диалогов"""
    results = []
    
    for dialog in dialogs:
        try:
            dialog_id = str(dialog[settings.col_id])
            
            # Пропускаем диалоги без доставки
            if dialog_id not in delivery_dialog_ids:
                continue
            
            text = str(dialog[settings.col_text])
            
            # Извлечение сущностей
            if settings.use_openai and settings.openai_api_key:
                extraction = extract_entities_openai(text, dialog_id)
            else:
                extraction = extract_entities_simple(text, dialog_id)
            
            # Валидация и создание результата
            try:
                # Извлечение цитат клиента
                turns = split_turns(text)
                citations = pick_client_quotes(turns, limit=3)
                
                # Определяем low_evidence
                low_evidence = len(citations) == 0
                
                # Валидация и преобразование типов для полей List[str]
                def ensure_string_list(value, field_name):
                    """Преобразует значение в список строк"""
                    if not isinstance(value, list):
                        logger.warning(f"Поле {field_name} не является списком: {type(value)}")
                        return []
                    
                    result = []
                    for item in value:
                        if isinstance(item, str):
                            result.append(item)
                        elif isinstance(item, (int, float)):
                            # Преобразуем числа в строки
                            result.append(str(item))
                        elif isinstance(item, dict):
                            # Извлекаем текст из словаря (если есть поле 'text' или 'name')
                            text = item.get('text') or item.get('name') or str(item)
                            result.append(str(text))
                        else:
                            logger.warning(f"Неожиданный тип в {field_name}: {type(item)}")
                            result.append(str(item))
                    
                    return result
                
                # Определяем категорию товара, если не указана
                product_category = extraction.get("product_category", "")
                if not product_category:
                    product_category = guess_product_category(text) or ""
                
                result = DialogExtraction(
                    dialog_id=dialog_id,
                    delivery_discussed=extraction.get("delivery_discussed", True),
                    delivery_types=ensure_string_list(extraction.get("delivery_types", []), "delivery_types"),
                    barriers=ensure_string_list(extraction.get("barriers", []), "barriers"),
                    ideas=ensure_string_list(extraction.get("ideas", []), "ideas"),
                    signals=ensure_string_list(extraction.get("signals", []), "signals"),
                    citations=citations,
                    region=extraction.get("region", ""),
                    segment=extraction.get("segment", ""),
                    product_category=product_category,
                    sentiment=extraction.get("sentiment", ""),
                    extras=extraction.get("extras", {})
                )
                
                # Добавляем low_evidence в extras
                result.extras["low_evidence"] = low_evidence
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Ошибка валидации диалога {dialog_id}: {e}")
                continue
            
        except Exception as e:
            logger.error(f"Ошибка обработки диалога {dialog.get(settings.col_id, 'unknown')}: {e}")
            continue
    
    return results


def load_delivery_detections() -> Set[str]:
    """Загрузка ID диалогов с доставкой из Stage 1"""
    delivery_file = Path("artifacts/stage1_delivery.jsonl")
    
    if not delivery_file.exists():
        logger.error(f"❌ Файл {delivery_file} не найден. Запустите Stage 1 сначала.")
        return set()
    
    delivery_ids = set()
    
    with open(delivery_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                if data.get("delivery_discussed", False):
                    delivery_ids.add(data["dialog_id"])
            except Exception as e:
                logger.error(f"Ошибка чтения строки: {e}")
                continue
    
    logger.info(f"📊 Найдено {len(delivery_ids)} диалогов с доставкой")
    return delivery_ids


def load_valid_samples() -> Set[str]:
    """Загрузка валидных образцов из Stage 1.5"""
    # Сначала пытаемся загрузить из Stage 1.5
    sampling_file = Path("artifacts/stage1_5_sampling.jsonl")
    
    if sampling_file.exists():
        logger.info("📁 Загрузка валидных образцов из Stage 1.5...")
        valid_ids = set()
        
        with open(sampling_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if data.get("valid_sample", False):
                        valid_ids.add(data["dialog_id"])
                except Exception as e:
                    logger.error(f"Ошибка чтения образца: {e}")
                    continue
        
        logger.info(f"📊 Найдено {len(valid_ids)} валидных образцов")
        
        # Если Stage 1.5 не дал валидных образцов, используем Stage 1
        if len(valid_ids) == 0:
            logger.warning("⚠️ Stage 1.5 не дал валидных образцов, используем Stage 1...")
            return load_delivery_detections()
        
        return valid_ids
    
    # Fallback на Stage 1
    logger.warning("⚠️ Stage 1.5 не найден, используем Stage 1...")
    delivery_file = Path("artifacts/stage1_delivery.jsonl")
    
    if not delivery_file.exists():
        logger.error(f"❌ Файл {delivery_file} не найден. Запустите Stage 1 сначала.")
        return set()
    
    delivery_ids = set()
    
    with open(delivery_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                if data.get("delivery_discussed", False):
                    delivery_ids.add(data["dialog_id"])
            except Exception as e:
                logger.error(f"Ошибка чтения строки: {e}")
                continue
    
    return delivery_ids


def main():
    """Главная функция Stage 2"""
    logger.info("🚀 Stage 2: Извлечение сущностей")
    
    # Создание папки artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Загрузка валидных образцов
    valid_dialog_ids = load_valid_samples()
    logger.info(f"📊 Найдено {len(valid_dialog_ids)} валидных диалогов")
    
    if not valid_dialog_ids:
        logger.error("❌ Нет валидных диалогов для обработки")
        return
    
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
    
    logger.info(f"🔄 Обработка диалогов с доставкой батчами по {settings.batch_size}")
    
    for i in tqdm(range(0, total_dialogs, settings.batch_size), desc="Обработка батчей"):
        batch_df = df.iloc[i:i + settings.batch_size]
        batch_dialogs = batch_df.to_dict('records')
        
        batch_results = process_dialog_batch(batch_dialogs, valid_dialog_ids)
        results.extend(batch_results)
    
    # Сохранение результатов
    output_file = artifacts_dir / "stage2_extracted.jsonl"
    logger.info(f"💾 Сохранение результатов в {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for result in results:
            f.write(json.dumps(result.dict()) + '\n')
    
    # Статистика
    total_barriers = sum(len(r.barriers) for r in results)
    total_ideas = sum(len(r.ideas) for r in results)
    total_signals = sum(len(r.signals) for r in results)
    total_citations = sum(len(r.citations) for r in results)
    
    logger.info("📊 Статистика Stage 2:")
    logger.info(f"  Обработано диалогов: {len(results)}")
    logger.info(f"  Всего барьеров: {total_barriers}")
    logger.info(f"  Всего идей: {total_ideas}")
    logger.info(f"  Всего сигналов: {total_signals}")
    logger.info(f"  Всего цитат: {total_citations}")
    
    logger.info("✅ Stage 2 завершен успешно!")


if __name__ == "__main__":
    main()
