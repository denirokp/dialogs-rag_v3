#!/usr/bin/env python3
"""
Stage 1.5: Фильтрация образцов диалогов
Детальная проверка валидности диалогов с обоснованным скорингом
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from tqdm import tqdm

import sys
sys.path.append(str(Path(__file__).parent.parent))

from filters.regexes import DELIVERY_ANY, PLATFORM_NOISE, BARRIER_MARK, IDEA_MARK, YESNO
from utils.turns import split_turns
from models.validation import DeliveryDetection

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
MIN_DURATION_SEC = 180


def extract_duration(dialog: Dict[str, Any]) -> int:
    """Извлечение длительности диалога в секундах"""
    duration_str = dialog.get("duration", "")
    if not duration_str:
        return 0
    
    # Парсинг формата "MM:SS" или "HH:MM:SS"
    try:
        parts = duration_str.split(":")
        if len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
        elif len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
    except (ValueError, IndexError):
        pass
    
    return 0


def is_valid_delivery_dialog(text: str, turns: list, duration_sec: int) -> Tuple[bool, str]:
    """
    Проверка валидности диалога о доставке
    """
    # 1. Проверка длительности
    if duration_sec and duration_sec < MIN_DURATION_SEC:
        return False, "short_call"
    
    # 2. Анализ инициативы и содержательности у клиента
    client_kw_hits = []
    client_markers = []
    first_initiator = "unknown"
    delivered_seen = False

    for i, t in enumerate(turns):
        sp, tx = t["speaker"], t["text"]
        
        # Проверка инициативы оператора
        if sp == "оператор" and not delivered_seen and DELIVERY_ANY.search(tx):
            first_initiator = "operator"
        
        # Проверка ключевых слов у клиента
        if sp == "клиент" and DELIVERY_ANY.search(tx):
            if not delivered_seen:
                first_initiator = "client"
                delivered_seen = True
            client_kw_hits.append((i, DELIVERY_ANY.search(tx).group(0)))
        
        # Проверка маркеров у клиента
        if sp == "клиент":
            barrier_match = BARRIER_MARK.search(tx)
            idea_match = IDEA_MARK.search(tx)
            if barrier_match:
                client_markers.append((i, barrier_match.group(0)))
            elif idea_match:
                client_markers.append((i, idea_match.group(0)))

    # 3. Проверка наличия ключевых слов у клиента
    if not client_kw_hits:
        return False, "no_client_kw"
    
    # 4. Проверка наличия маркеров у клиента
    if not client_markers:
        return False, "no_marker"

    # 5. Проверка платформенного шума без доставочных терминов у клиента
    if PLATFORM_NOISE.search(text) and not client_kw_hits:
        return False, "platform_noise"

    # 6. Проверка односложных ответов при инициативе оператора
    if first_initiator == "operator":
        client_only_yesno = all(YESNO.match(t["text"]) for t in turns if t["speaker"] == "клиент")
        if client_only_yesno:
            return False, "operator_initiated_yesno"

    return True, "ok"


def analyze_dialog(dialog: Dict[str, Any]) -> Dict[str, Any]:
    """
    Анализ диалога с детальным скорингом
    """
    dialog_id = str(dialog.get("ID звонка", "unknown"))
    text = dialog.get("Текст транскрибации", "")
    
    # Извлечение длительности
    duration_sec = extract_duration(dialog)
    
    # Разбор диалога на реплики
    turns = split_turns(text)
    
    # Проверка валидности
    is_valid, reason = is_valid_delivery_dialog(text, turns, duration_sec)
    
    # Подсчет метрик для валидных диалогов
    if is_valid:
        client_kw_hits = []
        client_markers = []
        first_initiator = "unknown"
        delivered_seen = False

        for i, t in enumerate(turns):
            sp, tx = t["speaker"], t["text"]
            
            if sp == "оператор" and not delivered_seen and DELIVERY_ANY.search(tx):
                first_initiator = "operator"
            
            if sp == "клиент" and DELIVERY_ANY.search(tx):
                if not delivered_seen:
                    first_initiator = "client"
                    delivered_seen = True
                client_kw_hits.append((i, DELIVERY_ANY.search(tx).group(0)))
            
            if sp == "клиент":
                barrier_match = BARRIER_MARK.search(tx)
                idea_match = IDEA_MARK.search(tx)
                if barrier_match:
                    client_markers.append((i, barrier_match.group(0)))
                elif idea_match:
                    client_markers.append((i, idea_match.group(0)))

        # Подсчет уникальных ключевых слов
        client_kw_unique = set(match[1].lower() for match in client_kw_hits)
        
        return {
            "dialog_id": dialog_id,
            "valid_sample": True,
            "reason": reason,
            "duration_sec": duration_sec,
            "first_delivery_initiator": first_initiator,
            "client_kw_hits_total": len(client_kw_hits),
            "client_kw_unique": list(client_kw_unique),
            "client_marker_hits_total": len(client_markers),
            "client_kw_examples": [match[1] for match in client_kw_hits[:3]],
            "client_marker_examples": [match[1] for match in client_markers[:3]],
        }
    else:
        return {
            "dialog_id": dialog_id,
            "valid_sample": False,
            "reason": reason,
            "duration_sec": duration_sec,
            "first_delivery_initiator": "unknown",
            "client_kw_hits_total": 0,
            "client_kw_unique": [],
            "client_marker_hits_total": 0,
            "client_kw_examples": [],
            "client_marker_examples": [],
        }


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


def load_dialogs() -> List[Dict[str, Any]]:
    """Загрузка диалогов из Excel"""
    import pandas as pd
    
    dialogs_file = Path("data/dialogs.xlsx")
    if not dialogs_file.exists():
        logger.error(f"❌ Файл {dialogs_file} не найден")
        return []
    
    try:
        df = pd.read_excel(dialogs_file)
        dialogs = df.to_dict('records')
        logger.info(f"✅ Загружено {len(dialogs)} диалогов")
        return dialogs
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки диалогов: {e}")
        return []


def main():
    """Основная функция Stage 1.5"""
    logger.info("🚀 Stage 1.5: Фильтрация образцов диалогов")
    
    # Загрузка результатов Stage 1
    logger.info("📁 Загрузка результатов Stage 1...")
    detections = load_delivery_detections()
    
    if not detections:
        logger.error("❌ Нет результатов Stage 1")
        return
    
    # Фильтрация диалогов с доставкой
    delivery_dialog_ids = {d.dialog_id for d in detections if d.delivery_discussed}
    logger.info(f"📊 Найдено {len(delivery_dialog_ids)} диалогов с доставкой")
    
    # Загрузка диалогов
    logger.info("📁 Загрузка данных из data/dialogs.xlsx...")
    dialogs = load_dialogs()
    
    if not dialogs:
        logger.error("❌ Нет данных диалогов")
        return
    
    # Фильтрация диалогов с доставкой
    delivery_dialogs = [d for d in dialogs if str(d.get("ID звонка", "")) in delivery_dialog_ids]
    logger.info(f"📊 Найдено {len(delivery_dialogs)} диалогов с доставкой")
    
    # Отладочная информация
    if len(delivery_dialogs) == 0:
        logger.warning("⚠️ Не найдено диалогов с доставкой. Проверяем ID...")
        sample_dialog_ids = [str(d.get("ID звонка", "")) for d in dialogs[:3]]
        sample_delivery_ids = list(delivery_dialog_ids)[:3]
        logger.warning(f"  Примеры ID из Excel: {sample_dialog_ids}")
        logger.warning(f"  Примеры ID из Stage 1: {sample_delivery_ids}")
    
    # Анализ диалогов
    logger.info("🔍 Анализ диалогов...")
    results = []
    
    for dialog in tqdm(delivery_dialogs, desc="Анализ диалогов"):
        result = analyze_dialog(dialog)
        results.append(result)
    
    # Статистика
    valid_count = sum(1 for r in results if r["valid_sample"])
    invalid_count = len(results) - valid_count
    
    logger.info(f"📊 Статистика фильтрации:")
    logger.info(f"  Валидных диалогов: {valid_count}")
    logger.info(f"  Невалидных диалогов: {invalid_count}")
    if results:
        logger.info(f"  Процент валидных: {100.0 * valid_count / len(results):.1f}%")
    else:
        logger.info(f"  Процент валидных: 0.0%")
    
    # Анализ причин отклонения
    reasons = {}
    for result in results:
        if not result["valid_sample"]:
            reason = result["reason"]
            reasons[reason] = reasons.get(reason, 0) + 1
    
    if reasons:
        logger.info(f"📊 Причины отклонения:")
        for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {reason}: {count}")
    
    # Сохранение результатов
    output_file = Path("artifacts/stage1_5_sampling.jsonl")
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for result in results:
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
    
    logger.info(f"💾 Результаты сохранены в {output_file}")
    logger.info("✅ Stage 1.5 завершен успешно!")


if __name__ == "__main__":
    main()