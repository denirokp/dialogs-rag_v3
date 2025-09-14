#!/usr/bin/env python3
"""
Stage 3: Нормализация формулировок
Приводит барьеры и идеи к каноническим формулировкам
"""

import json
import logging
import re
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from models.validation import DialogExtraction, Citation
from prompts import NORMALIZATION_MAP

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def norm_text(s: str) -> str:
    """Нормализация текста для дедупликации"""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("ё", "е")
    return s


def dedupe_variants(variants: List[str]) -> List[str]:
    """Дедупликация вариантов с нормализацией"""
    seen = set()
    out = []
    for v in variants:
        nv = norm_text(v)
        if nv and nv not in seen:
            seen.add(nv)
            out.append(v)  # Сохраняем оригинальный текст
    return out


# Загружаем канонический словарь типов доставки
import json
import re

with open("canon/delivery_types_synonyms.json", "r", encoding="utf-8") as f:
    DELIVERY_CANON = json.load(f)


def canon_delivery(s: str) -> str:
    """Нормализация типов доставки к каноническим формам"""
    t = s.strip().lower().replace("ё", "е")
    for canon, syns in DELIVERY_CANON.items():
        for syn in syns + [canon]:
            ss = syn.lower().replace("ё", "е")
            if t == ss:
                return canon
    return s.strip()

# Словарь нормализации из prompts.py
NORMALIZE_RULES = {
    # Барьеры
    "barriers": {
        pattern: replacement for pattern, replacement in NORMALIZATION_MAP
    },
    
    # Идеи
    "ideas": {
        r"(скидка|дешевле|снизить цену)": "скидка на доставку",
        r"(бесплатная доставка|бесплатно)": "бесплатная доставка",
        r"(быстрее|ускорить|экспресс)": "быстрая доставка",
        r"(больше пвз|добавить пункты)": "больше пвз",
        r"(удобное время|выбор времени)": "удобное время доставки",
        r"(уведомления|смс|звонок)": "уведомления о доставке",
        r"(отслеживание|трек|статус)": "отслеживание заказа",
        r"(оплата при получении|наложенный платеж)": "оплата при получении",
        r"(примерка|проверка|осмотр)": "примерка перед покупкой",
        r"(возврат|обмен|гарантия)": "легкий возврат"
    },
    
    # Сигналы
    "signals": {
        r"(не понимаю|не разобрался|запутанно)": "незнание",
        r"(сложно|сложная система|запутанно)": "сложность",
        r"(сравниваю|сравнение|лучше)": "сравнение",
        r"(сомневаюсь|не уверен|может быть)": "сомнение",
        r"(интересно|интересуюсь|узнать)": "интерес",
        r"(жалуюсь|жалоба|недоволен)": "жалоба",
        r"(хвалю|хорошо|отлично)": "похвала",
        r"(предлагаю|идея|можно)": "предложение"
    }
}


def normalize_text(text: str, category: str) -> str:
    """Нормализация текста по категории"""
    if category not in NORMALIZE_RULES:
        return text
    
    text_lower = text.lower().strip()
    
    for pattern, replacement in NORMALIZE_RULES[category].items():
        if re.search(pattern, text_lower):
            return replacement
    
    return text


def normalize_phrase(text: str) -> str:
    """Нормализация фразы с использованием NORMALIZATION_MAP"""
    t = text.strip().lower()
    for rx, repl in NORMALIZATION_MAP:
        if re.search(rx, t, re.IGNORECASE):
            t = repl
            break
    return t


def normalize_dialog(dialog: DialogExtraction) -> DialogExtraction:
    """Нормализация диалога"""
    # Нормализация барьеров
    normalized_barriers = []
    for barrier in dialog.barriers:
        normalized = normalize_text(barrier, "barriers")
        if normalized not in normalized_barriers:
            normalized_barriers.append(normalized)
    
    # Нормализация идей
    normalized_ideas = []
    for idea in dialog.ideas:
        normalized = normalize_text(idea, "ideas")
        if normalized not in normalized_ideas:
            normalized_ideas.append(normalized)
    
    # Нормализация сигналов
    normalized_signals = []
    for signal in dialog.signals:
        normalized = normalize_text(signal, "signals")
        if normalized not in normalized_signals:
            normalized_signals.append(normalized)
    
    # Дедупликация вариантов
    normalized_barriers = dedupe_variants(normalized_barriers)
    normalized_ideas = dedupe_variants(normalized_ideas)
    normalized_signals = dedupe_variants(normalized_signals)
    
    # Нормализация типов доставки
    normalized_delivery_types = [canon_delivery(dt) for dt in dialog.delivery_types]
    
    # Per-dialog подсчеты (уникальные типы)
    types = set(canon_delivery(x) for x in dialog.delivery_types)
    # Не добавляем в объект, так как это не определено в модели
    normalized_delivery_types = list(set(normalized_delivery_types))  # убираем дубликаты
    
    # Создание нормализованного диалога
    return DialogExtraction(
        dialog_id=dialog.dialog_id,
        delivery_discussed=dialog.delivery_discussed,
        delivery_types=normalized_delivery_types,
        barriers=normalized_barriers,
        ideas=normalized_ideas,
        signals=normalized_signals,
        citations=dialog.citations,
        region=dialog.region,
        segment=dialog.segment,
        product_category=dialog.product_category,
        sentiment=dialog.sentiment,
        extras=dialog.extras
    )


def load_extracted_dialogs() -> List[DialogExtraction]:
    """Загрузка извлеченных диалогов"""
    extracted_file = Path("artifacts/stage2_extracted.jsonl")
    
    if not extracted_file.exists():
        logger.error(f"❌ Файл {extracted_file} не найден. Запустите Stage 2 сначала.")
        return []
    
    dialogs = []
    
    with open(extracted_file, 'r', encoding='utf-8') as f:
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
    """Главная функция Stage 3"""
    logger.info("🚀 Stage 3: Нормализация формулировок")
    
    # Создание папки artifacts
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    
    # Загрузка извлеченных диалогов
    dialogs = load_extracted_dialogs()
    logger.info(f"📊 Загружено {len(dialogs)} диалогов для нормализации")
    
    if not dialogs:
        logger.error("❌ Нет диалогов для нормализации")
        return
    
    # Нормализация
    logger.info("🔄 Нормализация формулировок...")
    normalized_dialogs = []
    
    for dialog in tqdm(dialogs, desc="Нормализация"):
        try:
            normalized_dialog = normalize_dialog(dialog)
            normalized_dialogs.append(normalized_dialog)
        except Exception as e:
            logger.error(f"Ошибка нормализации диалога {dialog.dialog_id}: {e}")
            continue
    
    # Сохранение результатов
    output_file = artifacts_dir / "stage3_normalized.jsonl"
    logger.info(f"💾 Сохранение результатов в {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for dialog in normalized_dialogs:
            f.write(json.dumps(dialog.dict()) + '\n')
    
    # Статистика нормализации
    original_barriers = set()
    original_ideas = set()
    original_signals = set()
    
    normalized_barriers = set()
    normalized_ideas = set()
    normalized_signals = set()
    
    for dialog in dialogs:
        original_barriers.update(dialog.barriers)
        original_ideas.update(dialog.ideas)
        original_signals.update(dialog.signals)
    
    for dialog in normalized_dialogs:
        normalized_barriers.update(dialog.barriers)
        normalized_ideas.update(dialog.ideas)
        normalized_signals.update(dialog.signals)
    
    logger.info("📊 Статистика нормализации:")
    logger.info(f"  Барьеры: {len(original_barriers)} → {len(normalized_barriers)}")
    logger.info(f"  Идеи: {len(original_ideas)} → {len(normalized_ideas)}")
    logger.info(f"  Сигналы: {len(original_signals)} → {len(normalized_signals)}")
    
    # Показываем примеры нормализации
    if original_barriers:
        logger.info("🔍 Примеры нормализации барьеров:")
        for original in list(original_barriers)[:5]:
            normalized = normalize_text(original, "barriers")
            if original != normalized:
                logger.info(f"  '{original}' → '{normalized}'")
    
    logger.info("✅ Stage 3 завершен успешно!")


if __name__ == "__main__":
    main()
