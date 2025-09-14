#!/usr/bin/env python3
"""
Модуль регулярных выражений для анализа диалогов
Компилированные паттерны с кириллическими границами слов
"""

import re
from typing import List, Pattern, Dict, Any

# Кириллические границы слов
CYRILLIC_WORD_BOUNDARY_LEFT = r"(?<![А-Яа-я0-9])"
CYRILLIC_WORD_BOUNDARY_RIGHT = r"(?![А-Яа-я0-9])"

def make_pattern(patterns: List[str]) -> Pattern:
    """Создание скомпилированного паттерна с кириллическими границами"""
    pattern_str = CYRILLIC_WORD_BOUNDARY_LEFT + r"(?:" + r"|".join(patterns) + r")" + CYRILLIC_WORD_BOUNDARY_RIGHT
    return re.compile(pattern_str, re.IGNORECASE)

# Ключевые слова доставки
KW_PATTERNS = [
    r"авито\s*доставк[аи]?",
    r"пункт(?:\s+выдачи)?|пвз|постамат\w*",
    r"курьер\w*",
    r"самовывоз\w*", 
    r"трек(?:-?номер)?",
    r"сдэк|boxberry|боксберри",
    r"почт[аы]",
    r"доставк\w*",
    r"отправ\w*",
    r"пересыл\w*",
    r"ozon|wildberries|яндекс\s*доставк[аи]?",
    r"пэк|деловые\s*линии",
    r"оформ(?:ить|ление)\s*отправк\w*",
    r"упаков\w*",
    r"трек(?:ать|[- ]?номер)?",
    r"возврат\w*",
    r"обратн(?:ая|ый)\s*доставк\w*",
    r"доставка\s*на\s*дом|до\s*двери",
    r"получен\w*",
    r"забрат\w*",
    r"привез\w*",
    r"логистик\w*"
]

# Маркеры барьеров
BARRIER_PATTERNS = [
    r"дорог\w+",
    r"комисси\w+", 
    r"сложн\w+",
    r"не\s*понимаю",
    r"не\s*разобрал\w*",
    r"жалоб\w+",
    r"проблем\w+",
    r"сбой\w+",
    r"возврат\w+",
    r"претензи\w+", 
    r"сломал\w+",
    r"поврежден\w+",
    r"нет\s+(?:пвз|доставк\w+|логистик\w+)"
]

# Маркеры идей
IDEA_PATTERNS = [
    r"идея",
    r"хотелось\s*бы",
    r"было\s*бы\s*удобно", 
    r"нужно\s*добавить",
    r"предложил\w*",
    r"можно\s*сделать",
    r"предлагаю"
]

# Негативные маркеры платформы (исключения)
NEG_PLATFORM_PATTERNS = [
    r"реклам\w+",
    r"подписк\w+",
    r"продвижен\w+",
    r"тариф\s*подписк\w+",
    r"цена\s*реклам\w+"
]

# Компилированные регексы
KW_REGEX = make_pattern(KW_PATTERNS)
BARRIER_RE = make_pattern(BARRIER_PATTERNS) 
IDEA_RE = make_pattern(IDEA_PATTERNS)
NEG_PLATFORM = make_pattern(NEG_PLATFORM_PATTERNS)

# Односложные ответы
YESNO = re.compile(r"^(да|нет|угу|ага|не\s*знаю|наверное|возможно)[.!?]?$", re.IGNORECASE)

def find_all_matches(pattern: Pattern, text: str) -> List[Dict[str, Any]]:
    """Найти все совпадения паттерна в тексте"""
    matches = []
    for match in pattern.finditer(text):
        matches.append({
            "text": match.group(0),
            "start": match.start(),
            "end": match.end()
        })
    return matches

def get_unique_keywords(text: str) -> List[str]:
    """Получить уникальные ключевые слова из текста"""
    matches = find_all_matches(KW_REGEX, text)
    return list(set([match["text"] for match in matches]))
