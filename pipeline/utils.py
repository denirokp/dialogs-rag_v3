import os, re, json
from dotenv import load_dotenv
load_dotenv()

EXTRACT_MODE = os.getenv("EXTRACT_MODE", "RULES").upper()

# Простейшая токенизация ролей — адаптируйте под ваш формат входа
ROLE_PATTERNS = [
    (re.compile(r"^(?:client|клиент)[:\-]", re.I), "client"),
    (re.compile(r"^(?:operator|support|оператор|поддержка)[:\-]", re.I), "operator"),
]

def detect_role(line: str) -> str:
    for rx, role in ROLE_PATTERNS:
        if rx.search(line):
            return role
    # эвристика: по умолчанию клиент
    return "client"

# RULES — минимальные правила для demo/валидации; расширяйте словарь
RULES = [
    # Доставка - более точные паттерны
    {
        "when": re.compile(r"доставк.*(то есть|то работает|то нет|выборочн)", re.I),
        "theme": "доставка", "subtheme": "не работает выборочно", "label_type": "барьер",
    },
    {
        "when": re.compile(r"пункт выдач|ПВЗ|не (работает|включается).*доставк", re.I),
        "theme": "доставка", "subtheme": "не работает выборочно", "label_type": "барьер",
    },
    {
        "when": re.compile(r"вес|габарит|КГТ.*доставк", re.I),
        "theme": "доставка", "subtheme": "вес/габариты/КГТ", "label_type": "барьер",
    },
    {
        "when": re.compile(r"регион|покрытие.*доставк", re.I),
        "theme": "доставка", "subtheme": "регион/покрытие", "label_type": "барьер",
    },
    
    # UI/настройки - более точные паттерны
    {
        "when": re.compile(r"(где .*включ(ить|ается)|не вижу .*включить)", re.I),
        "theme": "UI/настройки", "subtheme": "непонятный интерфейс", "label_type": "барьер",
    },
    {
        "when": re.compile(r"не (вижу|понимаю).*включ(ить|ения)|настройк.*непонят", re.I),
        "theme": "UI/настройки", "subtheme": "непонятный интерфейс", "label_type": "барьер",
    },
    
    # Продвижение - более точные паттерны
    {
        "when": re.compile(r"(ставк|бюджет|просмотр).*(не помогает|эффект тот же|не окуп)", re.I),
        "theme": "продвижение", "subtheme": "не окупается", "label_type": "барьер",
    },
    {
        "when": re.compile(r"продвижен|реклама.*(мало запрос|не работает)", re.I),
        "theme": "продвижение", "subtheme": "не окупается", "label_type": "барьер",
    },
    {
        "when": re.compile(r"дорог(о|ая)|не стоит своих денег|высокая стоимость", re.I),
        "theme": "продвижение", "subtheme": "высокая стоимость", "label_type": "барьер",
    },
    
    # Поддержка - более точные паттерны
    {
        "when": re.compile(r"(писал|обращал).*(не реш|без результата|долго ждат)", re.I),
        "theme": "поддержка", "subtheme": "обращался — не помогло", "label_type": "сигнал",
    },
    {
        "when": re.compile(r"поддержк.*(не отвеч|медленно|долго)", re.I),
        "theme": "поддержка", "subtheme": "обращался — не помогло", "label_type": "сигнал",
    },
    
    # Цены
    {
        "when": re.compile(r"дорог(о|ая).*категор", re.I),
        "theme": "цены", "subtheme": "дорого для категории", "label_type": "барьер",
    },
    {
        "when": re.compile(r"фиксированн.*цен", re.I),
        "theme": "цены", "subtheme": "фиксированная цена", "label_type": "барьер",
    },
]

# Заглушка LLM — при желании подключите SDK, возвращайте dict как ниже
async def classify_via_llm(text: str):
    # TODO: подключите OpenAI/Anthropic и верните {theme, subtheme, label_type, confidence}
    return None

def classify_via_rules(text: str):
    for r in RULES:
        if r["when"].search(text):
            return {"theme": r["theme"], "subtheme": r["subtheme"], "label_type": r["label_type"], "confidence": 0.75}
    return None

async def classify(text: str):
    if EXTRACT_MODE == "LLM":
        out = await classify_via_llm(text)
        if out: return out
    return classify_via_rules(text)
