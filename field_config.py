"""
Конфигурация полей для анализа диалогов
Позволяет легко добавлять новые поля без изменения кода
"""

# Основные поля (всегда присутствуют)
CORE_FIELDS = {
    "dialog_id": "ID диалога",
    "delivery_discussed": "Обсуждалась ли доставка",
    "delivery_types": "Типы доставки",
    "barriers": "Барьеры",
    "ideas": "Идеи",
    "signals": "Сигналы",
    "self_check": "Self-check",
    "citations": "Цитаты"
}

# Дополнительные поля для извлечения
ADDITIONAL_FIELDS = {
    "region": "Регион/город клиента",
    "segment": "Сегмент клиента",
    "product_category": "Категория товара",
    "sentiment": "Тональность клиента",
    "client_type": "Тип клиента",
    "payment_method": "Способ оплаты",
    "return_issue": "Проблема с возвратом"
}

# Поля для CSV экспорта (все поля)
CSV_FIELDS = {**CORE_FIELDS, **ADDITIONAL_FIELDS}

# Поля для агрегации (поля с множественными значениями)
AGGREGATION_FIELDS = ["barriers", "ideas", "signals", "delivery_types"]

# Поля для контекстного анализа (с дополнительной информацией)
CONTEXT_FIELDS = ["barriers", "ideas", "signals"]

# Поля для статистики (одиночные значения)
STATISTICS_FIELDS = ["region", "segment", "product_category", "sentiment", "client_type", "payment_method"]

def get_all_fields():
    """Возвращает все поля"""
    return list(CSV_FIELDS.keys())

def get_core_fields():
    """Возвращает основные поля"""
    return list(CORE_FIELDS.keys())

def get_additional_fields():
    """Возвращает дополнительные поля"""
    return list(ADDITIONAL_FIELDS.keys())

def get_aggregation_fields():
    """Возвращает поля для агрегации"""
    return AGGREGATION_FIELDS

def get_context_fields():
    """Возвращает поля для контекстного анализа"""
    return CONTEXT_FIELDS

def get_statistics_fields():
    """Возвращает поля для статистики"""
    return STATISTICS_FIELDS

def get_field_description(field_name):
    """Возвращает описание поля"""
    return CSV_FIELDS.get(field_name, field_name)

def add_custom_field(field_name, description):
    """Добавляет пользовательское поле"""
    ADDITIONAL_FIELDS[field_name] = description
    CSV_FIELDS[field_name] = description

def remove_custom_field(field_name):
    """Удаляет пользовательское поле"""
    if field_name in ADDITIONAL_FIELDS:
        del ADDITIONAL_FIELDS[field_name]
    if field_name in CSV_FIELDS:
        del CSV_FIELDS[field_name]
