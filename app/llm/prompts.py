EXTRACTION_SYSTEM = (
    "Ты извлекаешь только из слов клиента (role == client). Для каждого упоминания "
    "верни JSON со схемой: dialog_id, turn_id, theme, subtheme (опционально), "
    "text_quote (дословно), confidence (0..1). Никаких объяснений."
)

EXTRACTION_USER_TMPL = (
    "Таксономия (themes→subthemes):\n{taxonomy}\n---\n"
    "Диалоговое окно (только реплики клиента):\n{window}\n---\n"
    "Верни массив JSON-объектов со строгими ключами и только по словам клиента."
)
