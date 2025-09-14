import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.stage2_extract_entities import clean_sentence, split_to_sentences, postprocess_quotes
from pipeline.stage5_aggregate import promote_questions_to_barriers
from pipeline.stage3_normalize import canon_delivery
from pipeline.stage4_cluster import short_label


def test_quote_cleanup():
    """Тест очистки цитат от мусора"""
    raw = "Угу. А как подключиться тогда Avito доставку, почему не подключено?"
    cleaned = clean_sentence(raw)
    assert "Угу" not in cleaned
    assert len(cleaned) <= 200
    assert "подключиться" in cleaned.lower()


def test_promote_questions_to_barriers():
    """Тест переноса вопросов в барьеры"""
    rec = {"ideas": [{"text": "А как сделать единую кнопку доставки?", "source_role": "client"}], "barriers": []}
    out = promote_questions_to_barriers(rec)
    assert any("непонимание" in b or "настрой" in b for b in out["barriers"])


def test_canon_delivery():
    """Тест нормализации типов доставки"""
    assert canon_delivery("Сдэк") == "СДЭК"
    assert canon_delivery("яндекс") == "Яндекс Доставка"
    assert canon_delivery("пункт выдачи") == "ПВЗ/постамат"


def test_short_label():
    """Тест коротких лейблов кластеров"""
    lab = short_label(["Чтобы можно клиентов", "Единая кнопка доставки", "Проблема"])
    assert lab.lower().startswith("единая кнопка")


def test_split_to_sentences():
    """Тест разбиения на предложения"""
    text = "Как подключить доставку? А где кнопка? Вот так."
    sentences = split_to_sentences(text)
    assert len(sentences) == 3
    assert "Как подключить доставку" in sentences[0]


def test_postprocess_quotes():
    """Тест постобработки цитат"""
    quotes = [
        {"quote": "Угу… а как подключиться тогда Avito доставку, почему не подключено?", "speaker": "Клиент"},
        {"quote": "А как сделать, чтобы в профиле была единая кнопка и клиент не выбирал другое?", "speaker": "Клиент"}
    ]
    processed = postprocess_quotes(quotes, limit=3)
    assert len(processed) >= 1
    assert all("Угу" not in q["quote"] for q in processed)
    assert all(len(q["quote"]) >= 8 for q in processed)


if __name__ == "__main__":
    test_quote_cleanup()
    test_promote_questions_to_barriers()
    test_canon_delivery()
    test_short_label()
    test_split_to_sentences()
    test_postprocess_quotes()
    print("✅ Все тесты прошли успешно!")
