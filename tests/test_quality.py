import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.stage2_extract_entities import clean_sentence, split_turns, pick_client_quotes
from pipeline.stage3_normalize import canon_delivery
from pipeline.stage4_cluster import short_label, promote_questions_to_barriers
from pipeline.stage5_aggregate import cluster_mentions, split_sections
from pipeline.stage6_report import render_mentions


def test_mentions_unique_ids():
    """Тест подсчета уникальных ID диалогов"""
    cluster_records = [
        {"dialog_id": "123", "text": "test1"},
        {"dialog_id": "456", "text": "test2"},
        {"dialog_id": "123", "text": "test3"},  # дубликат
        {"dialog_id": "789", "text": "test4"}
    ]
    mentions = cluster_mentions(cluster_records)
    assert mentions == 3  # 3 уникальных ID


def test_quote_cleanup():
    """Тест очистки цитат"""
    s = "Угу. А как подключить Avito доставку, почему не включено?"
    cleaned = clean_sentence(s)
    assert not cleaned.lower().startswith("угу")
    assert cleaned.lower().startswith("а как") or cleaned.lower().startswith("как")


def test_canon_delivery():
    """Тест нормализации типов доставки"""
    assert canon_delivery("Сдэк") == "СДЭК"
    assert canon_delivery("яндекс") == "Яндекс Доставка"
    assert canon_delivery("пункт выдачи") == "ПВЗ/постамат"


def test_short_label():
    """Тест коротких лейблов кластеров"""
    lab = short_label(["Чтобы можно клиентов", "Единая кнопка доставки", "Проблема"])
    assert lab.lower().startswith("единая кнопка")


def test_promote_questions_to_barriers():
    """Тест промо вопросов в барьеры"""
    item = {
        "evidence_span": "Как подключить доставку?",
        "labels": {"barrier": []}
    }
    result = promote_questions_to_barriers(item)
    assert "непонимание процесса доставки" in result["labels"]["barrier"]


def test_split_sections():
    """Тест разделения по секциям"""
    records = [
        {"source_role": "client", "labels": {"barrier": ["дорого"]}},
        {"source_role": "client", "labels": {"idea": ["добавить ПВЗ"]}},
        {"source_role": "operator", "labels": {"idea": ["рекомендую СДЭК"]}},
        {"source_role": "client", "labels": {"signal": ["предпочитаю самовывоз"]}}
    ]
    barriers, ideas_client, signals, operator_recos = split_sections(records)
    assert len(barriers) == 1
    assert len(ideas_client) == 1
    assert len(signals) == 1
    assert len(operator_recos) == 1


def test_render_mentions():
    """Тест рендеринга упоминаний"""
    result = render_mentions(5, 20)
    assert result == "5 из 20 (25.0%)"
    
    result = render_mentions(0, 0)
    assert result == "0 из 0 (0%)"


def test_split_turns():
    """Тест парсинга диалога на реплики"""
    dialog = """клиент: Привет, как дела?
оператор: Хорошо, чем могу помочь?
клиент: Хочу подключить доставку"""
    turns = split_turns(dialog)
    assert len(turns) == 3
    assert turns[0]["speaker"] == "клиент"
    assert turns[1]["speaker"] == "оператор"
    assert turns[2]["speaker"] == "клиент"


def test_pick_client_quotes():
    """Тест выбора цитат клиента"""
    turns = [
        {"speaker": "клиент", "text": "Как подключить доставку?"},
        {"speaker": "оператор", "text": "Сейчас помогу"},
        {"speaker": "клиент", "text": "Хочу использовать СДЭК"}
    ]
    quotes = pick_client_quotes(turns, limit=2)
    assert len(quotes) <= 2
    assert all(q["speaker"] == "Клиент" for q in quotes)


if __name__ == "__main__":
    test_mentions_unique_ids()
    test_quote_cleanup()
    test_canon_delivery()
    test_short_label()
    test_promote_questions_to_barriers()
    test_split_sections()
    test_render_mentions()
    test_split_turns()
    test_pick_client_quotes()
    print("✅ Все тесты качества прошли успешно!")
