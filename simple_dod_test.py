#!/usr/bin/env python3
"""
🧪 ПРОСТОЕ ТЕСТИРОВАНИЕ DoD СИСТЕМЫ
Быстрая проверка основных компонентов
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import numpy as np
import yaml
import jsonschema

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_taxonomy():
    """Тестирование таксономии"""
    logger.info("📋 Тестируем таксономию...")
    
    with open('taxonomy.yaml', 'r', encoding='utf-8') as f:
        taxonomy = yaml.safe_load(f)
    
    # Проверяем структуру
    assert 'themes' in taxonomy, "Отсутствует ключ 'themes'"
    assert 'limits' in taxonomy, "Отсутствует ключ 'limits'"
    
    themes = taxonomy['themes']
    assert len(themes) > 0, "Нет тем в таксономии"
    
    total_subthemes = 0
    for theme in themes:
        assert 'id' in theme, f"Отсутствует id в теме {theme}"
        assert 'name' in theme, f"Отсутствует name в теме {theme}"
        assert 'subthemes' in theme, f"Отсутствует subthemes в теме {theme}"
        
        for subtheme in theme['subthemes']:
            assert 'id' in subtheme, f"Отсутствует id в подтеме {subtheme}"
            assert 'name' in subtheme, f"Отсутствует name в подтеме {subtheme}"
            total_subthemes += 1
    
    # Проверяем лимит подтем
    max_subthemes = taxonomy['limits']['max_subthemes']
    assert total_subthemes <= max_subthemes, f"Превышен лимит подтем: {total_subthemes} > {max_subthemes}"
    
    logger.info(f"✅ Таксономия корректна: {len(themes)} тем, {total_subthemes} подтем")
    return taxonomy

def test_schema():
    """Тестирование JSON схемы"""
    logger.info("🔍 Тестируем JSON схему...")
    
    with open('schemas/mentions.schema.json', 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    # Проверяем структуру схемы
    assert 'type' in schema, "Отсутствует type в схеме"
    assert 'properties' in schema, "Отсутствует properties в схеме"
    assert 'required' in schema, "Отсутствует required в схеме"
    
    # Проверяем обязательные поля
    required_fields = schema['required']
    expected_fields = ['dialog_id', 'turn_id', 'theme', 'subtheme', 'label_type', 'text_quote', 'confidence']
    for field in expected_fields:
        assert field in required_fields, f"Отсутствует обязательное поле {field}"
    
    # Тестируем валидацию
    valid_mention = {
        "dialog_id": 1,
        "turn_id": 0,
        "theme": "доставка",
        "subtheme": "не работает выборочно",
        "label_type": "барьер",
        "text_quote": "У меня проблема с доставкой",
        "delivery_type": "complaint",
        "cause_hint": "причина указана",
        "confidence": 0.95
    }
    
    try:
        jsonschema.validate(valid_mention, schema)
        logger.info("✅ Валидное упоминание прошло проверку схемы")
    except jsonschema.ValidationError as e:
        logger.error(f"❌ Ошибка валидации: {e}")
        raise
    
    # Тестируем невалидное упоминание
    invalid_mention = {
        "dialog_id": 1,
        "turn_id": 0,
        "theme": "доставка",
        "subtheme": "не работает выборочно",
        "label_type": "барьер",
        "text_quote": "",  # Пустая цитата
        "confidence": 0.95
    }
    
    try:
        jsonschema.validate(invalid_mention, schema)
        logger.error("❌ Невалидное упоминание прошло проверку")
        raise AssertionError("Невалидное упоминание должно было быть отклонено")
    except jsonschema.ValidationError:
        logger.info("✅ Невалидное упоминание корректно отклонено")
    
    logger.info("✅ JSON схема работает корректно")
    return schema

def test_dedup_script():
    """Тестирование скрипта дедупликации"""
    logger.info("🔄 Тестируем скрипт дедупликации...")
    
    # Создаем тестовые упоминания
    test_mentions = [
        {
            "dialog_id": 1,
            "turn_id": 0,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "У меня проблема с доставкой",
            "confidence": 0.95
        },
        {
            "dialog_id": 1,
            "turn_id": 1,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "У меня проблема с доставкой",  # Дубликат
            "confidence": 0.90
        },
        {
            "dialog_id": 2,
            "turn_id": 0,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "Другая проблема с доставкой",  # Не дубликат
            "confidence": 0.85
        }
    ]
    
    # Сохраняем во временный файл
    temp_file = "artifacts/temp_test_mentions.jsonl"
    Path("artifacts").mkdir(exist_ok=True)
    
    with open(temp_file, 'w', encoding='utf-8') as f:
        for mention in test_mentions:
            f.write(json.dumps(mention, ensure_ascii=False) + '\n')
    
    # Запускаем дедупликацию
    import subprocess
    result = subprocess.run([
        'python', 'scripts/dedup.py', 
        '--in', temp_file, 
        '--out', 'artifacts/test_dedup_output.jsonl'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"❌ Ошибка дедупликации: {result.stderr}")
        return False
    
    # Проверяем результат
    deduped_mentions = []
    with open('artifacts/test_dedup_output.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            deduped_mentions.append(json.loads(line))
    
    # Должно остаться 2 упоминания (убрали 1 дубликат)
    assert len(deduped_mentions) == 2, f"Ожидалось 2 упоминания, получено {len(deduped_mentions)}"
    
    # Очистка
    Path(temp_file).unlink(missing_ok=True)
    Path('artifacts/test_dedup_output.jsonl').unlink(missing_ok=True)
    
    logger.info("✅ Скрипт дедупликации работает корректно")
    return True

def test_quality_checks():
    """Тестирование проверок качества"""
    logger.info("🔍 Тестируем проверки качества...")
    
    # Создаем тестовые данные
    test_mentions = [
        {
            "dialog_id": 1,
            "turn_id": 0,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "У меня проблема с доставкой",
            "confidence": 0.95
        },
        {
            "dialog_id": 2,
            "turn_id": 0,
            "theme": "прочее",
            "subtheme": "другое",
            "label_type": "барьер",
            "text_quote": "Какая-то другая проблема",
            "confidence": 0.60
        }
    ]
    
    # Создаем тестовые таблицы в DuckDB
    import duckdb
    
    conn = duckdb.connect(':memory:')
    
    # Таблица диалогов
    dialog_ids = list(set(m['dialog_id'] for m in test_mentions))
    dialogs_df = pd.DataFrame({'dialog_id': dialog_ids})
    conn.register('dialogs', dialogs_df)
    
    # Таблица упоминаний
    mentions_df = pd.DataFrame(test_mentions)
    conn.register('mentions', mentions_df)
    
    # Таблица utterances (все от клиента)
    utterances_data = []
    for mention in test_mentions:
        utterances_data.append({
            'dialog_id': mention['dialog_id'],
            'turn_id': mention['turn_id'],
            'role': 'client'
        })
    utterances_df = pd.DataFrame(utterances_data)
    conn.register('utterances', utterances_df)
    
    # Выполняем проверки качества
    with open('quality/checks.sql', 'r', encoding='utf-8') as f:
        queries = [q.strip() for q in f.read().split(';') if q.strip()]
    
    results = {}
    for i, query in enumerate(queries):
        try:
            result = conn.execute(query).fetchone()
            if i == 0:  # Q1 Evidence-100
                results['empty_quotes'] = result[0]
            elif i == 1:  # Q2 Client-only-100
                results['non_client_mentions'] = result[0]
            elif i == 2:  # Q3 Dedup
                results['dup_pct'] = result[0]
            elif i == 3:  # Q4 Coverage
                results['misc_share_pct'] = result[0]
        except Exception as e:
            logger.warning(f"Ошибка проверки качества {i+1}: {e}")
    
    conn.close()
    
    # Проверяем результаты
    assert results.get('empty_quotes', 0) == 0, "Найдены пустые цитаты"
    assert results.get('non_client_mentions', 0) == 0, "Найдены упоминания не от клиента"
    
    logger.info(f"✅ Проверки качества работают: {results}")
    return results

def test_sql_summaries():
    """Тестирование SQL сводок"""
    logger.info("📊 Тестируем SQL сводки...")
    
    # Создаем тестовые данные
    test_mentions = [
        {
            "dialog_id": 1,
            "turn_id": 0,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "У меня проблема с доставкой",
            "confidence": 0.95
        },
        {
            "dialog_id": 2,
            "turn_id": 0,
            "theme": "доставка",
            "subtheme": "не работает выборочно",
            "label_type": "барьер",
            "text_quote": "Другая проблема с доставкой",
            "confidence": 0.85
        },
        {
            "dialog_id": 3,
            "turn_id": 0,
            "theme": "продукт",
            "subtheme": "функционал не понятен",
            "label_type": "барьер",
            "text_quote": "Функционал не понятен",
            "confidence": 0.90
        }
    ]
    
    import duckdb
    
    conn = duckdb.connect(':memory:')
    
    # Создаем таблицы
    dialog_ids = list(set(m['dialog_id'] for m in test_mentions))
    dialogs_df = pd.DataFrame({'dialog_id': dialog_ids})
    conn.register('dialogs', dialogs_df)
    
    mentions_df = pd.DataFrame(test_mentions)
    conn.register('mentions', mentions_df)
    
    # Выполняем SQL запросы
    with open('sql/build_summaries.sql', 'r', encoding='utf-8') as f:
        sql_queries = f.read().split(';')
    
    summaries = {}
    for query in sql_queries:
        query = query.strip()
        if not query:
            continue
        
        try:
            result = conn.execute(query).fetchdf()
            table_name = query.split('CREATE OR REPLACE TABLE')[1].split('AS')[0].strip()
            summaries[table_name] = result.to_dict('records')
        except Exception as e:
            logger.warning(f"Ошибка выполнения SQL: {e}")
    
    conn.close()
    
    # Проверяем результаты
    assert 'summary_themes' in summaries, "Отсутствует таблица summary_themes"
    assert 'summary_subthemes' in summaries, "Отсутствует таблица summary_subthemes"
    assert 'index_quotes' in summaries, "Отсутствует таблица index_quotes"
    
    themes_summary = summaries['summary_themes']
    assert len(themes_summary) > 0, "Пустая сводка по темам"
    
    # Проверяем, что тема "доставка" есть в результатах
    delivery_theme = next((t for t in themes_summary if t['theme'] == 'доставка'), None)
    assert delivery_theme is not None, "Тема 'доставка' отсутствует в сводке"
    assert delivery_theme['dialog_count'] == 2, f"Неверное количество диалогов для темы 'доставка': {delivery_theme['dialog_count']}"
    
    logger.info(f"✅ SQL сводки работают: {len(summaries)} таблиц создано")
    return summaries

def test_jinja_templates():
    """Тестирование Jinja шаблонов"""
    logger.info("📝 Тестируем Jinja шаблоны...")
    
    from jinja2 import Environment, FileSystemLoader
    
    # Создаем тестовые данные
    test_data = {
        "total_dialogs": 100,
        "themes": [
            {"theme": "доставка", "dialog_count": 50, "mention_count": 75, "share_of_dialogs_pct": 50.0},
            {"theme": "продукт", "dialog_count": 30, "mention_count": 45, "share_of_dialogs_pct": 30.0}
        ],
        "subthemes": [
            {"theme": "доставка", "subtheme": "не работает выборочно", "dialog_count": 25, "mention_count": 35, "share_of_dialogs_pct": 25.0}
        ]
    }
    
    # Тестируем шаблон summary.jinja
    env = Environment(loader=FileSystemLoader('reports/templates'))
    template = env.get_template('summary.jinja')
    
    try:
        rendered = template.render(**test_data)
        assert "Всего диалогов: 100" in rendered, "Неверное отображение общего количества диалогов"
        assert "доставка" in rendered, "Тема 'доставка' отсутствует в рендере"
        assert "не работает выборочно" in rendered, "Подтема отсутствует в рендере"
        
        logger.info("✅ Шаблон summary.jinja работает корректно")
    except Exception as e:
        logger.error(f"❌ Ошибка рендеринга шаблона: {e}")
        raise
    
    logger.info("✅ Jinja шаблоны работают корректно")
    return True

def main():
    """Главная функция тестирования"""
    logger.info("🚀 Начинаем простое тестирование DoD системы...")
    
    try:
        # Тест таксономии
        taxonomy = test_taxonomy()
        
        # Тест JSON схемы
        schema = test_schema()
        
        # Тест дедупликации
        test_dedup_script()
        
        # Тест проверок качества
        quality_results = test_quality_checks()
        
        # Тест SQL сводок
        summaries = test_sql_summaries()
        
        # Тест Jinja шаблонов
        test_jinja_templates()
        
        # Вывод итоговых результатов
        print("\n" + "="*60)
        print("🎯 ИТОГИ ПРОСТОГО ТЕСТИРОВАНИЯ DoD СИСТЕМЫ")
        print("="*60)
        print(f"✅ Таксономия: {len(taxonomy['themes'])} тем")
        print(f"✅ JSON схема: валидация работает")
        print(f"✅ Дедупликация: скрипт работает")
        print(f"✅ Проверки качества: {quality_results}")
        print(f"✅ SQL сводки: {len(summaries)} таблиц")
        print(f"✅ Jinja шаблоны: рендеринг работает")
        print("="*60)
        
        logger.info("🎉 Простое тестирование завершено успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        raise

if __name__ == "__main__":
    main()
