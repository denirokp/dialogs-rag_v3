#!/usr/bin/env python3
"""
Модуль агрегации результатов анализа диалогов
Строит отчеты по барьерам, идеям и сигналам с детальной аналитикой
"""
import json
import pandas as pd
from collections import Counter, defaultdict
from typing import Dict, List, Any, Optional
import os
from field_config import get_aggregation_fields, get_context_fields, get_statistics_fields, get_all_fields

def load_jsonl_results(filepath: str = "batch_results.jsonl") -> List[Dict]:
    """Загружает результаты из JSONL файла"""
    if not os.path.exists(filepath):
        print(f"❌ Файл {filepath} не найден")
        return []
    
    results = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    results.append(json.loads(line.strip()))
                except json.JSONDecodeError as e:
                    print(f"⚠️ Ошибка парсинга строки: {e}")
                    continue
    
    print(f"📊 Загружено {len(results)} результатов из {filepath}")
    return results

def extract_field_values(results: List[Dict], field: str) -> List[str]:
    """Извлекает все значения поля из результатов"""
    values = []
    for result in results:
        field_value = result.get(field, "")
        if isinstance(field_value, list):
            values.extend(field_value)
        elif field_value and field_value.strip():
            values.append(field_value.strip())
    return values

def get_field_distribution(results: List[Dict], field: str) -> Dict[str, int]:
    """Получает распределение значений поля"""
    values = extract_field_values(results, field)
    return dict(Counter(values))

def get_field_with_context(results: List[Dict], field: str) -> List[Dict]:
    """Получает значения поля с контекстом (dialog_id, citations)"""
    items = []
    for result in results:
        field_value = result.get(field, [])
        if isinstance(field_value, list):
            for item in field_value:
                if item and item.strip():
                    items.append({
                        "value": item.strip(),
                        "dialog_id": result.get("dialog_id", ""),
                        "citations": result.get("citations", [])
                    })
    return items

def format_card(title: str, count: int, total: int, 
                regions: Optional[Dict] = None, 
                segments: Optional[Dict] = None, 
                categories: Optional[Dict] = None,
                sentiment: Optional[Dict] = None,
                quotes: Optional[List[Dict]] = None) -> str:
    """Форматирует карточку для отчета"""
    percent = (count / total * 100) if total > 0 else 0
    
    lines = [
        f"### {title}",
        f"- Всего упоминаний: {count} ({percent:.1f}% диалогов)"
    ]
    
    if regions and len(regions) > 1:
        top_regions = sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]
        regions_str = ", ".join([f"{r} ({n})" for r, n in top_regions])
        lines.append(f"- Основные регионы: {regions_str}")
    
    if segments and len(segments) > 1:
        top_segments = sorted(segments.items(), key=lambda x: x[1], reverse=True)[:5]
        segments_str = ", ".join([f"{s} ({n})" for s, n in top_segments])
        lines.append(f"- Сегменты: {segments_str}")
    
    if categories and len(categories) > 1:
        top_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]
        categories_str = ", ".join([f"{c} ({n})" for c, n in top_categories])
        lines.append(f"- Категории товаров: {categories_str}")
    
    if sentiment and len(sentiment) > 1:
        total_sentiment = sum(sentiment.values())
        sentiment_str = ", ".join([f"{s} ({n/total_sentiment*100:.0f}%)" for s, n in sorted(sentiment.items(), key=lambda x: x[1], reverse=True)])
        lines.append(f"- Тональность: {sentiment_str}")
    
    if quotes:
        lines.append("- Цитаты:")
        for i, quote in enumerate(quotes[:5], 1):
            quote_text = quote.get("quote", "").replace('"', '\\"')
            dialog_id = quote.get("dialog_id", "")
            lines.append(f"   {i}. \"{quote_text}\" (Id: {dialog_id})")
    
    return "\n".join(lines)

def analyze_field_with_context(results: List[Dict], field: str) -> Dict[str, Any]:
    """Анализирует поле с контекстом"""
    items = get_field_with_context(results, field)
    
    # Группируем по значениям
    grouped = defaultdict(list)
    for item in items:
        grouped[item["value"]].append(item)
    
    analysis = {}
    for value, items_list in grouped.items():
        count = len(items_list)
        
        # Собираем контекст
        regions = {}
        segments = {}
        categories = {}
        sentiment = {}
        quotes = []
        
        for item in items_list:
            # Находим соответствующий результат
            dialog_id = item["dialog_id"]
            result = next((r for r in results if r.get("dialog_id") == dialog_id), {})
            
            # Собираем дополнительные поля
            if result.get("region"):
                regions[result["region"]] = regions.get(result["region"], 0) + 1
            if result.get("segment"):
                segments[result["segment"]] = segments.get(result["segment"], 0) + 1
            if result.get("product_category"):
                categories[result["product_category"]] = categories.get(result["product_category"], 0) + 1
            if result.get("sentiment"):
                sentiment[result["sentiment"]] = sentiment.get(result["sentiment"], 0) + 1
            
            # Добавляем цитаты
            for citation in item.get("citations", []):
                if citation.get("quote"):
                    quotes.append({
                        "quote": citation["quote"],
                        "dialog_id": dialog_id
                    })
        
        analysis[value] = {
            "count": count,
            "regions": regions,
            "segments": segments,
            "categories": categories,
            "sentiment": sentiment,
            "quotes": quotes
        }
    
    return analysis

def generate_markdown_report(results: List[Dict]) -> str:
    """Генерирует Markdown отчет"""
    if not results:
        return "# Отчет по анализу диалогов\n\n❌ Нет данных для анализа"
    
    total_dialogs = len(results)
    
    # Анализируем поля для агрегации
    aggregation_analyses = {}
    for field in get_aggregation_fields():
        if field in ["barriers", "ideas", "signals"]:  # Основные поля для детального анализа
            aggregation_analyses[field] = analyze_field_with_context(results, field)
    
    # Общая статистика
    delivery_discussed = sum(1 for r in results if r.get("delivery_discussed", False))
    delivery_percent = (delivery_discussed / total_dialogs * 100) if total_dialogs > 0 else 0
    
    report_lines = [
        "# 📊 Отчет по анализу диалогов",
        f"",
        f"## 📈 Общая статистика",
        f"- Всего диалогов: {total_dialogs}",
        f"- Диалогов с обсуждением доставки: {delivery_discussed} ({delivery_percent:.1f}%)",
        f"- Диалогов без обсуждения доставки: {total_dialogs - delivery_discussed} ({100 - delivery_percent:.1f}%)",
        f"",
        f"## 🚧 Барьеры клиентов",
        f""
    ]
    
    # Сортируем барьеры по количеству упоминаний
    if "barriers" in aggregation_analyses:
        sorted_barriers = sorted(aggregation_analyses["barriers"].items(), key=lambda x: x[1]["count"], reverse=True)
        
        for barrier, data in sorted_barriers:
            if data["count"] > 0:
                card = format_card(
                    barrier, 
                    data["count"], 
                    total_dialogs,
                    data["regions"] if data["regions"] else None,
                    data["segments"] if data["segments"] else None,
                    data["categories"] if data["categories"] else None,
                    data["sentiment"] if data["sentiment"] else None,
                    data["quotes"] if data["quotes"] else None
                )
                report_lines.append(card)
                report_lines.append("")
    
    report_lines.extend([
        "## 💡 Идеи клиентов",
        ""
    ])
    
    # Сортируем идеи по количеству упоминаний
    if "ideas" in aggregation_analyses:
        sorted_ideas = sorted(aggregation_analyses["ideas"].items(), key=lambda x: x[1]["count"], reverse=True)
        
        for idea, data in sorted_ideas:
            if data["count"] > 0:
                card = format_card(
                    idea, 
                    data["count"], 
                    total_dialogs,
                    data["regions"] if data["regions"] else None,
                    data["segments"] if data["segments"] else None,
                    data["categories"] if data["categories"] else None,
                    data["sentiment"] if data["sentiment"] else None,
                    data["quotes"] if data["quotes"] else None
                )
                report_lines.append(card)
                report_lines.append("")
    
    report_lines.extend([
        "## 🎯 Сигналы клиентов",
        ""
    ])
    
    # Сортируем сигналы по количеству упоминаний
    if "signals" in aggregation_analyses:
        sorted_signals = sorted(aggregation_analyses["signals"].items(), key=lambda x: x[1]["count"], reverse=True)
        
        for signal, data in sorted_signals:
            if data["count"] > 0:
                card = format_card(
                    signal, 
                    data["count"], 
                    total_dialogs,
                    data["regions"] if data["regions"] else None,
                    data["segments"] if data["segments"] else None,
                    data["categories"] if data["categories"] else None,
                    data["sentiment"] if data["sentiment"] else None,
                    data["quotes"] if data["quotes"] else None
                )
                report_lines.append(card)
                report_lines.append("")
    
    # Дополнительная аналитика
    report_lines.extend([
        "## 📊 Дополнительная аналитика",
        ""
    ])
    
    # Дополнительная аналитика по полям статистики
    for field in get_statistics_fields():
        field_dist = get_field_distribution(results, field)
        if field_dist and any(field_dist.values()):
            field_name = field.replace("_", " ").title()
            report_lines.append(f"### {field_name}")
            
            if field == "sentiment":
                # Для тональности показываем проценты
                total_field = sum(field_dist.values())
                for value, count in sorted(field_dist.items(), key=lambda x: x[1], reverse=True):
                    if value:
                        percent = (count / total_field * 100) if total_field > 0 else 0
                        report_lines.append(f"- {value}: {count} диалогов ({percent:.1f}%)")
            else:
                # Для остальных полей показываем количество
                for value, count in sorted(field_dist.items(), key=lambda x: x[1], reverse=True)[:10]:
                    if value:
                        report_lines.append(f"- {value}: {count} диалогов")
            report_lines.append("")
    
    return "\n".join(report_lines)

def generate_excel_report(results: List[Dict], output_file: str = "report.xlsx") -> None:
    """Генерирует Excel отчет"""
    if not results:
        print("❌ Нет данных для Excel отчета")
        return
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Общая статистика
        total_dialogs = len(results)
        delivery_discussed = sum(1 for r in results if r.get("delivery_discussed", False))
        
        stats_data = {
            "Метрика": ["Всего диалогов", "С обсуждением доставки", "Без обсуждения доставки"],
            "Количество": [total_dialogs, delivery_discussed, total_dialogs - delivery_discussed],
            "Процент": [100, (delivery_discussed/total_dialogs*100) if total_dialogs > 0 else 0, 
                       ((total_dialogs - delivery_discussed)/total_dialogs*100) if total_dialogs > 0 else 0]
        }
        pd.DataFrame(stats_data).to_excel(writer, sheet_name="Общая статистика", index=False)
        
        # Барьеры
        barriers_data = []
        for result in results:
            for barrier in result.get("barriers", []):
                barriers_data.append({
                    "Барьер": barrier,
                    "ID диалога": result.get("dialog_id", ""),
                    "Регион": result.get("region", ""),
                    "Сегмент": result.get("segment", ""),
                    "Категория": result.get("product_category", ""),
                    "Тональность": result.get("sentiment", "")
                })
        
        if barriers_data:
            pd.DataFrame(barriers_data).to_excel(writer, sheet_name="Барьеры", index=False)
        
        # Идеи
        ideas_data = []
        for result in results:
            for idea in result.get("ideas", []):
                ideas_data.append({
                    "Идея": idea,
                    "ID диалога": result.get("dialog_id", ""),
                    "Регион": result.get("region", ""),
                    "Сегмент": result.get("segment", ""),
                    "Категория": result.get("product_category", ""),
                    "Тональность": result.get("sentiment", "")
                })
        
        if ideas_data:
            pd.DataFrame(ideas_data).to_excel(writer, sheet_name="Идеи", index=False)
        
        # Сигналы
        signals_data = []
        for result in results:
            for signal in result.get("signals", []):
                signals_data.append({
                    "Сигнал": signal,
                    "ID диалога": result.get("dialog_id", ""),
                    "Регион": result.get("region", ""),
                    "Сегмент": result.get("segment", ""),
                    "Категория": result.get("product_category", ""),
                    "Тональность": result.get("sentiment", "")
                })
        
        if signals_data:
            pd.DataFrame(signals_data).to_excel(writer, sheet_name="Сигналы", index=False)
        
        # Детальные результаты
        detailed_data = []
        for result in results:
            detailed_data.append({
                "ID диалога": result.get("dialog_id", ""),
                "Обсуждалась доставка": result.get("delivery_discussed", False),
                "Типы доставки": "|".join(result.get("delivery_types", [])),
                "Барьеры": "|".join(result.get("barriers", [])),
                "Идеи": "|".join(result.get("ideas", [])),
                "Сигналы": "|".join(result.get("signals", [])),
                "Регион": result.get("region", ""),
                "Сегмент": result.get("segment", ""),
                "Категория": result.get("product_category", ""),
                "Тональность": result.get("sentiment", ""),
                "Тип клиента": result.get("client_type", ""),
                "Способ оплаты": result.get("payment_method", ""),
                "Проблема с возвратом": result.get("return_issue", ""),
                "Self-check": result.get("self_check", "")
            })
        
        pd.DataFrame(detailed_data).to_excel(writer, sheet_name="Детальные результаты", index=False)
    
    print(f"📊 Excel отчет сохранен: {output_file}")

def main():
    """Основная функция"""
    print("🚀 Запуск агрегации результатов анализа диалогов")
    print("=" * 50)
    
    # Загружаем результаты
    results = load_jsonl_results("batch_results.jsonl")
    
    if not results:
        print("❌ Нет данных для анализа. Сначала запустите eval_batch.py")
        return
    
    # Генерируем Markdown отчет
    print("📝 Генерируем Markdown отчет...")
    markdown_report = generate_markdown_report(results)
    
    with open("report.md", "w", encoding="utf-8") as f:
        f.write(markdown_report)
    
    print("✅ Markdown отчет сохранен: report.md")
    
    # Генерируем Excel отчет
    print("📊 Генерируем Excel отчет...")
    generate_excel_report(results, "report.xlsx")
    
    print("✅ Excel отчет сохранен: report.xlsx")
    
    # Выводим краткую статистику
    total_dialogs = len(results)
    delivery_discussed = sum(1 for r in results if r.get("delivery_discussed", False))
    
    print(f"\n📈 Краткая статистика:")
    print(f"   Всего диалогов: {total_dialogs}")
    print(f"   С обсуждением доставки: {delivery_discussed} ({delivery_discussed/total_dialogs*100:.1f}%)")
    
    # Считаем барьеры, идеи, сигналы
    barriers_count = sum(len(r.get("barriers", [])) for r in results)
    ideas_count = sum(len(r.get("ideas", [])) for r in results)
    signals_count = sum(len(r.get("signals", [])) for r in results)
    
    print(f"   Всего барьеров: {barriers_count}")
    print(f"   Всего идей: {ideas_count}")
    print(f"   Всего сигналов: {signals_count}")
    
    print(f"\n🎉 Агрегация завершена!")
    print(f"📁 Файлы отчетов:")
    print(f"   - report.md (Markdown)")
    print(f"   - report.xlsx (Excel)")

if __name__ == "__main__":
    main()
