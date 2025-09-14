#!/usr/bin/env python3
"""
Stage 6: Генерация отчетов
Создает Markdown и Excel отчеты в требуемом формате
"""

import json
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from models.validation import AggregateResults, Cluster, ClusterSlices
from prompts import STAGE_CONFIG

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def frac_of(total: int, part: int) -> str:
    """Форматирование 'X из Y (Z%)'"""
    if total <= 0:
        return f"{part}"
    pct = 100.0 * part / total
    return f"{part} из {total} ({pct:.1f}%)"


def frac_pct_only(total: int, part: int) -> str:
    """Форматирование 'X (Z%)'"""
    if total <= 0:
        return f"{part}"
    return f"{part} ({100.0 * part / total:.1f}%)"


def line_part_of(total: int, count: int) -> str:
    """Форматирование 'X из Y (Z%)' с проверкой на ноль"""
    return f"{count} из {total} ({(100.0*count/total):.1f}%)" if total else f"{count}"


def part_of(total: int, count: int) -> str:
    """Универсальное форматирование 'X из Y (Z%)'"""
    return f"{count} из {total} ({(100.0*count/total):.1f}%)" if total else f"{count}"


def rare_badge(cluster: Dict[str, Any]) -> str:
    """Бейдж для единичных кейсов"""
    from config import settings
    return " *(единичный кейс)*" if cluster.get("mentions_abs", 0) < settings.rare_threshold else ""


def low_evidence_badge(cluster: Dict[str, Any]) -> str:
    """Бейдж для низкой подтверждаемости"""
    low_evidence_share = cluster.get("low_evidence_share", 0)
    return " *(низкая подтверждаемость — мало цитат)*" if low_evidence_share >= 0.5 else ""


def pick_cluster_quotes(quotes, max_per_cluster=3):
    """Выбор цитат из разных диалогов для кластера с очисткой"""
    from pipeline.stage2_extract_entities import clean_sentence
    
    out, seen = [], set()
    for q in quotes:
        did = q.get("dialog_id")
        qt = q.get("quote", "").strip()
        if not qt or did in seen: 
            continue
        
        # Очищаем цитату
        cleaned_quote = clean_sentence(qt)
        if len(cleaned_quote) < 10:  # Слишком короткая после очистки
            continue
            
        seen.add(did)
        out.append({"dialog_id": did, "quote": cleaned_quote})
        if len(out) >= max_per_cluster: 
            break
    return out


def render_mentions(x, D):
    """Рендеринг упоминаний с процентами"""
    share = f"{(100*x/D):.1f}%" if D else "0%"
    return f"{x} из {D} ({share})"


def validate_cluster_data(cluster: Dict[str, Any], cluster_id: int) -> List[str]:
    """Проверка валидности данных кластера"""
    errors = []
    
    # Проверка соответствия ID и mentions_abs
    dialog_ids = cluster.get("dialog_ids", [])
    mentions_abs = cluster.get("mentions_abs", 0)
    
    if len(set(dialog_ids)) != mentions_abs:
        errors.append(f"Кластер {cluster_id}: IDs != mentions_abs ({len(set(dialog_ids))} != {mentions_abs})")
    
    # Проверка распределения тональности
    sentiment = cluster.get("slices", {}).get("sentiment", {})
    if sentiment and mentions_abs > 0:
        total_sentiment = sum(sentiment.values())
        if total_sentiment > 0:
            total_pct = sum(100.0 * v / total_sentiment for v in sentiment.values())
            if not (99.0 <= total_pct <= 101.0):
                errors.append(f"Кластер {cluster_id}: Тональность не ~100% ({total_pct:.1f}%)")
    
    return errors


def load_aggregate_results() -> Dict[str, Any]:
    """Загрузка результатов агрегации"""
    aggregate_file = Path("artifacts/aggregate_results.json")
    
    if not aggregate_file.exists():
        logger.error(f"❌ Файл {aggregate_file} не найден. Запустите Stage 5 сначала.")
        return None
    
    with open(aggregate_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data


def format_slices(slices: Dict[str, Any], D: int, mentions_abs: int) -> str:
    """Форматирование срезов данных"""
    lines = []
    
    if slices.get("regions"):
        lines.append("- По регионам:")
        for region, count in sorted(slices["regions"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {region}: {part_of(D, count)}")
    
    if slices.get("segments"):
        lines.append("- По сегментам:")
        for segment, count in sorted(slices["segments"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {segment}: {part_of(D, count)}")
    
    if slices.get("product_categories"):
        lines.append("- По категориям товаров:")
        for category, count in sorted(slices["product_categories"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {category}: {part_of(D, count)}")
    
    if slices.get("delivery_types"):
        lines.append("- По типам доставки:")
        for delivery_type, count in sorted(slices["delivery_types"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {delivery_type}: {part_of(D, count)}")
    
    if slices.get("sentiment"):
        lines.append("- Тональность (внутри кластера = 100%):")
        for sentiment, count in sorted(slices["sentiment"].items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {sentiment}: {part_of(mentions_abs, count)}")
    
    return "\n".join(lines)


def format_cluster_card(cluster: Dict[str, Any], cluster_id: int, D: int) -> str:
    """Форматирование карточки кластера"""
    mentions_abs = cluster.get("mentions_abs", 0)
    
    # Заголовок
    cluster_name = cluster.get('name', 'Неизвестно')
    rare_badge_text = rare_badge(cluster)
    low_evidence_badge_text = low_evidence_badge(cluster)
    lines = [f"### Кластер {cluster_id}: {cluster_name}{rare_badge_text}{low_evidence_badge_text}"]
    
    # Основные метрики
    lines.append(f"- Упоминаний в этом кластере: {part_of(D, mentions_abs)}")
    
    # Варианты формулировок
    if cluster.get("variants"):
        lines.append("- Варианты формулировок:")
        for variant in cluster["variants"]:
            lines.append(f'  • "{variant["text"]}" — {part_of(D, variant["count_abs"])}')
    
    # Срезы данных
    if cluster.get("slices"):
        slices_text = format_slices(cluster["slices"], D, mentions_abs)
        if slices_text:
            lines.append(slices_text)
    
    # ID диалогов
    if cluster.get("dialog_ids"):
        show_limit = STAGE_CONFIG["show_ids_limit"]
        ids = cluster["dialog_ids"]
        lines.append(f"- ID диалогов ({mentions_abs} из {D}): {ids[:show_limit]}")
        if len(ids) > show_limit:
            lines.append(f"  *(полный список — Приложение A)*")
    
    # Цитаты клиента
    quotes = pick_cluster_quotes(cluster.get("quotes", []), max_per_cluster=3)
    if quotes:
        lines.append("- Цитаты клиента (примеры):")
        for i, q in enumerate(quotes, 1):
            lines.append(f'  {i}) "{q["quote"]}" (Id: {q["dialog_id"]})')
    else:
        lines.append("- Цитаты клиента: не найдены (вероятно, проблема выражена оператором/неявно)")
    
    return "\n".join(lines)


def render_cluster_block(c, D, rare_threshold=3):
    """Рендеринг блока кластера с учетом редких кейсов"""
    rare = c["mentions_abs"] < rare_threshold
    # при rare — не печатаем длинные срезы
    return c, rare


def generate_markdown_report(results: Dict[str, Any]) -> str:
    """Генерация Markdown отчета"""
    lines = []
    
    # Извлекаем метаданные
    meta = results.get("meta", {})
    N = meta.get("N", 0)
    D = meta.get("D", 0)
    
    # Заголовок
    lines.extend([
        "# Отчет по анализу диалогов",
        "",
        f"*Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        "",
        "## Проверка охвата по доставке",
        f"- Всего звонков (N): {N}",
        f"- С доставкой (D): {part_of(N, D)}",
        f"- Без доставки: {N - D}",
        ""
    ])
    
    # Проверка наличия цитат клиента
    total_quotes = 0
    for category in ["barriers", "ideas", "signals"]:
        clusters = results.get(category, [])
        for cluster in clusters:
            quotes = cluster.get("quotes", [])
            total_quotes += len(quotes)
    
    if total_quotes == 0:
        lines.extend([
            "## ⚠️ Внимание: Цитаты клиента не найдены",
            "",
            "В ходе анализа не было найдено ни одной цитаты клиента. Это может указывать на:",
            "- Проблемы с извлечением цитат из диалогов",
            "- Недостаточное качество данных",
            "- Ошибки в процессе обработки",
            "",
            "**Рекомендация:** Проверьте настройки извлечения цитат в Stage 2.",
            ""
        ])
    
    # Предупреждение о малой выборке
    warn_threshold = STAGE_CONFIG.get("small_sample_threshold", 50)
    if D < warn_threshold:
        lines.append(f"> ⚠️ Внимание: малая выборка. D < {warn_threshold}, проценты ориентировочные.\n")
    
    # Барьеры
    if results.get("barriers"):
        lines.extend([
            "## Барьеры",
            ""
        ])
        
        for i, cluster in enumerate(results["barriers"], 1):
            cluster_card = format_cluster_card(cluster, i, D)
            lines.append(cluster_card)
            lines.append("")
    
    # Идеи
    if results.get("ideas"):
        lines.extend([
            "## Идеи",
            ""
        ])
        
        for i, cluster in enumerate(results["ideas"], 1):
            cluster_card = format_cluster_card(cluster, i, D)
            lines.append(cluster_card)
            lines.append("")
    
    # Сигналы платформы
    if results.get("signals_platform"):
        lines.extend([
            "## Сигналы платформы",
            ""
        ])
        
        for i, cluster in enumerate(results["signals_platform"], 1):
            cluster_card = format_cluster_card(cluster, i, D)
            lines.append(cluster_card)
            lines.append("")
    
    # Сигналы доставки
    if results.get("signals"):
        lines.extend([
            "## Сигналы доставки",
            ""
        ])
        
        for i, cluster in enumerate(results["signals"], 1):
            cluster_card = format_cluster_card(cluster, i, D)
            lines.append(cluster_card)
            lines.append("")
    
    return "\n".join(lines)


def generate_appendix_ids(results: Dict[str, Any]) -> str:
    """Генерация приложения с полными списками ID"""
    lines = []
    
    lines.extend([
        "# Приложение A: Полные списки ID диалогов",
        "",
        f"*Сгенерировано: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        ""
    ])
    
    # Барьеры
    if results.get("barriers"):
        lines.extend([
            "## Барьеры",
            ""
        ])
        
        for i, cluster in enumerate(results["barriers"], 1):
            lines.extend([
                f"### Кластер {i}: {cluster.get('name', 'Неизвестно')}",
                f"ID диалогов ({len(cluster.get('dialog_ids', []))}):",
                ", ".join(cluster.get('dialog_ids', [])),
                ""
            ])
    
    # Идеи
    if results.get("ideas"):
        lines.extend([
            "## Идеи",
            ""
        ])
        
        for i, cluster in enumerate(results["ideas"], 1):
            lines.extend([
                f"### Кластер {i}: {cluster.get('name', 'Неизвестно')}",
                f"ID диалогов ({len(cluster.get('dialog_ids', []))}):",
                ", ".join(cluster.get('dialog_ids', [])),
                ""
            ])
    
    # Сигналы доставки
    if results.get("signals"):
        lines.extend([
            "## Сигналы доставки",
            ""
        ])
        
        for i, cluster in enumerate(results["signals"], 1):
            lines.extend([
                f"### Кластер {i}: {cluster.get('name', 'Неизвестно')}",
                f"ID диалогов ({len(cluster.get('dialog_ids', []))}):",
                ", ".join(cluster.get('dialog_ids', [])),
                ""
            ])
    
    # Сигналы платформы
    if results.get("signals_platform"):
        lines.extend([
            "## Сигналы платформы",
            ""
        ])
        
        for i, cluster in enumerate(results["signals_platform"], 1):
            lines.extend([
                f"### Кластер {i}: {cluster.get('name', 'Неизвестно')}",
                f"ID диалогов ({len(cluster.get('dialog_ids', []))}):",
                ", ".join(cluster.get('dialog_ids', [])),
                ""
            ])
    
    return "\n".join(lines)


def generate_excel_report(results: Dict[str, Any]):
    """Генерация Excel отчета"""
    reports_dir = Path(settings.output_dir)
    reports_dir.mkdir(exist_ok=True)
    
    excel_file = reports_dir / "report.xlsx"
    
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        sheets_created = False
        
        # Барьеры
        if results.get("barriers"):
            barrier_data = []
            for i, cluster in enumerate(results["barriers"], 1):
                total_mentions = cluster.get("mentions_abs", 0)
                percentage = cluster.get("mentions_pct_of_D", 0)
                
                barrier_data.append({
                    "cluster_id": i,
                    "name": cluster.get("name", ""),
                    "total_mentions": total_mentions,
                    "percentage": percentage,
                    "unique_dialogs": len(cluster.get("dialog_ids", [])),
                    "variants_json": json.dumps(cluster.get("variants", []), ensure_ascii=False),
                    "slices_json": json.dumps(cluster.get("slices", {}), ensure_ascii=False),
                    "dialog_ids_json": json.dumps(cluster.get("dialog_ids", []), ensure_ascii=False)
                })
            
            barrier_df = pd.DataFrame(barrier_data)
            barrier_df.to_excel(writer, sheet_name='barriers', index=False)
            sheets_created = True
        
        # Идеи
        if results.get("ideas"):
            idea_data = []
            for i, cluster in enumerate(results["ideas"], 1):
                total_mentions = cluster.get("mentions_abs", 0)
                percentage = cluster.get("mentions_pct_of_D", 0)
                
                idea_data.append({
                    "cluster_id": i,
                    "name": cluster.get("name", ""),
                    "total_mentions": total_mentions,
                    "percentage": percentage,
                    "unique_dialogs": len(cluster.get("dialog_ids", [])),
                    "variants_json": json.dumps(cluster.get("variants", []), ensure_ascii=False),
                    "slices_json": json.dumps(cluster.get("slices", {}), ensure_ascii=False),
                    "dialog_ids_json": json.dumps(cluster.get("dialog_ids", []), ensure_ascii=False)
                })
            
            idea_df = pd.DataFrame(idea_data)
            idea_df.to_excel(writer, sheet_name='ideas', index=False)
            sheets_created = True
        
        # Сигналы
        if results.get("signals"):
            signal_data = []
            for i, cluster in enumerate(results["signals"], 1):
                total_mentions = cluster.get("mentions_abs", 0)
                percentage = cluster.get("mentions_pct_of_D", 0)
                
                signal_data.append({
                    "cluster_id": i,
                    "name": cluster.get("name", ""),
                    "total_mentions": total_mentions,
                    "percentage": percentage,
                    "unique_dialogs": len(cluster.get("dialog_ids", [])),
                    "variants_json": json.dumps(cluster.get("variants", []), ensure_ascii=False),
                    "slices_json": json.dumps(cluster.get("slices", {}), ensure_ascii=False),
                    "dialog_ids_json": json.dumps(cluster.get("dialog_ids", []), ensure_ascii=False)
                })
            
            signal_df = pd.DataFrame(signal_data)
            signal_df.to_excel(writer, sheet_name='signals', index=False)
            sheets_created = True
        
        # Если не создано ни одного листа, создаем пустой
        if not sheets_created:
            empty_df = pd.DataFrame({'message': ['Нет данных для отображения']})
            empty_df.to_excel(writer, sheet_name='summary', index=False)
    
    return excel_file


def main():
    """Главная функция Stage 6"""
    logger.info("🚀 Stage 6: Генерация отчетов")
    
    # Создание папки reports
    reports_dir = Path(settings.output_dir)
    reports_dir.mkdir(exist_ok=True)
    
    # Загрузка результатов агрегации
    results = load_aggregate_results()
    if not results:
        return
    
    # Генерация Markdown отчета
    logger.info("📝 Генерация Markdown отчета...")
    markdown_content = generate_markdown_report(results)
    
    markdown_file = reports_dir / "report.md"
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    logger.info(f"💾 Сохранен Markdown отчет: {markdown_file}")
    
    # Генерация приложения с ID
    logger.info("📝 Генерация приложения с ID...")
    appendix_content = generate_appendix_ids(results)
    
    appendix_file = reports_dir / "appendix_ids.md"
    with open(appendix_file, 'w', encoding='utf-8') as f:
        f.write(appendix_content)
    
    logger.info(f"💾 Сохранено приложение с ID: {appendix_file}")
    
    # Генерация Excel отчета
    logger.info("📊 Генерация Excel отчета...")
    excel_file = generate_excel_report(results)
    
    logger.info(f"💾 Сохранен Excel отчет: {excel_file}")
    
    # Проверки валидности данных
    logger.info("🔍 Проверка валидности данных...")
    all_errors = []
    
    for category in ["barriers", "ideas", "signals"]:
        clusters = results.get(category, [])
        for i, cluster in enumerate(clusters, 1):
            errors = validate_cluster_data(cluster, i)
            all_errors.extend(errors)
    
    if all_errors:
        logger.warning("⚠️ Найдены ошибки валидности:")
        for error in all_errors:
            logger.warning(f"  {error}")
    else:
        logger.info("✅ Все проверки валидности пройдены")
    
    # Статистика
    meta = results.get("meta", {})
    N = meta.get("N", 0)
    D = meta.get("D", 0)
    
    logger.info("📊 Статистика отчетов:")
    logger.info(f"  Кластеров барьеров: {len(results.get('barriers', []))}")
    logger.info(f"  Кластеров идей: {len(results.get('ideas', []))}")
    logger.info(f"  Кластеров сигналов доставки: {len(results.get('signals', []))}")
    logger.info(f"  Кластеров сигналов платформы: {len(results.get('signals_platform', []))}")
    logger.info(f"  Всего диалогов (N): {N}")
    logger.info(f"  С доставкой (D): {D} из {N} ({100.0*D/N:.1f}%)")
    
    logger.info("✅ Stage 6 завершен успешно!")
    logger.info(f"📁 Отчеты сохранены в папке: {reports_dir}")


if __name__ == "__main__":
    main()
