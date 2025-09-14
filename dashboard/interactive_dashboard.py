#!/usr/bin/env python3
"""
Интерактивный дашборд для анализа диалогов
Создает HTML дашборд с интерактивными графиками и таблицами
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_interactive_dashboard(results: Dict[str, Any], 
                                 enhanced_clusters: Dict[str, Any] = None,
                                 quality_metrics: Dict[str, Any] = None) -> str:
    """Генерация интерактивного HTML дашборда"""
    
    # Извлекаем метаданные
    meta = results.get("meta", {})
    N = meta.get("N", 0)
    D = meta.get("D", 0)
    
    # Подготавливаем данные для графиков
    sentiment_data = calculate_sentiment_distribution(results)
    priority_data = calculate_priority_matrix(results, enhanced_clusters)
    trend_data = calculate_trend_analysis(results, enhanced_clusters)
    
    # Генерируем HTML
    html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Анализ диалогов - Интерактивный дашборд</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 30px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 20px;
        }}
        .metric-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 10px;
            padding: 20px;
            margin: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .metric-card h3 {{
            margin: 0 0 10px 0;
            font-size: 1.2em;
        }}
        .metric-card .value {{
            font-size: 2em;
            font-weight: bold;
            margin: 10px 0;
        }}
        .metric-card .label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .chart-container {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 20px 0;
        }}
        .chart-title {{
            font-size: 1.3em;
            font-weight: bold;
            margin-bottom: 15px;
            color: #333;
        }}
        .priority-high {{ border-left: 4px solid #dc3545; }}
        .priority-medium {{ border-left: 4px solid #ffc107; }}
        .priority-low {{ border-left: 4px solid #28a745; }}
        .cluster-card {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            margin: 10px 0;
            border-left: 4px solid #007bff;
        }}
        .cluster-name {{
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }}
        .cluster-metrics {{
            font-size: 0.9em;
            color: #666;
        }}
        .solutions {{
            background: #e8f5e8;
            border-radius: 5px;
            padding: 10px;
            margin-top: 10px;
        }}
        .solutions h4 {{
            margin: 0 0 10px 0;
            color: #2d5a2d;
        }}
        .solutions ul {{
            margin: 0;
            padding-left: 20px;
        }}
        .quality-score {{
            font-size: 3em;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
        }}
        .quality-excellent {{ color: #28a745; }}
        .quality-good {{ color: #ffc107; }}
        .quality-poor {{ color: #dc3545; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Анализ диалогов по доставке</h1>
            <p>Интерактивный дашборд с результатами анализа</p>
        </div>
        
        <!-- Ключевые метрики -->
        <div class="grid">
            <div class="metric-card">
                <h3>Охват анализа</h3>
                <div class="value">{D}</div>
                <div class="label">из {N} диалогов ({100*D/N:.1f}%)</div>
            </div>
            
            <div class="metric-card">
                <h3>Выявленные проблемы</h3>
                <div class="value">{len(results.get('barriers', []))}</div>
                <div class="label">барьеров</div>
            </div>
            
            <div class="metric-card">
                <h3>Идеи пользователей</h3>
                <div class="value">{len(results.get('ideas', []))}</div>
                <div class="label">предложений</div>
            </div>
            
            <div class="metric-card">
                <h3>Сигналы</h3>
                <div class="value">{len(results.get('signals', []))}</div>
                <div class="label">сигналов</div>
            </div>
        </div>
        
        <!-- Общий балл качества -->
        {generate_quality_score_section(quality_metrics)}
        
        <!-- График распределения тональности -->
        <div class="chart-container">
            <div class="chart-title">📈 Распределение эмоциональных состояний</div>
            <div id="sentiment-chart"></div>
        </div>
        
        <!-- Матрица приоритетов -->
        <div class="chart-container">
            <div class="chart-title">🎯 Матрица приоритетов кластеров</div>
            <div id="priority-matrix"></div>
        </div>
        
        <!-- Анализ трендов -->
        <div class="chart-container">
            <div class="chart-title">📊 Анализ трендов</div>
            <div id="trend-analysis"></div>
        </div>
        
        <!-- Детальный анализ кластеров -->
        <div class="chart-container">
            <div class="chart-title">🔍 Детальный анализ кластеров</div>
            {generate_clusters_analysis(results, enhanced_clusters)}
        </div>
        
        <!-- Рекомендации -->
        <div class="chart-container">
            <div class="chart-title">💡 Рекомендации по улучшению</div>
            {generate_recommendations_section(results, enhanced_clusters, quality_metrics)}
        </div>
    </div>
    
    <script>
        // График распределения тональности
        var sentimentData = {json.dumps(sentiment_data)};
        Plotly.newPlot('sentiment-chart', sentimentData.data, sentimentData.layout);
        
        // Матрица приоритетов
        var priorityData = {json.dumps(priority_data)};
        Plotly.newPlot('priority-matrix', priorityData.data, priorityData.layout);
        
        // Анализ трендов
        var trendData = {json.dumps(trend_data)};
        Plotly.newPlot('trend-analysis', trendData.data, trendData.layout);
    </script>
</body>
</html>
"""
    
    return html_content

def generate_quality_score_section(quality_metrics: Dict[str, Any]) -> str:
    """Генерация секции с общим баллом качества"""
    
    if not quality_metrics:
        return ""
    
    overall_score = quality_metrics.get("overall_quality_score", 0)
    
    if overall_score >= 0.8:
        quality_class = "quality-excellent"
        quality_text = "Отличное качество"
    elif overall_score >= 0.6:
        quality_class = "quality-good"
        quality_text = "Хорошее качество"
    else:
        quality_class = "quality-poor"
        quality_text = "Требует улучшения"
    
    return f"""
    <div class="quality-score {quality_class}">
        <div>{overall_score:.2f}</div>
        <div style="font-size: 0.5em;">{quality_text}</div>
    </div>
    """

def generate_clusters_analysis(results: Dict[str, Any], enhanced_clusters: Dict[str, Any] = None) -> str:
    """Генерация детального анализа кластеров"""
    
    html = ""
    
    # Анализ барьеров
    barriers = results.get("barriers", [])
    if barriers:
        html += "<h3>🚧 Барьеры</h3>"
        for i, barrier in enumerate(barriers[:5], 1):  # Показываем топ-5
            name = barrier.get("name", f"Барьер {i}")
            mentions = barrier.get("mentions_abs", 0)
            pct = barrier.get("mentions_pct_of_D", 0)
            
            # Получаем дополнительную информацию из enhanced_clusters
            priority = "средний"
            solutions = []
            if enhanced_clusters and "barriers" in enhanced_clusters:
                for enhanced_barrier in enhanced_clusters["barriers"]:
                    if enhanced_barrier.get("name") == name:
                        priority = enhanced_barrier.get("priority", "средний")
                        solutions = enhanced_barrier.get("solutions", [])
                        break
            
            priority_class = f"priority-{priority}"
            
            html += f"""
            <div class="cluster-card {priority_class}">
                <div class="cluster-name">{i}. {name}</div>
                <div class="cluster-metrics">
                    Упоминаний: {mentions} ({pct:.1f}% от доставочных)
                </div>
                {generate_solutions_html(solutions)}
            </div>
            """
    
    # Анализ идей
    ideas = results.get("ideas", [])
    if ideas:
        html += "<h3>💡 Идеи пользователей</h3>"
        for i, idea in enumerate(ideas[:5], 1):
            name = idea.get("name", f"Идея {i}")
            mentions = idea.get("mentions_abs", 0)
            pct = idea.get("mentions_pct_of_D", 0)
            
            html += f"""
            <div class="cluster-card">
                <div class="cluster-name">{i}. {name}</div>
                <div class="cluster-metrics">
                    Упоминаний: {mentions} ({pct:.1f}% от доставочных)
                </div>
            </div>
            """
    
    return html

def generate_solutions_html(solutions: List[str]) -> str:
    """Генерация HTML для предложений по решению"""
    
    if not solutions:
        return ""
    
    solutions_list = "".join([f"<li>{solution}</li>" for solution in solutions])
    
    return f"""
    <div class="solutions">
        <h4>💡 Предложения по решению:</h4>
        <ul>{solutions_list}</ul>
    </div>
    """

def generate_recommendations_section(results: Dict[str, Any], 
                                   enhanced_clusters: Dict[str, Any] = None,
                                   quality_metrics: Dict[str, Any] = None) -> str:
    """Генерация секции с рекомендациями"""
    
    recommendations = []
    
    # Рекомендации на основе качества
    if quality_metrics:
        overall_score = quality_metrics.get("overall_quality_score", 0)
        
        if overall_score < 0.6:
            recommendations.append("🔧 Улучшить качество извлечения сущностей")
        
        extraction_quality = quality_metrics.get("extraction_quality", {})
        if extraction_quality.get("f1_score", 0) < 0.7:
            recommendations.append("📝 Пересмотреть промпты для извлечения")
        
        clustering_quality = quality_metrics.get("clustering_quality", {})
        if clustering_quality.get("duplicate_rate", 0) > 0.2:
            recommendations.append("🔄 Улучшить алгоритм кластеризации")
    
    # Рекомендации на основе кластеров
    barriers = results.get("barriers", [])
    if barriers:
        high_priority_barriers = [b for b in barriers if b.get("mentions_abs", 0) > 2]
        if high_priority_barriers:
            recommendations.append(f"⚡ Приоритетно решить {len(high_priority_barriers)} критических барьеров")
    
    # Рекомендации на основе контекстного анализа
    if enhanced_clusters and "trend_analysis" in enhanced_clusters:
        trend_analysis = enhanced_clusters["trend_analysis"]
        high_impact_clusters = trend_analysis.get("high_impact_clusters", 0)
        if high_impact_clusters > 0:
            recommendations.append(f"🎯 Сосредоточиться на {high_impact_clusters} кластерах с высоким влиянием")
    
    if not recommendations:
        recommendations = ["✅ Анализ выполнен качественно, продолжайте мониторинг"]
    
    recommendations_html = "".join([f"<li>{rec}</li>" for rec in recommendations])
    
    return f"""
    <ul style="font-size: 1.1em; line-height: 1.6;">
        {recommendations_html}
    </ul>
    """

def calculate_sentiment_distribution(results: Dict[str, Any]) -> Dict[str, Any]:
    """Вычисление распределения тональности для графика"""
    
    sentiment_counts = {}
    
    for category in ["barriers", "ideas", "signals"]:
        clusters = results.get(category, [])
        for cluster in clusters:
            sentiment = cluster.get("slices", {}).get("sentiment", {})
            for emotion, count in sentiment.items():
                if emotion not in sentiment_counts:
                    sentiment_counts[emotion] = 0
                sentiment_counts[emotion] += count
    
    if not sentiment_counts:
        sentiment_counts = {"нейтрально": 1}
    
    return {
        "data": [{
            "values": list(sentiment_counts.values()),
            "labels": list(sentiment_counts.keys()),
            "type": "pie",
            "marker": {
                "colors": ["#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4", "#feca57"]
            }
        }],
        "layout": {
            "title": "Распределение эмоциональных состояний",
            "font": {"size": 12}
        }
    }

def calculate_priority_matrix(results: Dict[str, Any], enhanced_clusters: Dict[str, Any] = None) -> Dict[str, Any]:
    """Вычисление матрицы приоритетов для графика"""
    
    priorities = {"высокий": 0, "средний": 0, "низкий": 0}
    
    if enhanced_clusters:
        for category in ["barriers", "ideas", "signals"]:
            clusters = enhanced_clusters.get(category, [])
            for cluster in clusters:
                priority = cluster.get("priority", "средний")
                if priority in priorities:
                    priorities[priority] += 1
    
    return {
        "data": [{
            "x": list(priorities.keys()),
            "y": list(priorities.values()),
            "type": "bar",
            "marker": {
                "color": ["#dc3545", "#ffc107", "#28a745"]
            }
        }],
        "layout": {
            "title": "Распределение приоритетов кластеров",
            "xaxis": {"title": "Приоритет"},
            "yaxis": {"title": "Количество кластеров"},
            "font": {"size": 12}
        }
    }

def calculate_trend_analysis(results: Dict[str, Any], enhanced_clusters: Dict[str, Any] = None) -> Dict[str, Any]:
    """Вычисление данных для анализа трендов"""
    
    # Простой анализ трендов на основе количества упоминаний
    categories = ["barriers", "ideas", "signals"]
    category_counts = []
    
    for category in categories:
        clusters = results.get(category, [])
        total_mentions = sum(cluster.get("mentions_abs", 0) for cluster in clusters)
        category_counts.append(total_mentions)
    
    return {
        "data": [{
            "x": categories,
            "y": category_counts,
            "type": "scatter",
            "mode": "lines+markers",
            "line": {"color": "#007bff", "width": 3},
            "marker": {"size": 10}
        }],
        "layout": {
            "title": "Тренды по категориям",
            "xaxis": {"title": "Категория"},
            "yaxis": {"title": "Количество упоминаний"},
            "font": {"size": 12}
        }
    }

def main():
    """Основная функция"""
    logger.info("🚀 Генерация интерактивного дашборда")
    
    # Загружаем данные
    results_file = "artifacts/aggregate_results.json"
    enhanced_clusters_file = "artifacts/stage4_5_semantic_enrichment.json"
    quality_metrics_file = "reports/quality_enhanced.json"
    
    results = {}
    enhanced_clusters = {}
    quality_metrics = {}
    
    if Path(results_file).exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
    
    if Path(enhanced_clusters_file).exists():
        with open(enhanced_clusters_file, 'r', encoding='utf-8') as f:
            enhanced_clusters = json.load(f)
    
    if Path(quality_metrics_file).exists():
        with open(quality_metrics_file, 'r', encoding='utf-8') as f:
            quality_metrics = json.load(f)
    
    # Генерируем дашборд
    dashboard_html = generate_interactive_dashboard(results, enhanced_clusters, quality_metrics)
    
    # Сохраняем дашборд
    output_file = "reports/interactive_dashboard.html"
    Path("reports").mkdir(exist_ok=True, parents=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(dashboard_html)
    
    logger.info(f"✅ Интерактивный дашборд сохранен: {output_file}")

if __name__ == "__main__":
    main()
