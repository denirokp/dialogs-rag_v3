#!/usr/bin/env python3
"""
Quality Monitoring and Dashboard System
Система мониторинга качества и дашборд
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, Counter
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as pyo
from jinja2 import Template
import webbrowser
import threading
import time

logger = logging.getLogger(__name__)

@dataclass
class QualityMetric:
    """Метрика качества"""
    name: str
    value: float
    timestamp: datetime
    threshold: float
    status: str  # 'good', 'warning', 'critical'
    trend: str  # 'improving', 'stable', 'declining'
    metadata: Dict[str, Any] = None

@dataclass
class QualityAlert:
    """Алерт о качестве"""
    alert_id: str
    metric_name: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    message: str
    timestamp: datetime
    resolved: bool = False
    resolution_notes: str = ""

@dataclass
class PerformanceSnapshot:
    """Снимок производительности"""
    timestamp: datetime
    total_dialogs_processed: int
    avg_quality_score: float
    processing_time_avg: float
    error_rate: float
    throughput_per_hour: float
    active_prompts: Dict[str, int]
    quality_distribution: Dict[str, int]

class QualityMonitor:
    """Монитор качества системы"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics_history: List[QualityMetric] = []
        self.alerts: List[QualityAlert] = []
        self.performance_snapshots: List[PerformanceSnapshot] = []
        
        # Пороги для алертов
        self.alert_thresholds = {
            'avg_quality_score': {'warning': 0.7, 'critical': 0.5},
            'processing_time': {'warning': 5.0, 'critical': 10.0},
            'error_rate': {'warning': 0.05, 'critical': 0.1},
            'throughput': {'warning': 50, 'critical': 20}
        }
        
        # Настройки мониторинга
        self.monitoring_config = {
            'snapshot_interval_minutes': 15,
            'alert_cooldown_minutes': 30,
            'max_history_days': 30,
            'auto_cleanup': True
        }
        
        # Запускаем фоновый мониторинг
        self._start_background_monitoring()
    
    def _start_background_monitoring(self):
        """Запуск фонового мониторинга"""
        def monitor_loop():
            while True:
                try:
                    self._take_performance_snapshot()
                    self._check_quality_alerts()
                    self._cleanup_old_data()
                    time.sleep(self.monitoring_config['snapshot_interval_minutes'] * 60)
                except Exception as e:
                    logger.error(f"Ошибка фонового мониторинга: {e}")
                    time.sleep(60)  # Ждем минуту при ошибке
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logger.info("Фоновый мониторинг запущен")
    
    def record_processing_result(self, dialog: str, extracted_entities: Dict[str, List[str]], 
                               quality_score: float, processing_time: float, 
                               prompt_variant: str = "unknown", error: str = None):
        """Запись результата обработки"""
        
        # Записываем метрики
        self._record_quality_metric("quality_score", quality_score)
        self._record_quality_metric("processing_time", processing_time)
        
        if error:
            self._record_quality_metric("error_rate", 1.0)
        else:
            self._record_quality_metric("error_rate", 0.0)
        
        # Записываем распределение качества
        quality_grade = self._get_quality_grade(quality_score)
        self._record_quality_metric(f"quality_grade_{quality_grade}", 1.0)
        
        # Записываем метрики по цитатам
        quotes = extracted_entities.get('quotes', [])
        if quotes:
            avg_quote_length = np.mean([len(q) for q in quotes])
            self._record_quality_metric("avg_quote_length", avg_quote_length)
            
            # Проверяем качество цитат
            garbage_quotes = sum(1 for q in quotes if self._is_garbage_quote(q))
            garbage_rate = garbage_quotes / len(quotes) if quotes else 0
            self._record_quality_metric("quote_garbage_rate", garbage_rate)
        
        # Проверяем алерты
        self._check_quality_alerts()
    
    def _record_quality_metric(self, metric_name: str, value: float):
        """Запись метрики качества"""
        # Определяем статус метрики
        status = self._get_metric_status(metric_name, value)
        
        # Определяем тренд
        trend = self._calculate_metric_trend(metric_name, value)
        
        metric = QualityMetric(
            name=metric_name,
            value=value,
            timestamp=datetime.now(),
            threshold=self.alert_thresholds.get(metric_name, {}).get('warning', 0.5),
            status=status,
            trend=trend
        )
        
        self.metrics_history.append(metric)
        
        # Ограничиваем историю
        if self.monitoring_config['auto_cleanup']:
            cutoff_date = datetime.now() - timedelta(days=self.monitoring_config['max_history_days'])
            self.metrics_history = [m for m in self.metrics_history if m.timestamp > cutoff_date]
    
    def _get_metric_status(self, metric_name: str, value: float) -> str:
        """Получение статуса метрики"""
        thresholds = self.alert_thresholds.get(metric_name, {})
        
        if not thresholds:
            return 'good'
        
        if value <= thresholds.get('critical', float('inf')):
            return 'critical'
        elif value <= thresholds.get('warning', float('inf')):
            return 'warning'
        else:
            return 'good'
    
    def _calculate_metric_trend(self, metric_name: str, current_value: float) -> str:
        """Вычисление тренда метрики"""
        # Получаем последние 10 значений этой метрики
        recent_metrics = [m for m in self.metrics_history 
                         if m.name == metric_name][-10:]
        
        if len(recent_metrics) < 3:
            return 'stable'
        
        values = [m.value for m in recent_metrics]
        
        # Простой анализ тренда
        if len(values) >= 3:
            first_third = np.mean(values[:len(values)//3])
            last_third = np.mean(values[-len(values)//3:])
            
            change_percent = (last_third - first_third) / first_third if first_third > 0 else 0
            
            if change_percent > 0.05:
                return 'improving'
            elif change_percent < -0.05:
                return 'declining'
            else:
                return 'stable'
        
        return 'stable'
    
    def _get_quality_grade(self, quality_score: float) -> str:
        """Получение оценки качества"""
        if quality_score >= 0.9:
            return 'A+'
        elif quality_score >= 0.8:
            return 'A'
        elif quality_score >= 0.7:
            return 'B'
        elif quality_score >= 0.6:
            return 'C'
        else:
            return 'D'
    
    def _is_garbage_quote(self, quote: str) -> bool:
        """Проверка на мусор в цитате"""
        garbage_words = ['угу', 'ага', 'да', 'нет', 'хм', 'эм', 'мм']
        quote_lower = quote.lower().strip()
        
        # Проверка на повторяющиеся слова
        words = quote_lower.split()
        if len(words) > 1:
            unique_words = set(words)
            if len(unique_words) == 1 and len(words) > 2:
                return True
        
        # Проверка на мусорные слова
        if any(word in quote_lower for word in garbage_words):
            return True
        
        # Проверка на слишком короткие цитаты
        if len(quote_lower) < 10:
            return True
        
        return False
    
    def _check_quality_alerts(self):
        """Проверка алертов качества"""
        # Получаем последние метрики
        recent_metrics = [m for m in self.metrics_history 
                         if m.timestamp > datetime.now() - timedelta(minutes=5)]
        
        for metric in recent_metrics:
            if metric.status in ['warning', 'critical']:
                self._create_alert_if_needed(metric)
    
    def _create_alert_if_needed(self, metric: QualityMetric):
        """Создание алерта при необходимости"""
        # Проверяем, не создавали ли мы недавно алерт для этой метрики
        recent_alerts = [a for a in self.alerts 
                        if a.metric_name == metric.name and 
                        a.timestamp > datetime.now() - timedelta(minutes=self.monitoring_config['alert_cooldown_minutes'])]
        
        if recent_alerts:
            return  # Не создаем дублирующие алерты
        
        # Создаем алерт
        alert_id = f"{metric.name}_{int(metric.timestamp.timestamp())}"
        severity = 'high' if metric.status == 'critical' else 'medium'
        
        message = f"Метрика {metric.name} = {metric.value:.2f} ({metric.status})"
        
        alert = QualityAlert(
            alert_id=alert_id,
            metric_name=metric.name,
            severity=severity,
            message=message,
            timestamp=datetime.now()
        )
        
        self.alerts.append(alert)
        logger.warning(f"Создан алерт: {message}")
    
    def _take_performance_snapshot(self):
        """Создание снимка производительности"""
        # Получаем метрики за последний час
        hour_ago = datetime.now() - timedelta(hours=1)
        recent_metrics = [m for m in self.metrics_history if m.timestamp > hour_ago]
        
        if not recent_metrics:
            return
        
        # Группируем по типам метрик
        metrics_by_name = defaultdict(list)
        for metric in recent_metrics:
            metrics_by_name[metric.name].append(metric.value)
        
        # Вычисляем средние значения
        avg_quality = np.mean(metrics_by_name.get('quality_score', [0]))
        avg_processing_time = np.mean(metrics_by_name.get('processing_time', [0]))
        error_rate = np.mean(metrics_by_name.get('error_rate', [0]))
        
        # Вычисляем пропускную способность
        total_dialogs = len([m for m in recent_metrics if m.name == 'quality_score'])
        throughput_per_hour = total_dialogs
        
        # Распределение качества
        quality_distribution = Counter()
        for metric in recent_metrics:
            if metric.name.startswith('quality_grade_'):
                grade = metric.name.replace('quality_grade_', '')
                quality_distribution[grade] += int(metric.value)
        
        # Активные промпты (заглушка)
        active_prompts = {'base': 50, 'detailed': 30, 'contextual': 20}
        
        snapshot = PerformanceSnapshot(
            timestamp=datetime.now(),
            total_dialogs_processed=total_dialogs,
            avg_quality_score=avg_quality,
            processing_time_avg=avg_processing_time,
            error_rate=error_rate,
            throughput_per_hour=throughput_per_hour,
            active_prompts=active_prompts,
            quality_distribution=dict(quality_distribution)
        )
        
        self.performance_snapshots.append(snapshot)
        
        # Ограничиваем историю снимков
        if len(self.performance_snapshots) > 1000:
            self.performance_snapshots = self.performance_snapshots[-500:]
    
    def _cleanup_old_data(self):
        """Очистка старых данных"""
        if not self.monitoring_config['auto_cleanup']:
            return
        
        cutoff_date = datetime.now() - timedelta(days=self.monitoring_config['max_history_days'])
        
        # Очищаем старые метрики
        self.metrics_history = [m for m in self.metrics_history if m.timestamp > cutoff_date]
        
        # Очищаем старые алерты
        self.alerts = [a for a in self.alerts if a.timestamp > cutoff_date]
        
        # Очищаем старые снимки
        self.performance_snapshots = [s for s in self.performance_snapshots if s.timestamp > cutoff_date]
    
    def get_quality_dashboard_data(self) -> Dict[str, Any]:
        """Получение данных для дашборда качества"""
        now = datetime.now()
        
        # Текущие метрики
        current_metrics = {}
        for metric_name in ['quality_score', 'processing_time', 'error_rate', 'throughput']:
            recent_metrics = [m for m in self.metrics_history 
                            if m.name == metric_name and 
                            m.timestamp > now - timedelta(hours=1)]
            
            if recent_metrics:
                current_metrics[metric_name] = {
                    'value': np.mean([m.value for m in recent_metrics]),
                    'status': recent_metrics[-1].status,
                    'trend': recent_metrics[-1].trend
                }
        
        # Активные алерты
        active_alerts = [a for a in self.alerts if not a.resolved]
        
        # Тренды за последние 24 часа
        trends_24h = self._get_trends_data(hours=24)
        
        # Распределение качества
        quality_distribution = self._get_quality_distribution()
        
        # Производительность по времени
        performance_timeline = self._get_performance_timeline(hours=24)
        
        return {
            'current_metrics': current_metrics,
            'active_alerts': [asdict(a) for a in active_alerts],
            'trends_24h': trends_24h,
            'quality_distribution': quality_distribution,
            'performance_timeline': performance_timeline,
            'last_updated': now.isoformat()
        }
    
    def _get_trends_data(self, hours: int) -> Dict[str, Any]:
        """Получение данных трендов"""
        cutoff = datetime.now() - timedelta(hours=hours)
        recent_metrics = [m for m in self.metrics_history if m.timestamp > cutoff]
        
        trends = {}
        for metric_name in ['quality_score', 'processing_time', 'error_rate']:
            metric_data = [m for m in recent_metrics if m.name == metric_name]
            if metric_data:
                values = [m.value for m in metric_data]
                timestamps = [m.timestamp for m in metric_data]
                
                trends[metric_name] = {
                    'values': values,
                    'timestamps': [t.isoformat() for t in timestamps],
                    'trend': self._calculate_trend_direction(values)
                }
        
        return trends
    
    def _calculate_trend_direction(self, values: List[float]) -> str:
        """Вычисление направления тренда"""
        if len(values) < 2:
            return 'stable'
        
        # Простая линейная регрессия
        x = np.arange(len(values))
        y = np.array(values)
        
        if len(values) > 1:
            slope = np.polyfit(x, y, 1)[0]
            if slope > 0.01:
                return 'improving'
            elif slope < -0.01:
                return 'declining'
            else:
                return 'stable'
        
        return 'stable'
    
    def _get_quality_distribution(self) -> Dict[str, int]:
        """Получение распределения качества"""
        recent_metrics = [m for m in self.metrics_history 
                         if m.timestamp > datetime.now() - timedelta(hours=24)]
        
        distribution = Counter()
        for metric in recent_metrics:
            if metric.name.startswith('quality_grade_'):
                grade = metric.name.replace('quality_grade_', '')
                distribution[grade] += int(metric.value)
        
        return dict(distribution)
    
    def _get_performance_timeline(self, hours: int) -> Dict[str, List]:
        """Получение временной линии производительности"""
        cutoff = datetime.now() - timedelta(hours=hours)
        recent_snapshots = [s for s in self.performance_snapshots if s.timestamp > cutoff]
        
        if not recent_snapshots:
            return {'timestamps': [], 'quality_scores': [], 'throughput': []}
        
        timestamps = [s.timestamp.isoformat() for s in recent_snapshots]
        quality_scores = [s.avg_quality_score for s in recent_snapshots]
        throughput = [s.throughput_per_hour for s in recent_snapshots]
        
        return {
            'timestamps': timestamps,
            'quality_scores': quality_scores,
            'throughput': throughput
        }
    
    def generate_html_dashboard(self, output_path: str = "quality_dashboard.html"):
        """Генерация HTML дашборда"""
        dashboard_data = self.get_quality_dashboard_data()
        
        # HTML шаблон
        html_template = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quality Dashboard - Dialogs RAG</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            text-align: center;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border-left: 4px solid #667eea;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }
        .metric-status {
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            font-weight: bold;
        }
        .status-good { background-color: #d4edda; color: #155724; }
        .status-warning { background-color: #fff3cd; color: #856404; }
        .status-critical { background-color: #f8d7da; color: #721c24; }
        .alerts-section {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .alert-item {
            padding: 10px;
            margin: 10px 0;
            border-radius: 5px;
            border-left: 4px solid #dc3545;
        }
        .alert-high { background-color: #f8d7da; }
        .alert-medium { background-color: #fff3cd; }
        .chart-container {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Quality Dashboard - Dialogs RAG</h1>
        <p>Мониторинг качества анализа диалогов в реальном времени</p>
        <p>Последнее обновление: {{ last_updated }}</p>
    </div>

    <div class="metrics-grid">
        {% for metric_name, metric_data in current_metrics.items() %}
        <div class="metric-card">
            <h3>{{ metric_name.replace('_', ' ').title() }}</h3>
            <div class="metric-value">{{ "%.2f"|format(metric_data.value) }}</div>
            <div class="metric-status status-{{ metric_data.status }}">
                {{ metric_data.status.upper() }} - {{ metric_data.trend.upper() }}
            </div>
        </div>
        {% endfor %}
    </div>

    {% if active_alerts %}
    <div class="alerts-section">
        <h2>🚨 Активные алерты ({{ active_alerts|length }})</h2>
        {% for alert in active_alerts %}
        <div class="alert-item alert-{{ alert.severity }}">
            <strong>{{ alert.metric_name }}:</strong> {{ alert.message }}
            <br><small>{{ alert.timestamp }}</small>
        </div>
        {% endfor %}
    </div>
    {% endif %}

    <div class="chart-container">
        <h2>📊 Тренды качества (24 часа)</h2>
        <div id="quality-trends-chart"></div>
    </div>

    <div class="chart-container">
        <h2>📈 Распределение качества</h2>
        <div id="quality-distribution-chart"></div>
    </div>

    <div class="chart-container">
        <h2>⚡ Производительность</h2>
        <div id="performance-chart"></div>
    </div>

    <script>
        // Данные для графиков
        const dashboardData = {{ dashboard_data | tojson }};
        
        // График трендов качества
        if (dashboardData.trends_24h.quality_score) {
            const qualityTrend = {
                x: dashboardData.trends_24h.quality_score.timestamps,
                y: dashboardData.trends_24h.quality_score.values,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Качество',
                line: { color: '#667eea' }
            };
            
            Plotly.newPlot('quality-trends-chart', [qualityTrend], {
                title: 'Тренд качества во времени',
                xaxis: { title: 'Время' },
                yaxis: { title: 'Оценка качества' }
            });
        }
        
        // График распределения качества
        const qualityDist = {
            labels: Object.keys(dashboardData.quality_distribution),
            values: Object.values(dashboardData.quality_distribution),
            type: 'pie',
            marker: {
                colors: ['#28a745', '#ffc107', '#fd7e14', '#dc3545']
            }
        };
        
        Plotly.newPlot('quality-distribution-chart', [qualityDist], {
            title: 'Распределение оценок качества'
        });
        
        // График производительности
        if (dashboardData.performance_timeline.timestamps.length > 0) {
            const performanceTrace = {
                x: dashboardData.performance_timeline.timestamps,
                y: dashboardData.performance_timeline.quality_scores,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Качество',
                yaxis: 'y1'
            };
            
            const throughputTrace = {
                x: dashboardData.performance_timeline.timestamps,
                y: dashboardData.performance_timeline.throughput,
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Пропускная способность',
                yaxis: 'y2'
            };
            
            Plotly.newPlot('performance-chart', [performanceTrace, throughputTrace], {
                title: 'Производительность системы',
                xaxis: { title: 'Время' },
                yaxis: { title: 'Качество', side: 'left' },
                yaxis2: { title: 'Диалогов/час', side: 'right', overlaying: 'y' }
            });
        }
        
        // Автообновление каждые 5 минут
        setInterval(() => {
            location.reload();
        }, 300000);
    </script>
</body>
</html>
        """
        
        template = Template(html_template)
        html_content = template.render(**dashboard_data)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Дашборд сохранен: {output_path}")
        return output_path
    
    def open_dashboard(self, output_path: str = "quality_dashboard.html"):
        """Открытие дашборда в браузере"""
        full_path = Path(output_path).absolute()
        webbrowser.open(f"file://{full_path}")
        logger.info(f"Дашборд открыт: {full_path}")
    
    def get_quality_report(self) -> Dict[str, Any]:
        """Получение отчета о качестве"""
        dashboard_data = self.get_quality_dashboard_data()
        
        # Дополнительная аналитика
        total_dialogs = sum(s.total_dialogs_processed for s in self.performance_snapshots)
        avg_quality = np.mean([s.avg_quality_score for s in self.performance_snapshots]) if self.performance_snapshots else 0
        
        # Рекомендации
        recommendations = self._generate_recommendations()
        
        return {
            **dashboard_data,
            'summary': {
                'total_dialogs_processed': total_dialogs,
                'overall_avg_quality': avg_quality,
                'active_alerts_count': len(dashboard_data['active_alerts']),
                'system_health': self._get_system_health_score()
            },
            'recommendations': recommendations
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Генерация рекомендаций по улучшению"""
        recommendations = []
        
        # Анализируем текущие метрики
        recent_metrics = [m for m in self.metrics_history 
                         if m.timestamp > datetime.now() - timedelta(hours=24)]
        
        if not recent_metrics:
            return ["Недостаточно данных для анализа"]
        
        # Проверяем качество
        quality_metrics = [m for m in recent_metrics if m.name == 'quality_score']
        if quality_metrics:
            avg_quality = np.mean([m.value for m in quality_metrics])
            if avg_quality < 0.7:
                recommendations.append("Качество анализа низкое. Рекомендуется улучшить промпты или добавить больше обучающих данных.")
        
        # Проверяем время обработки
        time_metrics = [m for m in recent_metrics if m.name == 'processing_time']
        if time_metrics:
            avg_time = np.mean([m.value for m in time_metrics])
            if avg_time > 5.0:
                recommendations.append("Время обработки высокое. Рекомендуется оптимизировать промпты или использовать более быстрые модели.")
        
        # Проверяем ошибки
        error_metrics = [m for m in recent_metrics if m.name == 'error_rate']
        if error_metrics:
            avg_errors = np.mean([m.value for m in error_metrics])
            if avg_errors > 0.05:
                recommendations.append("Высокий уровень ошибок. Рекомендуется проверить стабильность системы и добавить обработку ошибок.")
        
        # Проверяем алерты
        if len(self.alerts) > 5:
            recommendations.append("Много активных алертов. Рекомендуется провести анализ и исправить критические проблемы.")
        
        if not recommendations:
            recommendations.append("Система работает стабильно. Продолжайте мониторинг.")
        
        return recommendations
    
    def _get_system_health_score(self) -> float:
        """Получение общего индекса здоровья системы"""
        if not self.performance_snapshots:
            return 0.5
        
        recent_snapshots = self.performance_snapshots[-10:]  # Последние 10 снимков
        
        # Факторы здоровья
        quality_factor = np.mean([s.avg_quality_score for s in recent_snapshots])
        error_factor = 1 - np.mean([s.error_rate for s in recent_snapshots])
        throughput_factor = min(1.0, np.mean([s.throughput_per_hour for s in recent_snapshots]) / 100)
        
        # Взвешенная оценка
        health_score = (quality_factor * 0.5 + error_factor * 0.3 + throughput_factor * 0.2)
        
        return min(1.0, max(0.0, health_score))
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Получение статистики обработки"""
        return {
            'total_dialogs_processed': len(self.metrics_history),
            'avg_quality_score': np.mean([m.value for m in self.metrics_history if m.name == 'quality_score']) if self.metrics_history else 0,
            'active_alerts': len([a for a in self.alerts if not a.resolved]),
            'system_health': self._get_system_health_score(),
            'performance_snapshots': len(self.performance_snapshots)
        }

# Пример использования
if __name__ == "__main__":
    # Тестирование системы мониторинга
    config = {
        'monitoring_enabled': True,
        'alert_email': 'admin@example.com'
    }
    
    monitor = QualityMonitor(config)
    
    # Симулируем обработку диалогов
    test_dialogs = [
        {
            "dialog": "Клиент: Здравствуйте, у меня проблема с доставкой. Менеджер: Расскажите подробнее.",
            "entities": {"problems": ["проблема с доставкой"], "quotes": ["у меня проблема с доставкой"]},
            "quality_score": 0.9,
            "processing_time": 2.5
        },
        {
            "dialog": "Угу. Угу угу угу. Угу. Угу. Угу, ну угу",
            "entities": {"problems": [], "quotes": ["Угу. Угу угу угу. Угу. Угу. Угу, ну угу"]},
            "quality_score": 0.1,
            "processing_time": 1.0
        }
    ]
    
    print("=== Система мониторинга качества ===")
    
    for i, dialog_data in enumerate(test_dialogs):
        monitor.record_processing_result(
            dialog=dialog_data["dialog"],
            extracted_entities=dialog_data["entities"],
            quality_score=dialog_data["quality_score"],
            processing_time=dialog_data["processing_time"],
            prompt_variant="test"
        )
        print(f"Обработан диалог {i+1}: качество {dialog_data['quality_score']:.2f}")
    
    # Генерируем дашборд
    dashboard_path = monitor.generate_html_dashboard("test_dashboard.html")
    print(f"Дашборд создан: {dashboard_path}")
    
    # Получаем отчет
    report = monitor.get_quality_report()
    print(f"Отчет о качестве: {report['summary']}")
    print(f"Рекомендации: {report['recommendations']}")
