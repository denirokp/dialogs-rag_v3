#!/usr/bin/env python3
"""
Pipeline Dashboard - Веб-интерфейс для управления pipeline анализа диалогов
"""

import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import time
import asyncio

# Настройка страницы
st.set_page_config(
    page_title="Dialogs RAG Pipeline Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Конфигурация API
API_BASE_URL = "http://localhost:8000"

class APIClient:
    """Клиент для работы с Pipeline API"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    def _request(self, method: str, endpoint: str, **kwargs):
        """Выполнение HTTP запроса"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Ошибка API: {e}")
            return None
    
    def get_health(self):
        """Проверка здоровья системы"""
        return self._request("GET", "/health")
    
    def run_pipeline(self, input_file: str, stages: list = None, config: dict = None):
        """Запуск pipeline"""
        params = {"input_file": input_file}
        if stages:
            params["stages"] = stages
        if config:
            params["config"] = config
        return self._request("POST", "/pipeline/run", params=params)
    
    def get_pipeline_status(self, request_id: str):
        """Получение статуса pipeline"""
        return self._request("GET", f"/pipeline/status/{request_id}")
    
    def get_pipeline_results(self, request_id: str):
        """Получение результатов pipeline"""
        return self._request("GET", f"/pipeline/results/{request_id}")
    
    def list_analyses(self, user_id: str = None, limit: int = 10):
        """Список анализов"""
        params = {"limit": limit}
        if user_id:
            params["user_id"] = user_id
        return self._request("GET", "/pipeline/analyses", params=params)
    
    def get_stage_data(self, stage_id: str):
        """Получение данных этапа"""
        return self._request("GET", f"/data/stage/{stage_id}")
    
    def get_artifacts_summary(self):
        """Сводка по артефактам"""
        return self._request("GET", "/data/artifacts")
    
    def get_available_reports(self):
        """Список доступных отчетов"""
        return self._request("GET", "/data/reports")
    
    def get_system_info(self):
        """Информация о системе"""
        return self._request("GET", "/system/info")

# Инициализация API клиента
api_client = APIClient(API_BASE_URL)

def main():
    """Главная функция дашборда"""
    
    # Заголовок
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0;">
        <h1 style="color: #2E86AB; margin-bottom: 0.5rem;">🔍 Dialogs RAG Pipeline Dashboard</h1>
        <p style="color: #666; font-size: 1.1rem;">Управление pipeline анализа диалогов</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Боковая панель
    with st.sidebar:
        st.header("🔧 Настройки")
        
        # Проверка подключения к API
        if st.button("🔄 Проверить подключение", use_container_width=True):
            health = api_client.get_health()
            if health:
                st.success("✅ API подключен")
            else:
                st.error("❌ API недоступен")
        
        # Настройки pipeline
        st.subheader("📋 Pipeline Настройки")
        
        # Выбор файла
        input_file = st.selectbox(
            "Файл для анализа",
            options=["data/dialogs.xlsx"],
            help="Выберите файл с диалогами для анализа"
        )
        
        # Выбор этапов
        st.subheader("🎯 Этапы анализа")
        stages_config = {
            "1": ("Детекция доставки", True),
            "2": ("Извлечение сущностей", True),
            "3": ("Нормализация формулировок", True),
            "4": ("Кластеризация", True),
            "5": ("Агрегация метрик", True),
            "6": ("Генерация отчетов", True)
        }
        
        selected_stages = []
        for stage_id, (stage_name, default) in stages_config.items():
            if st.checkbox(f"{stage_id}. {stage_name}", value=default, key=f"stage_{stage_id}"):
                selected_stages.append(stage_id)
        
        # Дополнительные настройки
        st.subheader("⚙️ Дополнительные настройки")
        skip_failed = st.checkbox("Продолжить при ошибках", value=False)
        parallel_execution = st.checkbox("Параллельное выполнение", value=False)
        
        # Кнопка запуска
        if st.button("🚀 Запустить Pipeline", use_container_width=True, type="primary"):
            if not selected_stages:
                st.error("Выберите хотя бы один этап")
            else:
                config = {
                    "skip_failed_stages": skip_failed,
                    "parallel_execution": parallel_execution
                }
                
                with st.spinner("Запуск pipeline..."):
                    result = api_client.run_pipeline(input_file, selected_stages, config)
                    if result:
                        st.session_state.current_analysis = result
                        st.success(f"Pipeline запущен! ID: {result['request_id']}")
                        st.rerun()
    
    # Основной контент
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏠 Главная", "📊 Аналитика", "🚀 Pipeline", "📁 Файлы", "⚙️ Система"
    ])
    
    with tab1:
        show_overview()
    
    with tab2:
        show_analytics()
    
    with tab3:
        show_pipeline_management()
    
    with tab4:
        show_files()
    
    with tab5:
        show_system_info()

def show_overview():
    """Главная страница с обзором"""
    st.header("📊 Обзор системы")
    
    # Получаем информацию о системе
    system_info = api_client.get_system_info()
    if not system_info:
        st.error("Не удалось получить информацию о системе")
        return
    
    # Метрики системы
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Активных анализов",
            system_info["pipeline_info"]["active_analyses"],
            delta=None
        )
    
    with col2:
        st.metric(
            "Всего анализов",
            system_info["pipeline_info"]["total_analyses"],
            delta=None
        )
    
    with col3:
        st.metric(
            "Артефактов",
            system_info["artifacts_summary"]["total_files"],
            delta=None
        )
    
    with col4:
        st.metric(
            "Отчетов",
            system_info["available_reports"],
            delta=None
        )
    
    # График активности
    st.subheader("📈 Активность анализов")
    
    # Получаем список анализов
    analyses = api_client.list_analyses(limit=20)
    if analyses and "analyses" in analyses:
        df_analyses = pd.DataFrame(analyses["analyses"])
        
        if not df_analyses.empty:
            # Конвертируем даты
            df_analyses["created_at"] = pd.to_datetime(df_analyses["created_at"])
            df_analyses["date"] = df_analyses["created_at"].dt.date
            
            # Группируем по дням
            daily_counts = df_analyses.groupby("date").size().reset_index(name="count")
            
            # Строим график
            fig = px.line(
                daily_counts, 
                x="date", 
                y="count",
                title="Количество анализов по дням",
                markers=True
            )
            fig.update_layout(
                xaxis_title="Дата",
                yaxis_title="Количество анализов",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Нет данных для отображения")
    else:
        st.info("Нет данных об анализах")

def show_analytics():
    """Страница аналитики"""
    st.header("📊 Аналитика результатов")
    
    # Выбор этапа для анализа
    stage_id = st.selectbox(
        "Выберите этап для анализа",
        options=["1", "2", "3", "4", "5", "6"],
        format_func=lambda x: {
            "1": "1. Детекция доставки",
            "2": "2. Извлечение сущностей",
            "3": "3. Нормализация формулировок",
            "4": "4. Кластеризация",
            "5": "5. Агрегация метрик",
            "6": "6. Генерация отчетов"
        }[x]
    )
    
    # Получаем данные этапа
    stage_data = api_client.get_stage_data(stage_id)
    if not stage_data:
        st.warning("Данные этапа не найдены")
        return
    
    st.subheader(f"Данные этапа {stage_id}")
    
    if "data" in stage_data:
        # Табличные данные
        df = pd.DataFrame(stage_data["data"])
        st.dataframe(df, use_container_width=True)
        
        # Статистика
        st.subheader("📈 Статистика")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Всего записей", stage_data["count"])
        
        with col2:
            if "delivery_discussed" in df.columns:
                delivery_rate = df["delivery_discussed"].mean() * 100
                st.metric("Процент с доставкой", f"{delivery_rate:.1f}%")
    
    elif "content" in stage_data:
        # Markdown контент
        st.markdown(stage_data["content"])
    
    else:
        st.json(stage_data)

def show_pipeline_management():
    """Управление pipeline"""
    st.header("🚀 Управление Pipeline")
    
    # Текущий анализ
    if "current_analysis" in st.session_state:
        st.subheader("🔄 Текущий анализ")
        analysis = st.session_state.current_analysis
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text(f"ID: {analysis['request_id']}")
        with col2:
            st.text(f"Статус: {analysis['status']}")
        with col3:
            if analysis['completed_at']:
                st.text(f"Завершен: {analysis['completed_at']}")
        
        # Обновление статуса
        if st.button("🔄 Обновить статус"):
            updated_status = api_client.get_pipeline_status(analysis['request_id'])
            if updated_status:
                st.session_state.current_analysis.update(updated_status)
                st.rerun()
        
        # Результаты
        if analysis['status'] == 'completed':
            st.success("✅ Анализ завершен!")
            
            if st.button("📊 Показать результаты"):
                results = api_client.get_pipeline_results(analysis['request_id'])
                if results:
                    st.json(results)
    
    # История анализов
    st.subheader("📚 История анализов")
    
    analyses = api_client.list_analyses(limit=10)
    if analyses and "analyses" in analyses:
        df_analyses = pd.DataFrame(analyses["analyses"])
        
        if not df_analyses.empty:
            # Фильтрация по статусу
            status_filter = st.selectbox(
                "Фильтр по статусу",
                options=["Все", "completed", "running", "failed"],
                key="status_filter"
            )
            
            if status_filter != "Все":
                df_analyses = df_analyses[df_analyses["status"] == status_filter]
            
            # Отображение таблицы
            st.dataframe(
                df_analyses[["request_id", "status", "message", "created_at"]],
                use_container_width=True
            )
        else:
            st.info("Нет данных об анализах")
    else:
        st.info("Не удалось загрузить историю анализов")

def show_files():
    """Управление файлами"""
    st.header("📁 Управление файлами")
    
    # Артефакты
    st.subheader("📦 Артефакты")
    artifacts_summary = api_client.get_artifacts_summary()
    if artifacts_summary:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Всего файлов", artifacts_summary["total_files"])
        with col2:
            st.metric("Общий размер", f"{artifacts_summary['total_size'] / 1024:.1f} KB")
        with col3:
            st.metric("По этапам", len(artifacts_summary["by_stage"]))
        
        # Детализация по этапам
        if artifacts_summary["by_stage"]:
            st.subheader("📊 Файлы по этапам")
            stage_data = pd.DataFrame([
                {"Этап": stage, "Файлов": count}
                for stage, count in artifacts_summary["by_stage"].items()
            ])
            st.dataframe(stage_data, use_container_width=True)
    
    # Отчеты
    st.subheader("📋 Отчеты")
    reports = api_client.get_available_reports()
    if reports:
        df_reports = pd.DataFrame(reports)
        st.dataframe(df_reports, use_container_width=True)
        
        # Кнопки скачивания
        for report in reports:
            if st.button(f"📥 Скачать {report['name']}", key=f"download_{report['name']}"):
                st.info(f"Скачивание {report['name']}...")
    else:
        st.info("Отчеты не найдены")

def show_system_info():
    """Информация о системе"""
    st.header("⚙️ Системная информация")
    
    system_info = api_client.get_system_info()
    if not system_info:
        st.error("Не удалось получить информацию о системе")
        return
    
    # Информация о pipeline
    st.subheader("🔧 Pipeline")
    pipeline_info = system_info["pipeline_info"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.json({
            "Активных анализов": pipeline_info["active_analyses"],
            "Всего анализов": pipeline_info["total_analyses"]
        })
    
    with col2:
        st.json(pipeline_info["default_config"])
    
    # Доступные этапы
    st.subheader("🎯 Доступные этапы")
    stages_df = pd.DataFrame([
        {"ID": stage_id, "Название": stage_name}
        for stage_id, stage_name in pipeline_info["available_stages"].items()
    ])
    st.dataframe(stages_df, use_container_width=True)
    
    # Логи системы
    st.subheader("📝 Системные логи")
    if st.button("🔄 Обновить логи"):
        logs = api_client._request("GET", "/system/logs")
        if logs and "logs" in logs:
            st.text_area("Логи", "\n".join(logs["logs"][-50:]), height=400)
        else:
            st.warning("Логи не найдены")

if __name__ == "__main__":
    main()
