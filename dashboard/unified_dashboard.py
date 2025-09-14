#!/usr/bin/env python3
"""
Unified Dashboard - Единый дашборд для всех режимов анализа диалогов
Объединяет legacy, pipeline, enhanced и scaled системы
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
from typing import Dict, Any, Optional

# Настройка страницы
st.set_page_config(
    page_title="Unified Dialogs RAG Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Конфигурация API
API_BASE_URL = "http://localhost:8000"

class UnifiedAPIClient:
    """Клиент для работы с Unified API"""
    
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
        return self._request("GET", "/api/health")
    
    def get_quality(self, mode: str = "auto"):
        """Получение метрик качества"""
        return self._request("GET", "/api/quality", params={"mode": mode})
    
    def get_summary_themes(self, mode: str = "auto"):
        """Получение сводки по темам"""
        return self._request("GET", "/api/summary_themes", params={"mode": mode})
    
    def get_summary_subthemes(self, theme: str = None, mode: str = "auto"):
        """Получение сводки по подтемам"""
        params = {"mode": mode}
        if theme:
            params["theme"] = theme
        return self._request("GET", "/api/summary_subthemes", params=params)
    
    def get_index_quotes(self, theme: str = None, subtheme: str = None, page: int = 1, page_size: int = 50, mode: str = "auto"):
        """Получение индекса цитат"""
        params = {"mode": mode, "page": page, "page_size": page_size}
        if theme:
            params["theme"] = theme
        if subtheme:
            params["subtheme"] = subtheme
        return self._request("GET", "/api/index_quotes", params=params)
    
    def get_cooccurrence(self, top: int = 50, mode: str = "auto"):
        """Получение совстречаемости тем"""
        return self._request("GET", "/api/cooccurrence", params={"mode": mode, "top": top})
    
    def get_system_info(self):
        """Получение информации о системе"""
        return self._request("GET", "/api/system_info")

# Инициализация API клиента
api_client = UnifiedAPIClient(API_BASE_URL)

def main():
    """Главная функция дашборда"""
    
    # Заголовок
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0;">
        <h1 style="color: #2E86AB; margin-bottom: 0.5rem;">🔍 Unified Dialogs RAG Dashboard</h1>
        <p style="color: #666; font-size: 1.1rem;">Единый интерфейс для всех режимов анализа диалогов</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Боковая панель
    with st.sidebar:
        st.header("🔧 Настройки")
        
        # Проверка подключения к API
        if st.button("🔄 Проверить подключение", use_container_width=True):
            health = api_client.get_health()
            if health:
                st.success(f"✅ API подключен (режим: {health.get('mode', 'unknown')})")
                st.metric("Упоминаний", health.get('mentions_count', 0))
            else:
                st.error("❌ API недоступен")
        
        # Выбор режима работы
        st.subheader("🎯 Режим работы")
        mode = st.selectbox(
            "Режим данных",
            options=["auto", "legacy", "pipeline", "enhanced", "scaled"],
            index=0,
            help="Выберите режим анализа диалогов"
        )
        
        # Информация о режиме
        if mode != "auto":
            system_info = api_client.get_system_info()
            if system_info and system_info.get("current_mode") == mode:
                st.success(f"✅ Режим {mode} активен")
            else:
                st.warning(f"⚠️ Режим {mode} не активен")
        
        # Настройки фильтрации
        st.subheader("🔍 Фильтры")
        filter_theme = st.selectbox(
            "Тема",
            options=["(все)"] + ["доставка", "продвижение", "цены", "поддержка", "UI/настройки", "логистика/сроки", "продукт", "оплата/возвраты", "ассортимент", "сравнение/конкуренты", "прочее"],
            index=0
        )
        
        # Настройки отображения
        st.subheader("📊 Отображение")
        page_size = st.slider("Размер страницы", 10, 100, 50)
        show_charts = st.checkbox("Показывать графики", value=True)
        show_details = st.checkbox("Показывать детали", value=True)
    
    # Основной контент
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Обзор", "🏷️ Темы", "📝 Цитаты", "🔗 Совстречаемость", "⚙️ Система"])
    
    with tab1:
        show_overview(mode, show_charts)
    
    with tab2:
        show_themes(mode, filter_theme, show_charts, show_details)
    
    with tab3:
        show_quotes(mode, filter_theme, page_size, show_details)
    
    with tab4:
        show_cooccurrence(mode, show_charts)
    
    with tab5:
        show_system_info()

def show_overview(mode: str, show_charts: bool):
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
            "Всего диалогов",
            system_info["statistics"]["total_dialogs"],
            delta=None
        )
    
    with col2:
        st.metric(
            "Всего упоминаний",
            system_info["statistics"]["total_mentions"],
            delta=None
        )
    
    with col3:
        st.metric(
            "Текущий режим",
            system_info["current_mode"],
            delta=None
        )
    
    with col4:
        features = system_info["features"]
        active_features = sum(features.values())
        st.metric(
            "Активных функций",
            active_features,
            delta=None
        )
    
    # Проверка качества
    st.subheader("🔍 Проверка качества")
    quality = api_client.get_quality(mode)
    if quality:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status = "✅" if quality.get("evidence_100", False) else "❌"
            st.metric("Evidence-100", status)
        
        with col2:
            dedup_rate = quality.get("dedup_rate", 0.0)
            status = "✅" if dedup_rate <= 0.01 else "❌"
            st.metric("Dedup ≤1%", f"{dedup_rate:.3%}", status)
        
        with col3:
            coverage = quality.get("coverage_other_pct", 0.0)
            status = "✅" if coverage <= 2.0 else "❌"
            st.metric("Coverage ≥98%", f"{100-coverage:.1f}%", status)
        
        with col4:
            passed = quality.get("passed", False)
            status = "✅" if passed else "❌"
            st.metric("DoD Passed", status)
    
    # График активности (если доступен)
    if show_charts:
        st.subheader("📈 Активность по темам")
        themes_data = api_client.get_summary_themes(mode)
        if themes_data and "themes" in themes_data:
            themes_df = pd.DataFrame(themes_data["themes"])
            if not themes_df.empty:
                fig = px.bar(
                    themes_df.head(10), 
                    x="theme", 
                    y="dialogov",
                    title="Топ-10 тем по количеству диалогов",
                    hover_data=["upominanii", "share_dialogs_pct"]
                )
                st.plotly_chart(fig, use_container_width=True)

def show_themes(mode: str, filter_theme: str, show_charts: bool, show_details: bool):
    """Страница с темами и подтемами"""
    st.header("🏷️ Темы и подтемы")
    
    # Сводка по темам
    st.subheader("📊 Сводка по темам")
    themes_data = api_client.get_summary_themes(mode)
    if themes_data and "themes" in themes_data:
        themes_df = pd.DataFrame(themes_data["themes"])
        if not themes_df.empty:
            st.dataframe(themes_df, use_container_width=True)
            
            if show_charts:
                col1, col2 = st.columns(2)
                
                with col1:
                    fig1 = px.bar(
                        themes_df.head(10), 
                        x="theme", 
                        y="dialogov",
                        title="Диалогов по темам",
                        hover_data=["upominanii", "share_dialogs_pct"]
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    fig2 = px.pie(
                        themes_df.head(8), 
                        values="dialogov", 
                        names="theme",
                        title="Распределение тем"
                    )
                    st.plotly_chart(fig2, use_container_width=True)
    
    # Подтемы
    st.subheader("🔍 Подтемы")
    subthemes_data = api_client.get_summary_subthemes(
        theme=filter_theme if filter_theme != "(все)" else None,
        mode=mode
    )
    if subthemes_data and "items" in subthemes_data:
        subthemes_df = pd.DataFrame(subthemes_data["items"])
        if not subthemes_df.empty:
            st.dataframe(subthemes_df.head(50), use_container_width=True)
            
            if show_charts and show_details:
                fig = px.bar(
                    subthemes_df.head(20), 
                    x="subtheme", 
                    y="dialogov",
                    color="theme",
                    title="Топ-20 подтем",
                    hover_data=["upominanii", "share_dialogs_pct"]
                )
                st.plotly_chart(fig, use_container_width=True)

def show_quotes(mode: str, filter_theme: str, page_size: int, show_details: bool):
    """Страница с цитатами"""
    st.header("📝 Индекс цитат")
    
    # Фильтры
    col1, col2, col3 = st.columns(3)
    
    with col1:
        theme_filter = filter_theme if filter_theme != "(все)" else None
    
    with col2:
        subtheme_filter = st.selectbox(
            "Подтема",
            options=["(все)"] + ["не работает выборочно", "не удаётся настроить", "не окупается", "высокая стоимость", "непонятный интерфейс"],
            index=0
        )
        subtheme_filter = subtheme_filter if subtheme_filter != "(все)" else None
    
    with col3:
        page = st.number_input("Страница", min_value=1, value=1)
    
    # Получение цитат
    quotes_data = api_client.get_index_quotes(
        theme=theme_filter,
        subtheme=subtheme_filter,
        page=page,
        page_size=page_size,
        mode=mode
    )
    
    if quotes_data and "items" in quotes_data:
        quotes_df = pd.DataFrame(quotes_data["items"])
        if not quotes_df.empty:
            st.dataframe(quotes_df, use_container_width=True)
            
            # Пагинация
            if quotes_data.get("next_page"):
                st.info(f"Страница {page} из {page + 1 if quotes_data.get('next_page') else page}")
            else:
                st.info(f"Страница {page} (последняя)")
            
            if show_details:
                # Статистика по цитатам
                st.subheader("📊 Статистика цитат")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Всего цитат", len(quotes_df))
                
                with col2:
                    avg_confidence = quotes_df["confidence"].mean() if "confidence" in quotes_df.columns else 0
                    st.metric("Средняя уверенность", f"{avg_confidence:.2f}")
                
                with col3:
                    unique_themes = quotes_df["theme"].nunique()
                    st.metric("Уникальных тем", unique_themes)
        else:
            st.info("Цитаты не найдены")
    else:
        st.error("Не удалось загрузить цитаты")

def show_cooccurrence(mode: str, show_charts: bool):
    """Страница с совстречаемостью тем"""
    st.header("🔗 Совстречаемость тем")
    
    # Настройки
    col1, col2 = st.columns(2)
    
    with col1:
        top_n = st.slider("Количество топ пар", 10, 100, 50)
    
    with col2:
        show_heatmap = st.checkbox("Показывать тепловую карту", value=True)
    
    # Получение данных
    cooccurrence_data = api_client.get_cooccurrence(top=top_n, mode=mode)
    if cooccurrence_data and "items" in cooccurrence_data:
        co_df = pd.DataFrame(cooccurrence_data["items"])
        if not co_df.empty:
            st.dataframe(co_df, use_container_width=True)
            
            if show_charts and show_heatmap:
                # Создаем матрицу совстречаемости
                pivot_df = co_df.pivot_table(
                    index="theme_a", 
                    columns="theme_b", 
                    values="cnt", 
                    fill_value=0
                )
                
                if not pivot_df.empty:
                    fig = px.imshow(
                        pivot_df, 
                        aspect="auto", 
                        title="Тепловая карта совстречаемости тем",
                        color_continuous_scale="Blues"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            if show_charts:
                # График топ пар
                fig = px.bar(
                    co_df.head(20), 
                    x="cnt", 
                    y="theme_a",
                    color="theme_b",
                    title="Топ-20 пар совстречающихся тем",
                    orientation="h"
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Данные о совстречаемости не найдены")
    else:
        st.error("Не удалось загрузить данные о совстречаемости")

def show_system_info():
    """Страница с информацией о системе"""
    st.header("⚙️ Информация о системе")
    
    system_info = api_client.get_system_info()
    if not system_info:
        st.error("Не удалось получить информацию о системе")
        return
    
    # Общая информация
    st.subheader("📋 Общая информация")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Текущий режим", system_info["current_mode"])
        st.metric("Всего диалогов", system_info["statistics"]["total_dialogs"])
        st.metric("Всего упоминаний", system_info["statistics"]["total_mentions"])
    
    with col2:
        st.metric("Доступных режимов", len(system_info["available_modes"]))
        features = system_info["features"]
        active_features = sum(features.values())
        st.metric("Активных функций", active_features)
    
    # Доступные режимы
    st.subheader("🎯 Доступные режимы")
    modes_df = pd.DataFrame([
        {"Режим": mode, "Статус": "✅ Активен" if mode == system_info["current_mode"] else "⏸️ Неактивен"}
        for mode in system_info["available_modes"]
    ])
    st.dataframe(modes_df, use_container_width=True)
    
    # Функции системы
    st.subheader("🔧 Функции системы")
    features_df = pd.DataFrame([
        {"Функция": feature, "Статус": "✅ Включена" if status else "❌ Отключена"}
        for feature, status in system_info["features"].items()
    ])
    st.dataframe(features_df, use_container_width=True)
    
    # Проверка качества
    st.subheader("🔍 Проверка качества")
    quality = api_client.get_quality()
    if quality:
        quality_df = pd.DataFrame([
            {"Метрика": "Evidence-100", "Значение": "✅" if quality.get("evidence_100", False) else "❌"},
            {"Метрика": "Dedup ≤1%", "Значение": f"{quality.get('dedup_rate', 0.0):.3%}"},
            {"Метрика": "Coverage ≥98%", "Значение": f"{100-quality.get('coverage_other_pct', 0.0):.1f}%"},
            {"Метрика": "DoD Passed", "Значение": "✅" if quality.get("passed", False) else "❌"}
        ])
        st.dataframe(quality_df, use_container_width=True)

if __name__ == "__main__":
    main()
