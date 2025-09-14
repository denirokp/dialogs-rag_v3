#!/usr/bin/env python3
"""
Simple Dashboard для визуализации результатов анализа диалогов
"""

import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Настройка страницы
st.set_page_config(
    page_title="Dialogs Analysis Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Конфигурация API
API_BASE_URL = "http://localhost:8000"

def load_data():
    """Загрузка данных из API"""
    try:
        # Статистика
        stats = requests.get(f"{API_BASE_URL}/api/statistics").json()
        
        # Проблемы
        problems = requests.get(f"{API_BASE_URL}/api/problems").json()
        
        # Идеи
        ideas = requests.get(f"{API_BASE_URL}/api/ideas").json()
        
        # Сигналы
        signals = requests.get(f"{API_BASE_URL}/api/signals").json()
        
        return {
            "stats": stats,
            "problems": problems,
            "ideas": ideas,
            "signals": signals
        }
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return None

def main():
    """Главная функция дашборда"""
    
    # Заголовок
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0;">
        <h1 style="color: #2E86AB; margin-bottom: 0.5rem;">🔍 Dialogs Analysis Dashboard</h1>
        <p style="color: #666; font-size: 1.1rem;">Анализ диалогов клиентов с операторами</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Загружаем данные
    data = load_data()
    if not data:
        st.error("Не удалось загрузить данные")
        return
    
    # Боковая панель
    with st.sidebar:
        st.header("📊 Статистика")
        
        stats = data["stats"]
        st.metric("Всего диалогов", stats["total_dialogs"])
        st.metric("Успешность", f"{stats['success_rate']:.1%}")
        st.metric("Качество", f"{stats['quality_score']:.1%}")
        st.metric("Бизнес-релевантность", f"{stats['business_relevance']:.1%}")
        st.metric("Действенность", f"{stats['actionability']:.1%}")
    
    # Основной контент
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Обзор", "🚫 Проблемы", "💡 Идеи", "📡 Сигналы"])
    
    with tab1:
        show_overview(data)
    
    with tab2:
        show_problems(data)
    
    with tab3:
        show_ideas(data)
    
    with tab4:
        show_signals(data)

def show_overview(data):
    """Обзорная страница"""
    st.header("📊 Общий обзор")
    
    # Сводная статистика
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Проблемы", data["problems"]["total_problems"])
    
    with col2:
        st.metric("Идеи", data["ideas"]["total_ideas"])
    
    with col3:
        st.metric("Сигналы", data["signals"]["total_signals"])
    
    with col4:
        total_entities = data["problems"]["total_problems"] + data["ideas"]["total_ideas"] + data["signals"]["total_signals"]
        st.metric("Всего сущностей", total_entities)
    
    # График распределения сущностей
    entity_data = {
        "Тип": ["Проблемы", "Идеи", "Сигналы"],
        "Количество": [
            data["problems"]["total_problems"],
            data["ideas"]["total_ideas"],
            data["signals"]["total_signals"]
        ]
    }
    
    df_entities = pd.DataFrame(entity_data)
    
    fig = px.pie(
        df_entities, 
        values="Количество", 
        names="Тип",
        title="Распределение сущностей",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig, use_container_width=True)

def show_problems(data):
    """Страница с проблемами"""
    st.header("🚫 Проблемы клиентов")
    
    problems = data["problems"]["problems"]
    
    if not problems:
        st.info("Проблемы не найдены")
        return
    
    # Создаем DataFrame для проблем
    problems_data = []
    for problem in problems:
        problems_data.append({
            "Название": problem["name"],
            "Упоминаний": problem["mentions_abs"],
            "Процент диалогов": f"{problem['mentions_pct_of_D']:.1f}%",
            "Диалоги": len(problem["dialog_ids"])
        })
    
    df_problems = pd.DataFrame(problems_data)
    st.dataframe(df_problems, use_container_width=True)
    
    # График проблем
    fig = px.bar(
        df_problems, 
        x="Название", 
        y="Упоминаний",
        title="Количество упоминаний проблем",
        color="Упоминаний",
        color_continuous_scale="Reds"
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Детали по каждой проблеме
    st.subheader("📝 Детали проблем")
    
    for i, problem in enumerate(problems):
        with st.expander(f"Проблема {i+1}: {problem['name']}"):
            st.write(f"**Упоминаний:** {problem['mentions_abs']} ({problem['mentions_pct_of_D']:.1f}% диалогов)")
            st.write(f"**Диалоги:** {', '.join(problem['dialog_ids'])}")
            
            # Варианты проблемы
            st.write("**Варианты:**")
            for variant in problem["variants"]:
                st.write(f"- {variant['text']} ({variant['count_abs']} раз)")
            
            # Цитаты
            st.write("**Цитаты:**")
            for quote in problem["quotes"][:3]:  # Показываем первые 3 цитаты
                st.write(f"*\"{quote['quote'][:200]}{'...' if len(quote['quote']) > 200 else ''}\"*")
                st.write(f"*— Диалог {quote['dialog_id']}*")
                st.write("---")

def show_ideas(data):
    """Страница с идеями"""
    st.header("💡 Идеи клиентов")
    
    ideas = data["ideas"]["ideas"]
    
    if not ideas:
        st.info("Идеи не найдены")
        return
    
    # Создаем DataFrame для идей
    ideas_data = []
    for idea in ideas:
        ideas_data.append({
            "Название": idea["name"],
            "Упоминаний": idea["mentions_abs"],
            "Процент диалогов": f"{idea['mentions_pct_of_D']:.1f}%",
            "Диалоги": len(idea["dialog_ids"])
        })
    
    df_ideas = pd.DataFrame(ideas_data)
    st.dataframe(df_ideas, use_container_width=True)
    
    # График идей
    fig = px.bar(
        df_ideas, 
        x="Название", 
        y="Упоминаний",
        title="Количество упоминаний идей",
        color="Упоминаний",
        color_continuous_scale="Greens"
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Детали по каждой идее
    st.subheader("📝 Детали идей")
    
    for i, idea in enumerate(ideas):
        with st.expander(f"Идея {i+1}: {idea['name']}"):
            st.write(f"**Упоминаний:** {idea['mentions_abs']} ({idea['mentions_pct_of_D']:.1f}% диалогов)")
            st.write(f"**Диалоги:** {', '.join(idea['dialog_ids'])}")
            
            # Варианты идеи
            st.write("**Варианты:**")
            for variant in idea["variants"]:
                st.write(f"- {variant['text']} ({variant['count_abs']} раз)")
            
            # Цитаты
            st.write("**Цитаты:**")
            for quote in idea["quotes"][:3]:  # Показываем первые 3 цитаты
                st.write(f"*\"{quote['quote'][:200]}{'...' if len(quote['quote']) > 200 else ''}\"*")
                st.write(f"*— Диалог {quote['dialog_id']}*")
                st.write("---")

def show_signals(data):
    """Страница с сигналами"""
    st.header("📡 Сигналы клиентов")
    
    signals = data["signals"]["signals"]
    
    if not signals:
        st.info("Сигналы не найдены")
        return
    
    # Создаем DataFrame для сигналов
    signals_data = []
    for signal in signals:
        signals_data.append({
            "Название": signal["name"],
            "Упоминаний": signal["mentions_abs"],
            "Процент диалогов": f"{signal['mentions_pct_of_D']:.1f}%",
            "Диалоги": len(signal["dialog_ids"])
        })
    
    df_signals = pd.DataFrame(signals_data)
    st.dataframe(df_signals, use_container_width=True)
    
    # График сигналов
    fig = px.bar(
        df_signals, 
        x="Название", 
        y="Упоминаний",
        title="Количество упоминаний сигналов",
        color="Упоминаний",
        color_continuous_scale="Blues"
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Детали по каждому сигналу
    st.subheader("📝 Детали сигналов")
    
    for i, signal in enumerate(signals):
        with st.expander(f"Сигнал {i+1}: {signal['name']}"):
            st.write(f"**Упоминаний:** {signal['mentions_abs']} ({signal['mentions_pct_of_D']:.1f}% диалогов)")
            st.write(f"**Диалоги:** {', '.join(signal['dialog_ids'])}")
            
            # Варианты сигнала
            st.write("**Варианты:**")
            for variant in signal["variants"]:
                st.write(f"- {variant['text']} ({variant['count_abs']} раз)")
            
            # Цитаты
            st.write("**Цитаты:**")
            for quote in signal["quotes"][:3]:  # Показываем первые 3 цитаты
                st.write(f"*\"{quote['quote'][:200]}{'...' if len(quote['quote']) > 200 else ''}\"*")
                st.write(f"*— Диалог {quote['dialog_id']}*")
                st.write("---")

if __name__ == "__main__":
    main()
