import streamlit as st
import duckdb, pandas as pd
import plotly.express as px

DB = st.sidebar.text_input("DuckDB path", "data/duckdb/mentions.duckdb")
con = duckdb.connect(DB)

st.title("Dialogs Analyzer — сводка")

@st.cache_data
def load_table(sql):
    return con.execute(sql).df()

# KPI
total_dialogs = con.execute("SELECT COUNT(DISTINCT dialog_id) FROM mentions").fetchone()[0]
mentions_cnt = con.execute("SELECT COUNT(*) FROM mentions").fetchone()[0]
st.metric("Всего диалогов", total_dialogs)
st.metric("Всего упоминаний", mentions_cnt)

# Темы
themes = load_table(open("app/sql/summary.sql", "r", encoding="utf-8").read())
st.subheader("Темы (частоты)")
st.dataframe(themes)
fig = px.bar(themes, x="theme", y="dialogov", hover_data=["upominanii","share_dialogs_pct"], title="Диалогов по темам")
st.plotly_chart(fig, use_container_width=True)

# Подтемы
subs = load_table(open("app/sql/subthemes.sql", "r", encoding="utf-8").read())
st.subheader("Топ‑подтем (по числу диалогов)")
st.dataframe(subs.head(50))
fig2 = px.bar(subs.head(20), x="subtheme", y="dialogov", color="theme", title="Топ‑20 подтем")
st.plotly_chart(fig2, use_container_width=True)

# Co‑occurrence heatmap
co = load_table(open("app/sql/cooccurrence.sql", "r", encoding="utf-8").read())
if not co.empty:
    pivot = co.pivot_table(index="theme_a", columns="theme_b", values="cnt", fill_value=0)
    fig3 = px.imshow(pivot, aspect="auto", title="Совстречаемость тем")
    st.plotly_chart(fig3, use_container_width=True)

# Индекс цитат (по фильтрам)
st.subheader("Индекс цитат")
sel_theme = st.selectbox("Тема", ["(все)"] + sorted(themes["theme"].unique().tolist()))
q = "SELECT dialog_id, turn_id, theme, subtheme, text_quote, confidence FROM mentions"
if sel_theme != "(все)":
    q += f" WHERE theme = '{sel_theme.replace("'","''")}'"
st.dataframe(con.execute(q + " ORDER BY dialog_id, turn_id").df(), use_container_width=True)
