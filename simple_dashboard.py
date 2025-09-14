# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="DialogsRAG Dashboard", layout="wide")

ART = Path("artifacts")
RES_PATH = ART / "comprehensive_results.json"
STATS_PATH = ART / "statistics.json"

# ---------- загрузка ----------
@st.cache_data(show_spinner=False)
def load_mentions() -> pd.DataFrame:
    if not RES_PATH.exists():
        return pd.DataFrame(columns=["dialog_id","turn_id","label_type","theme","subtheme","text_quote","confidence"])
    js = json.loads(RES_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(js.get("mentions", []))
    if df.empty:
        return df
    for c in ["theme","subtheme","label_type","text_quote"]:
        if c in df.columns:
            df[c] = df[c].fillna("")
    if "confidence" in df.columns:
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    return df

@st.cache_data(show_spinner=False)
def load_stats() -> dict:
    if not STATS_PATH.exists():
        return {}
    return json.loads(STATS_PATH.read_text(encoding="utf-8"))

# ---------- UI: sidebar ----------
st.sidebar.header("Фильтры")
df = load_mentions()
stats = load_stats()

label_opts = sorted(df["label_type"].unique()) if not df.empty else ["problems","ideas","signals"]
label_sel = st.sidebar.multiselect("Типы сущностей", options=label_opts, default=label_opts)
conf_min, conf_max = st.sidebar.slider("Диапазон confidence", 0.0, 1.0, (0.0, 1.0), 0.05)
search = st.sidebar.text_input("Поиск в цитатах")

# Динамический список тем/подтем
if not df.empty:
    df_f = df[df["label_type"].isin(label_sel) & df["confidence"].between(conf_min, conf_max)]
    theme_opts = sorted(df_f["theme"].unique())
    theme_sel = st.sidebar.multiselect("Темы", options=theme_opts, default=theme_opts)
    sub_opts = sorted(df_f[df_f["theme"].isin(theme_sel)]["subtheme"].unique())
    sub_sel = st.sidebar.multiselect("Подтемы", options=sub_opts, default=sub_opts)
else:
    df_f = df.copy(); theme_sel = []; sub_sel = []

# применим фильтры
if not df_f.empty:
    mask = df_f["theme"].isin(theme_sel) & df_f["subtheme"].isin(sub_sel)
    if search.strip():
        mask &= df_f["text_quote"].str.contains(search, case=False, regex=True)
    df_f = df_f[mask]

# ---------- Заголовок / KPI ----------
st.title("DialogsRAG Dashboard — v2")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Диалогов", value=stats.get("dialogs", 0))
col2.metric("Упоминаний", value=stats.get("mentions", 0))
col3.metric("Problems", value=stats.get("problems", 0))
col4.metric("Ideas", value=stats.get("ideas", 0))
col5.metric("Signals", value=stats.get("signals", 0))

st.caption(
    f"Evidence-100: {'✅' if stats.get('evidence_100') else '❌'} · "
    f"Дедуп снято: {stats.get('dedup_removed_pct', 0)}% · "
    f"Ambiguity (<0.6): {stats.get('ambiguity_pct', 0)}%"
)

# ---------- Tabs ----------

tab_overview, tab_problems, tab_ideas, tab_signals, tab_problems_cons = st.tabs(
    ["Обзор", "Проблемы (raw)", "Идеи (raw)", "Сигналы (raw)", "🚫 Проблемы (консолидация)"]
)

# ===== Обзор =====
with tab_overview:
    st.subheader("Топ тем по типам сущностей")
    if df_f.empty:
        st.info("Нет данных для отображения.")
    else:
        agg = (
            df_f.groupby(["label_type", "theme"], as_index=False)["dialog_id"].nunique()
        ).rename(columns={"dialog_id": "dialogs"})
        colA, colB = st.columns(2)
        with colA:
            st.plotly_chart(
                px.bar(
                    agg.sort_values(["label_type", "dialogs"], ascending=[True, False]),
                    x="theme", y="dialogs", color="label_type",
                    title="Диалоги по темам",
                ), use_container_width=True
            )
        with colB:
            agg2 = (
                df_f.groupby(["label_type", "theme", "subtheme"], as_index=False)["text_quote"].count()
            ).rename(columns={"text_quote": "mentions"})
            st.plotly_chart(
                px.treemap(
                    agg2, path=["label_type", "theme", "subtheme"], values="mentions",
                    title="Treemap: mentions по типу/теме/подтеме",
                ), use_container_width=True
            )

    st.markdown("---")
    st.subheader("Сырые упоминания (фильтрованные)")
    st.dataframe(
        df_f.sort_values(["label_type", "theme", "subtheme"]).reset_index(drop=True),
        use_container_width=True, height=420
    )

# ===== RAW проблем/идей/сигналов =====
def render_raw(df_src: pd.DataFrame, label: str):
    d = df_src[df_src["label_type"] == label]
    if d.empty:
        st.info("Нет строк.")
        return
    c1, c2 = st.columns([1,2])
    with c1:
        top_t = d.groupby("theme")["dialog_id"].nunique().sort_values(ascending=False).head(15)
        st.plotly_chart(px.bar(top_t, title=f"Топ тем — {label}"), use_container_width=True)
    with c2:
        top_st = d.groupby(["theme","subtheme"][1:])["text_quote"].count().sort_values(ascending=False).head(20)
        # небольшой хак для подписи
        top_st = top_st.rename("mentions").reset_index()
        st.plotly_chart(px.bar(top_st, x="subtheme", y="mentions", title=f"Топ подтем — {label}"), use_container_width=True)
    st.markdown("---")
    st.dataframe(d.sort_values(["theme","subtheme","confidence"], ascending=[True,True,False]), use_container_width=True, height=480)

with tab_problems:
    render_raw(df_f, "problems")
with tab_ideas:
    render_raw(df_f, "ideas")
with tab_signals:
    render_raw(df_f, "signals")

# ===== Консолидация проблем =====
with tab_problems_cons:
    st.header("🚫 Проблемы (консолидация + саммори)")
    ps_path = ART / "problems_summary.csv"
    sub_path = ART / "problems_subthemes.csv"
    idx_path = ART / "problems_mentions.csv"
    cards_jsonl = ART / "problem_cards.jsonl"

    if not ps_path.exists():
        st.warning("Нет artifacts/problems_summary.csv — прогоните consolidate_and_summarize.py")
    else:
        ps = pd.read_csv(ps_path)
        st.dataframe(ps, use_container_width=True)
        st.plotly_chart(
            px.bar(
                ps.sort_values("dialogs", ascending=False).head(15),
                x="problem_title", y="mentions",
                hover_data=["dialogs","share_dialogs_pct","freq_per_1k","intensity_mpd"],
                title="Количество упоминаний проблем",
            ),
            use_container_width=True,
        )

        st.markdown("---")
        st.subheader("Карточки проблем")
        cards = pd.read_json(cards_jsonl, lines=True) if cards_jsonl.exists() else pd.DataFrame()
        sub = pd.read_csv(sub_path) if sub_path.exists() else pd.DataFrame()
        idx = pd.read_csv(idx_path) if idx_path.exists() else pd.DataFrame()

        for _, row in ps.sort_values("dialogs", ascending=False).iterrows():
            pid, title = row["problem_id"], row["problem_title"]
            with st.expander(f"{title} — {int(row['mentions'])} упом. / {int(row['dialogs'])} диалогов ({row['share_dialogs_pct']}%)"):
                if not cards.empty and pid in set(cards["problem_id"]):
                    js = cards[cards["problem_id"] == pid].iloc[0]
                    st.markdown(f"**Определение.** {js.get('definition','')}")
                    st.markdown(f"**Почему важно.** {js.get('why_it_matters','')}")
                    motifs = js.get("common_motifs", [])
                    if isinstance(motifs, str):
                        try:
                            motifs = json.loads(motifs)
                        except:
                            motifs = [motifs]
                    if motifs:
                        st.markdown("**Частые мотивы:** " + ", ".join(motifs))
                if not sub.empty:
                    st.markdown("**Подтемы (топ):**")
                    st.dataframe(sub[sub["problem_id"] == pid].head(10), use_container_width=True)
                if not idx.empty:
                    st.markdown("**Цитаты (фрагмент):**")
                    cols = ["dialog_id","turn_id","theme","subtheme","text_quote","confidence"]
                    st.dataframe(idx[idx["problem_id"] == pid][cols].head(50), use_container_width=True)