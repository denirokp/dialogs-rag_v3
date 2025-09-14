# -*- coding: utf-8 -*-
import io
import json
from pathlib import Path
from urllib.parse import urlencode, quote

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

st.set_page_config(page_title="DialogsRAG Dashboard", layout="wide")

ART = Path("artifacts")
RES_PATH = ART / "comprehensive_results.json"
STATS_PATH = ART / "statistics.json"
PM_SUM = ART / "problems_summary.csv"
PM_SUB = ART / "problems_subthemes.csv"
PM_IDX = ART / "problems_mentions.csv"
PM_CARDS = ART / "problem_cards.jsonl"

PALETTE = {"problems": "#e74c3c", "ideas": "#f1c40f", "signals": "#3498db"}

# ---------- helpers ----------

def _get_query_params():
    # Streamlit 1.32+: st.query_params, fallback for older
    try:
        return dict(st.query_params)
    except Exception:
        return {k: v[0] if isinstance(v, list) and v else v for k, v in st.experimental_get_query_params().items()}


def _set_query_params(params: dict):
    try:
        st.query_params.clear()
        st.query_params.update(params)
    except Exception:
        st.experimental_set_query_params(**params)


@st.cache_data(show_spinner=False)
def load_mentions() -> pd.DataFrame:
    if not RES_PATH.exists():
        return pd.DataFrame(columns=["dialog_id","turn_id","label_type","theme","subtheme","text_quote","confidence"])
    js = json.loads(RES_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(js.get("mentions", []))
    if df.empty:
        return df
    df["theme"] = df["theme"].fillna("")
    df["subtheme"] = df["subtheme"].fillna("")
    df["label_type"] = df["label_type"].fillna("")
    df["text_quote"] = df["text_quote"].fillna("")
    df["confidence"] = pd.to_numeric(df.get("confidence", 0.0), errors="coerce").fillna(0.0)
    df["dialog_id"] = df["dialog_id"].astype(str)
    return df


@st.cache_data(show_spinner=False)
def load_stats() -> dict:
    if not STATS_PATH.exists():
        return {}
    return json.loads(STATS_PATH.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def load_problems_artifacts():
    ps = pd.read_csv(PM_SUM) if PM_SUM.exists() else pd.DataFrame()
    sub = pd.read_csv(PM_SUB) if PM_SUB.exists() else pd.DataFrame()
    idx = pd.read_csv(PM_IDX) if PM_IDX.exists() else pd.DataFrame()
    cards = pd.read_json(PM_CARDS, lines=True) if PM_CARDS.exists() else pd.DataFrame()
    return ps, sub, idx, cards


def file_hash() -> str:
    # Простая сигнатура артефактов
    parts = []
    for p in [RES_PATH, STATS_PATH, PM_SUM, PM_SUB, PM_IDX, PM_CARDS]:
        if p.exists():
            st_mtime = int(p.stat().st_mtime)
            size = p.stat().st_size
            parts.append(f"{p.name}:{st_mtime}:{size}")
    return str(hash("|".join(parts)))


def annotate_bars(fig, x_col: str, y_col: str, fmt: str = "+.0f"):
    try:
        fig.update_traces(texttemplate="%{y}", textposition="outside", cliponaxis=False)
    except Exception:
        pass
    fig.update_layout(uniformtext_minsize=10, uniformtext_mode="hide")
    return fig


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO(); df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def to_excel_bytes(df_dict: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        for name, d in df_dict.items():
            d.to_excel(w, sheet_name=name[:31], index=False)
    buf.seek(0)
    return buf.getvalue()


def highlight_html(text: str, q: str) -> str:
    if not q: return text
    try:
        import re
        pattern = re.compile(re.escape(q), re.IGNORECASE)
        return pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", text)
    except Exception:
        return text


# ---------- data ----------

qparams = _get_query_params()
df = load_mentions()
stats = load_stats()
ps, sub_idx, idx_idx, cards = load_problems_artifacts()

# автоперезагрузка при изменении артефактов
sig = file_hash()
if "_sig" not in st.session_state:
    st.session_state["_sig"] = sig
elif st.session_state["_sig"] != sig:
    st.info("Найдены новые артефакты. Нажмите для обновления.")
    if st.button("🔄 Обновить дашборд"):
        st.session_state["_sig"] = sig
        st.cache_data.clear()
        st.rerun()

# ---------- sidebar ----------
st.sidebar.header("Фильтры")
metric_mode = st.sidebar.radio("Метрика", ["dialogs", "mentions"], index=0, help="Переключает расчёт графиков")

label_opts = sorted(df["label_type"].unique()) if not df.empty else ["problems","ideas","signals"]
label_default = qparams.get("labels", ",".join(label_opts)).split(",") if df.shape[0] else label_opts
label_sel = st.sidebar.multiselect("Типы сущностей", options=label_opts, default=[x for x in label_default if x in label_opts])

conf_min, conf_max = st.sidebar.slider("Диапазон confidence", 0.0, 1.0, (
    float(qparams.get("cmin", 0.0)), float(qparams.get("cmax", 1.0))
), 0.05)

search = st.sidebar.text_input("Поиск в цитатах", value=qparams.get("q", ""))

if not df.empty:
    df_seed = df[df["label_type"].isin(label_sel) & df["confidence"].between(conf_min, conf_max)]
    theme_opts = sorted(df_seed["theme"].unique())
    theme_default = [t for t in qparams.get("themes", ",".join(theme_opts)).split(",") if t in theme_opts]
    theme_sel = st.sidebar.multiselect("Темы", options=theme_opts, default=(theme_default or theme_opts))
    sub_opts = sorted(df_seed[df_seed["theme"].isin(theme_sel)]["subtheme"].unique())
    sub_default = [t for t in qparams.get("subs", ",".join(sub_opts)).split(",") if t in sub_opts]
    sub_sel = st.sidebar.multiselect("Подтемы", options=sub_opts, default=(sub_default or sub_opts))
else:
    df_seed = df.copy(); theme_sel = []; sub_sel = []

# применим фильтры
if not df_seed.empty:
    mask = df_seed["theme"].isin(theme_sel) & df_seed["subtheme"].isin(sub_sel)
    if search.strip():
        mask &= df_seed["text_quote"].str.contains(search, case=False, regex=True)
    df_f = df_seed[mask]
else:
    df_f = df_seed.copy()

# Ссылка-пересет
current_params = {
    "labels": ",".join(label_sel),
    "cmin": f"{conf_min}",
    "cmax": f"{conf_max}",
    "q": search,
    "themes": ",".join(theme_sel),
    "subs": ",".join(sub_sel),
}
try:
    base_url = st.secrets.get("BASE_URL", "") or ""
except Exception:
    base_url = ""
share_url = (base_url or st.request.url if hasattr(st, "request") else "")
try:
    # на Streamlit Cloud ст.request недоступен — сделаем относительную ссылку
    share_url = "?" + urlencode(current_params)
except Exception:
    share_url = "?" + urlencode(current_params)

with st.sidebar.expander("Пресеты/шаринг"):
    st.code(share_url, language="text")
    st.caption("Скопируй ссылку — она восстановит те же фильтры")
    if st.button("Применить текущие фильтры в URL"):
        _set_query_params(current_params)

# ---------- HEADER / KPI ----------
st.title("DialogsRAG Dashboard — v2.1")
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

# ---------- tabs ----------
(
    tab_overview,
    tab_problems_raw,
    tab_ideas_raw,
    tab_signals_raw,
    tab_problems_cons,
    tab_links,
    tab_quality,
) = st.tabs([
    "Обзор",
    "Проблемы (raw)",
    "Идеи (raw)",
    "Сигналы (raw)",
    "🚫 Проблемы (консолидация)",
    "Связи тем",
    "Качество",
])

# ===== Обзор =====
with tab_overview:
    if df_f.empty:
        st.info("Нет данных для отображения.")
    else:
        st.subheader("Top‑N тем по типам")
        # Faceted bars: три маленьких чарта
        topn = 8
        left, right = st.columns([1,1])
        
        def _agg(metric: str):
            if metric == "dialogs":
                return (
                    df_f.groupby(["label_type", "theme"], as_index=False)["dialog_id"].nunique()
                    .rename(columns={"dialog_id": "value"})
                )
            else:
                return (
                    df_f.groupby(["label_type", "theme"], as_index=False)["text_quote"].count()
                    .rename(columns={"text_quote": "value"})
                )
        agg = _agg(metric_mode)
        # разбить по типам
        for lbl in ["problems","ideas","signals"]:
            d = agg[agg["label_type"]==lbl].sort_values("value", ascending=False).head(topn)
            if d.empty: continue
            fig = px.bar(
                d.sort_values("value"),
                x="value", y="theme", orientation="h",
                title=f"{lbl}: Топ тем ({metric_mode})",
                color_discrete_sequence=[PALETTE.get(lbl, "#777")],
                text="value",
            )
            fig.update_layout(yaxis_title="", xaxis_title=metric_mode)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Структура выбранной темы (100% stacked)")
        sel_lbl = st.selectbox("Тип", ["problems","ideas","signals"], index=0)
        themes_for_lbl = sorted(df_f[df_f["label_type"]==sel_lbl]["theme"].unique())
        if themes_for_lbl:
            sel_theme = st.selectbox("Тема", themes_for_lbl, index=0)
            d = df_f[(df_f["label_type"]==sel_lbl) & (df_f["theme"]==sel_theme)]
            by_sub = d.groupby("subtheme")["text_quote"].count().reset_index(name="mentions")
            by_sub["share"] = (100*by_sub["mentions"]/max(1,by_sub["mentions"].sum())).round(1)
            fig = px.bar(by_sub.sort_values("share"), x="share", y="subtheme", orientation="h",
                         text="share", color_discrete_sequence=[PALETTE.get(sel_lbl,"#777")])
            fig.update_layout(xaxis_title="Доля, %", yaxis_title="Подтема")
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Treemap: состав mentions по типу/теме/подтеме")
        agg2 = (
            df_f.groupby(["label_type", "theme", "subtheme"], as_index=False)["text_quote"].count()
        ).rename(columns={"text_quote": "mentions"})
        fig = px.treemap(
            agg2, path=["label_type", "theme", "subtheme"], values="mentions",
            color="label_type", color_discrete_map=PALETTE,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Сырые упоминания (фильтрованные)")
        # подсветка поиска (опционально)
        hl = st.toggle("Подсветить фразу поиска в цитатах", value=bool(search.strip()))
        table = df_f.sort_values(["label_type","theme","subtheme"]).reset_index(drop=True)
        if hl and search.strip():
            t = table.copy()
            t["text_quote"] = t["text_quote"].apply(lambda x: highlight_html(x, search))
            st.markdown(t.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.dataframe(table, use_container_width=True, height=440)
        
        # Экспорт
        colx, coly = st.columns(2)
        with colx:
            st.download_button("⬇️ Экспорт CSV (фильтр)", data=to_csv_bytes(table), file_name="mentions_filtered.csv", mime="text/csv")
        with coly:
            st.download_button(
                "⬇️ Экспорт Excel (лист Mentions)",
                data=to_excel_bytes({"Mentions": table}),
                file_name="mentions_filtered.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# ===== RAW =====

def render_raw(df_src: pd.DataFrame, label: str):
    d = df_src[df_src["label_type"] == label]
    if d.empty:
        st.info("Нет строк.")
        return
    c1, c2 = st.columns([1,1])
    # Диалоги vs Упоминания по темам
    top_dialogs = d.groupby("theme")["dialog_id"].nunique().rename("dialogs")
    top_mentions = d.groupby("theme")["text_quote"].count().rename("mentions")
    top = pd.concat([top_dialogs, top_mentions], axis=1).reset_index().fillna(0)
    top = top.sort_values("dialogs", ascending=False).head(15)
    fig = px.bar(top.melt(id_vars="theme", value_vars=["dialogs","mentions"]),
                 x="value", y="theme", color="variable", barmode="group",
                 orientation="h", title=f"{label}: Диалоги vs Упоминания (топ тем)")
    st.plotly_chart(fig, use_container_width=True)
    
    # Box по confidence для топ‑подтем
    sub_counts = d.groupby("subtheme").size().sort_values(ascending=False).head(12)
    top_sub = d[d["subtheme"].isin(sub_counts.index)]
    if not top_sub.empty:
        fig2 = px.box(top_sub, x="subtheme", y="confidence", points="outliers",
                      title="Распределение confidence по топ‑подтемам")
        fig2.update_layout(xaxis_tickangle=30)
        st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("---")
    st.dataframe(d.sort_values(["theme","subtheme","confidence"], ascending=[True,True,False]),
                 use_container_width=True, height=480)

with tab_problems_raw:
    render_raw(df_f, "problems")
with tab_ideas_raw:
    render_raw(df_f, "ideas")
with tab_signals_raw:
    render_raw(df_f, "signals")

# ===== Консолидация проблем =====
with tab_problems_cons:
    st.header("🚫 Проблемы (консолидация)")
    if not PM_SUM.exists():
        st.warning("Нет artifacts/problems_summary.csv — прогоните consolidate_and_summarize.py")
    else:
        ps, sub, idx, cards = load_problems_artifacts()
        st.subheader("Сводные метрики")
        st.dataframe(ps, use_container_width=True)
        # Pareto: dialogs + cum share
        d = ps.sort_values("dialogs", ascending=False).copy()
        d["cum_share"] = (d["dialogs"].cumsum() / max(1, d["dialogs"].sum()) * 100).round(1)
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=d["problem_title"], y=d["dialogs"], name="dialogs"), secondary_y=False)
        fig.add_trace(go.Scatter(x=d["problem_title"], y=d["cum_share"], name="cum %", mode="lines+markers"), secondary_y=True)
        fig.update_layout(title_text="Pareto: диалоги по проблемам", xaxis_tickangle=30)
        fig.update_yaxes(title_text="Диалоги", secondary_y=False)
        fig.update_yaxes(title_text="Кумулятивная доля, %", secondary_y=True, range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Sankey: подтемы → проблема")
        if not idx.empty:
            g = idx.groupby(["theme","subtheme","problem_id","problem_title"])['dialog_id'].nunique().reset_index(name="dialogs")
            # узлы
            subs = g.apply(lambda r: f"{r['theme']} / {r['subtheme']}", axis=1).unique().tolist()
            probs = g["problem_title"].unique().tolist()
            nodes = subs + probs
            idx_map = {name: i for i, name in enumerate(nodes)}
            src = [idx_map[f"{r.theme} / {r.subtheme}"] for r in g.itertuples()]
            dst = [idx_map[r.problem_title] for r in g.itertuples()]
            val = [int(r.dialogs) for r in g.itertuples()]
            sankey = go.Sankey(
                node=dict(label=nodes),
                link=dict(source=src, target=dst, value=val)
            )
            st.plotly_chart(go.Figure(sankey), use_container_width=True)
        else:
            st.info("Нет индекса цитат для Sankey.")

        st.markdown("---")
        st.subheader("Heatmap покрытия карты (темы × проблема)")
        if not idx.empty:
            cov = idx.groupby(["theme","problem_title"])['dialog_id'].nunique().reset_index(name="dialogs")
            pivot = cov.pivot(index="theme", columns="problem_title", values="dialogs").fillna(0)
            fig = px.imshow(pivot, aspect="auto", color_continuous_scale="Blues", origin="lower")
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        # «Прочее/не сконсолидировано» — детектор доли
        if not ps.empty and (ps["problem_id"]=="other_unmapped").any():
            unm = ps[ps["problem_id"]=="other_unmapped"].iloc[0]
            st.warning(f"Прочее/не сконсолидировано: {unm['share_dialogs_pct']}% диалогов · {int(unm['mentions'])} упомин.")
            if not idx.empty:
                cand = (idx[idx["problem_id"]=="other_unmapped"]
                        .groupby(["theme","subtheme"])['dialog_id'].nunique().reset_index(name="dialogs")
                        .sort_values("dialogs", ascending=False).head(15))
                st.write("Кандидаты для карты (тема/подтема по диалогам):")
                st.dataframe(cand, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Карточки проблем")
        for _, row in ps.sort_values("dialogs", ascending=False).iterrows():
            pid, title = row["problem_id"], row["problem_title"]
            with st.expander(f"{title} — {int(row['mentions'])} упом. / {int(row['dialogs'])} диалогов ({row['share_dialogs_pct']}%)"):
                if not cards.empty and pid in set(cards.get("problem_id", pd.Series()).values):
                    js = cards[cards["problem_id"] == pid].iloc[0]
                    st.markdown(f"**Определение.** {js.get('definition','')}")
                    st.markdown(f"**Почему важно.** {js.get('why_it_matters','')}")
                    motifs = js.get("common_motifs", [])
                    if isinstance(motifs, str):
                        try:
                            motifs = json.loads(motifs)
                        except Exception:
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
        
        # экспорт сводных
        st.download_button("⬇️ Экспорт проблем (CSV)", data=to_csv_bytes(ps), file_name="problems_summary.csv", mime="text/csv")

# ===== Связи тем =====
with tab_links:
    st.subheader("Ко‑встречаемость тем (по диалогам)")
    if df_f.empty:
        st.info("Нет данных.")
    else:
        # Собираем множество тем по диалогу → матрица пересечений
        per_dialog = df_f.groupby(["dialog_id","theme"]).size().reset_index().drop(columns=0)
        themes = sorted(per_dialog["theme"].unique())
        idx_map = {t:i for i,t in enumerate(themes)}
        n = len(themes)
        mat = np.zeros((n,n), dtype=int)
        for dlg, g in per_dialog.groupby("dialog_id"):
            ts = sorted(set(g["theme"]))
            for i in range(len(ts)):
                for j in range(i, len(ts)):
                    a, b = idx_map[ts[i]], idx_map[ts[j]]
                    mat[a,b] += 1
                    if a!=b: mat[b,a] += 1
        fig = px.imshow(mat, x=themes, y=themes, aspect="auto", color_continuous_scale="Reds", origin="lower")
        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Топ‑пар
        pairs = []
        for i in range(n):
            for j in range(i+1, n):
                pairs.append((themes[i], themes[j], int(mat[i,j])))
        pairs_df = pd.DataFrame(pairs, columns=["theme_a","theme_b","dialogs"]).sort_values("dialogs", ascending=False).head(20)
        st.dataframe(pairs_df, use_container_width=True)

# ===== Качество =====
with tab_quality:
    st.subheader("Распределение confidence")
    if df_f.empty:
        st.info("Нет данных.")
    else:
        fig = px.histogram(df_f, x="confidence", color="label_type", nbins=20, barmode="overlay", color_discrete_map=PALETTE)
        fig.update_layout(xaxis_title="confidence", yaxis_title="кол-во упоминаний")
        st.plotly_chart(fig, use_container_width=True)

        # метрики по порогам
        low = (df_f["confidence"] < 0.6).mean()*100
        med = df_f["confidence"].median()
        p90 = df_f["confidence"].quantile(0.9)
        st.caption(f"<0.6: {low:.1f}% · median: {med:.2f} · p90: {p90:.2f}")