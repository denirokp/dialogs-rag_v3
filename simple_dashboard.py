# -*- coding: utf-8 -*-
import io
import json
from pathlib import Path
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

st.set_page_config(page_title="DialogsRAG — панель разговоров", layout="wide")

# --- Универсальный выбор колонки title/id без изменения DataFrame ---

def _pick(cols, *names):
    for n in names:
        if n in cols:
            return n
    return None

def pick_title_col(df):
    # поддерживаем старый и новый варианты
    col = _pick(df.columns, "problem_title", "idea_title", "signal_title", "title")
    if not col:
        raise KeyError("Не найдена колонка с названием сущности (title).")
    return col

def pick_id_col(df):
    col = _pick(df.columns, "problem_id", "idea_id", "signal_id", "entity_id")
    if not col:
        raise KeyError("Не найдена колонка с id сущности.")
    return col

ART = Path("artifacts")
RES_PATH = ART / "comprehensive_results.json"
STATS_PATH = ART / "statistics.json"

# Проблемы (консолидация)
PM_SUM = ART / "problems_summary.csv"
PM_SUB = ART / "problems_subthemes.csv"
PM_IDX = ART / "problems_mentions.csv"
PM_CARDS = ART / "problem_cards.jsonl"
# Идеи (консолидация)
ID_SUM = ART / "ideas_summary.csv"
ID_SUB = ART / "ideas_subthemes.csv"
ID_IDX = ART / "ideas_mentions.csv"
ID_CARDS = ART / "idea_cards.jsonl"
# Сигналы (консолидация)
SG_SUM = ART / "signals_summary.csv"
SG_SUB = ART / "signals_subthemes.csv"
SG_IDX = ART / "signals_mentions.csv"
SG_CARDS = ART / "signal_cards.jsonl"

PALETTE = {"problems": "#e74c3c", "ideas": "#f1c40f", "signals": "#3498db"}

# ---------- helpers ----------

def _get_query_params():
    try:
        return dict(st.query_params)
    except Exception:
        return {k: v[0] if isinstance(v, list) and v else v for k, v in st.experimental_get_query_params().items()}


def _set_query_params(params: dict):
    try:
        st.query_params.clear(); st.query_params.update(params)
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
def load_artifacts(prefix: str):
    # Возвращает (summary, subthemes, mentions_idx, cards_df)
    paths = {
        "problems": (PM_SUM, PM_SUB, PM_IDX, PM_CARDS),
        "ideas": (ID_SUM, ID_SUB, ID_IDX, ID_CARDS),
        "signals": (SG_SUM, SG_SUB, SG_IDX, SG_CARDS),
    }[prefix]
    sum_df = pd.read_csv(paths[0]) if paths[0].exists() else pd.DataFrame()
    sub_df = pd.read_csv(paths[1]) if paths[1].exists() else pd.DataFrame()
    idx_df = pd.read_csv(paths[2]) if paths[2].exists() else pd.DataFrame()
    cards_df = pd.read_json(paths[3], lines=True) if paths[3].exists() else pd.DataFrame()
    return sum_df, sub_df, idx_df, cards_df


def file_hash() -> str:
    parts = []
    for p in [RES_PATH, STATS_PATH, PM_SUM, PM_SUB, PM_IDX, PM_CARDS, ID_SUM, ID_SUB, ID_IDX, ID_CARDS, SG_SUM, SG_SUB, SG_IDX, SG_CARDS]:
        if p.exists():
            parts.append(f"{p.name}:{int(p.stat().st_mtime)}:{p.stat().st_size}")
    return str(hash("|".join(parts)))


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


def prettify_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={
        "dialog_id":"ID звонка",
        "turn_id":"№ реплики",
        "label_type":"Тип",
        "theme":"Тема",
        "subtheme":"Подтема",
        "text_quote":"Цитата клиента",
        "confidence":"Уверенность"
    })

# ---------- data ----------

qparams = _get_query_params()
df = load_mentions()
stats = load_stats()

# автоперезагрузка при изменении артефактов
sig = file_hash()
if "_sig" not in st.session_state:
    st.session_state["_sig"] = sig
elif st.session_state["_sig"] != sig:
    st.info("Обнаружены новые файлы отчётов. Нажмите, чтобы обновить экран.")
    if st.button("🔄 Обновить дашборд"):
        st.session_state["_sig"] = sig
        st.cache_data.clear(); st.rerun()

# ---------- sidebar ----------
st.sidebar.header("Фильтры")
st.sidebar.caption("Слева выбираем, что смотреть. Все графики и таблицы ниже перестраиваются.")

metric_label = st.sidebar.radio(
    "Что считаем на графиках?",
    ["Звонки (диалоги)", "Фразы клиентов (упоминания)"], index=0,
    help="Звонки — уникальные разговоры. Фразы — количество цитат в этих разговорах."
)
metric_mode = "dialogs" if metric_label.startswith("Звонки") else "mentions"
metric_axis_name = "Звонки" if metric_mode == "dialogs" else "Фразы"

label_opts = sorted(df["label_type"].unique()) if not df.empty else ["problems","ideas","signals"]
label_default = qparams.get("labels", ",".join(label_opts)).split(",") if df.shape[0] else label_opts
label_sel = st.sidebar.multiselect(
    "Типы сущностей",
    options=label_opts, default=[x for x in label_default if x in label_opts],
    help="Проблемы — жалобы; Идеи — просьбы и предложения; Сигналы — позитив/негатив/нейтральные реакции."
)

conf_min, conf_max = st.sidebar.slider(
    "Надёжность распознавания (confidence)", 0.0, 1.0,
    (float(qparams.get("cmin", 0.0)), float(qparams.get("cmax", 1.0))), 0.05,
    help="0 — нет уверенности, 1 — очень уверенно. Обычно смотрим ≥ 0.6."
)

search = st.sidebar.text_input(
    "Поиск по словам клиента",
    value=qparams.get("q", ""),
    help="Например: подписка, доставка, возврат"
)

if not df.empty:
    df_seed = df[df["label_type"].isin(label_sel) & df["confidence"].between(conf_min, conf_max)]
    theme_opts = sorted(df_seed["theme"].unique())
    theme_default = [t for t in qparams.get("themes", ",".join(theme_opts)).split(",") if t in theme_opts]
    theme_sel = st.sidebar.multiselect("Темы", options=theme_opts, default=(theme_default or theme_opts), help="Группа вопросов. Например: доставка, поддержка.")
    sub_opts = sorted(df_seed[df_seed["theme"].isin(theme_sel)]["subtheme"].unique())
    sub_default = [t for t in qparams.get("subs", ",".join(sub_opts)).split(",") if t in sub_opts]
    sub_sel = st.sidebar.multiselect("Подтемы", options=sub_opts, default=(sub_default or sub_opts), help="Более конкретный аспект темы.")
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
share_url = "?" + urlencode(current_params)

with st.sidebar.expander("Пресеты и шаринг", expanded=False):
    st.caption("Ссылка ниже откроет этот же срез для коллег.")
    st.code(share_url, language="text")
    if st.button("📎 Применить ссылку в адресной строке"):
        _set_query_params(current_params)
    st.caption("Можно добавить в закладки браузера.")

# ---------- HEADER / KPI ----------
st.title("Панель разговоров — понятным языком")
st.caption("Эта панель помогает быстро увидеть, о чём говорят клиенты: где болит (проблемы), что просят (идеи) и как реагируют (сигналы).")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Звонков", value=stats.get("dialogs", 0), help="Сколько уникальных разговоров обработано")
col2.metric("Фраз клиентов", value=stats.get("mentions", 0), help="Сколько кусочков речи мы извлекли из этих разговоров")
col3.metric("Проблем", value=stats.get("problems", 0))
col4.metric("Идей", value=stats.get("ideas", 0))
col5.metric("Сигналов", value=stats.get("signals", 0))

st.caption(
    f"Доказательства: {'все есть ✅' if stats.get('evidence_100') else 'не везде ❌'}  •  "
    f"Дубликаты сняты: {stats.get('dedup_removed_pct', 0)}%  •  "
    f"Сомнительных (<0.6): {stats.get('ambiguity_pct', 0)}%"
)

# ---------- tabs ----------
(
    tab_overview,
    tab_problems_raw,
    tab_ideas_raw,
    tab_signals_raw,
    tab_problems_cons,
    tab_ideas_cons,
    tab_signals_cons,
    tab_links,
    tab_quality,
) = st.tabs([
    "Обзор",
    "Проблемы — список",
    "Идеи — список",
    "Сигналы — список",
    "Сводка по проблемам",
    "Сводка по идеям",
    "Сводка по сигналам",
    "Связи тем",
    "Качество распознавания",
])

# ===== Обзор =====
with tab_overview:
    st.info("""
    **Как читать экран:**
    1) Слева выберите типы сущностей и нужные темы. 
    2) Ниже три графика покажут самые частые темы для проблем, идей и сигналов. 
    3) Можно переключить расчёт: **Звонки** (сколько разговоров затрагивали тему) или **Фразы** (сколько цитат).
    """)

    if df_f.empty:
        st.warning("Нет данных для отображения. Проверьте фильтры слева.")
    else:
        st.subheader("Топ тем по типам (чем чаще — тем выше)")
        topn = 8
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
        for lbl in ["problems","ideas","signals"]:
            d = agg[agg["label_type"]==lbl].sort_values("value", ascending=False).head(topn)
            if d.empty: 
                st.caption(f"Нет данных для: {lbl}")
                continue
            title_lbl = {"problems":"Проблемы","ideas":"Идеи","signals":"Сигналы"}[lbl]
            fig = px.bar(
                d.sort_values("value"),
                x="value", y="theme", orientation="h",
                title=f"{title_lbl}: самые частые темы ({metric_axis_name})",
                color_discrete_sequence=[PALETTE.get(lbl, "#777")], text="value",
            )
            fig.update_layout(yaxis_title="Тема", xaxis_title=metric_axis_name)
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Из чего состоит тема (100% столбик)")
        st.caption("Выберите тип и тему — увидите, какие подтемы дают основной вклад. Сумма всегда = 100%.")
        sel_lbl = st.selectbox("Тип сущности", ["problems","ideas","signals"], index=0)
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
        else:
            st.info("Для выбранных фильтров нет тем.")
        
        st.markdown("---")
        st.subheader("Карта тем → подтем (Treemap)")
        st.caption("Размер прямоугольника — сколько фраз. Наводите курсор для деталей.")
        agg2 = (
            df_f.groupby(["label_type", "theme", "subtheme"], as_index=False)["text_quote"].count()
        ).rename(columns={"text_quote": "mentions"})
        fig = px.treemap(
            agg2, path=["label_type", "theme", "subtheme"], values="mentions",
            color="label_type", color_discrete_map=PALETTE,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Сырые фразы (по выбранным фильтрам)")
        st.caption("Это реальные цитаты клиентов. Сначала — короткий список, можно скачать файл ниже.")
        hl = st.toggle("Подсветить слово поиска в цитатах", value=bool(search.strip()))
        table = df_f.sort_values(["label_type","theme","subtheme"]).reset_index(drop=True)
        if hl and search.strip():
            t = table.copy(); t["text_quote"] = t["text_quote"].apply(lambda x: highlight_html(x, search))
            st.markdown(prettify_table(t).to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.dataframe(prettify_table(table), use_container_width=True, height=440)
        
        colx, coly = st.columns(2)
        with colx:
            st.download_button("⬇️ Скачать CSV (то, что на экране)", data=to_csv_bytes(table), file_name="mentions_filtered.csv", mime="text/csv")
        with coly:
            st.download_button("⬇️ Скачать Excel", data=to_excel_bytes({"Цитаты": prettify_table(table)}), file_name="mentions_filtered.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ===== RAW =====

def render_raw(df_src: pd.DataFrame, label: str):
    name = {"problems":"Проблемы","ideas":"Идеи","signals":"Сигналы"}[label]
    st.info(f"Здесь список тем и цитат для: **{name}**. Слева можно сузить фильтры.")
    d = df_src[df_src["label_type"] == label]
    if d.empty:
        st.warning("Нет строк.")
        return
    c1, c2 = st.columns([1,1])
    top_dialogs = d.groupby("theme")["dialog_id"].nunique().rename("Звонки")
    top_mentions = d.groupby("theme")["text_quote"].count().rename("Фразы")
    top = pd.concat([top_dialogs, top_mentions], axis=1).reset_index().fillna(0)
    top = top.sort_values("Звонки", ascending=False).head(15)
    fig = px.bar(top.melt(id_vars="theme", value_vars=["Звонки","Фразы"]),
                 x="value", y="theme", color="variable", barmode="group",
                 orientation="h", title=f"{name}: темы — звонки vs фразы")
    fig.update_layout(xaxis_title="Количество", yaxis_title="Тема")
    st.plotly_chart(fig, use_container_width=True)
    
    sub_counts = d.groupby("subtheme").size().sort_values(ascending=False).head(12)
    top_sub = d[d["subtheme"].isin(sub_counts.index)]
    if not top_sub.empty:
        fig2 = px.box(top_sub, x="subtheme", y="confidence", points="outliers",
                      title="Надёжность распознавания по подтемам (чем правее — тем лучше)")
        fig2.update_layout(xaxis_tickangle=30)
        st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("---")
    st.dataframe(prettify_table(d.sort_values(["theme","subtheme","confidence"], ascending=[True,True,False])),
                 use_container_width=True, height=480)

with tab_problems_raw: render_raw(df_f, "problems")
with tab_ideas_raw:    render_raw(df_f, "ideas")
with tab_signals_raw:  render_raw(df_f, "signals")

# ===== Консолидация =====

def render_consolidation(prefix: str, title: str, icon: str):
    st.header(f"{icon} {title}")
    st.info(f"""
    **Зачем эта вкладка:** мы объединяем похожие формулировки в укрупнённые {title.lower()}. 
    Ниже видно, какие из них встречаются чаще всего и из каких подтем они складываются.
    """)
    
    sum_path = {
        "problems": PM_SUM, "ideas": ID_SUM, "signals": SG_SUM
    }[prefix]
    
    if not sum_path.exists():
        st.warning(f"Нет artifacts/{prefix}_summary.csv — запустите consolidate_and_summarize.py")
    else:
        sum_df, sub_df, idx_df, cards_df = load_artifacts(prefix)
        st.subheader(f"Таблица по всем {title.lower()}")
        st.caption("Звонки — в скольких разговорах всплывала тема. Фразы — сколько цитат внутри них.")
        
        # Универсальный выбор колонок
        title_col = pick_title_col(sum_df)
        id_col = pick_id_col(sum_df)
        
        show_cols = [id_col, title_col, "dialogs", "mentions", "share_dialogs_pct", "freq_per_1k", "intensity_mpd"]
        view = sum_df[show_cols].rename(columns={
            id_col: "id",
            title_col: "название",
            "dialogs": "звонки",
            "mentions": "фразы",
            "share_dialogs_pct": "доля звонков, %",
            "freq_per_1k": "на 1000 звонков",
            "intensity_mpd": "фраз на звонок",
        })
        st.dataframe(view, use_container_width=True)
        
        st.subheader("Какие 20% дают 80% охвата (Pareto)")
        st.caption("Столбики — звонки, линия — накопленная доля звонков, %.")
        d = sum_df.sort_values("dialogs", ascending=False).copy()
        d["cum_share"] = (d["dialogs"].cumsum() / max(1, d["dialogs"].sum()) * 100).round(1)
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=d[title_col], y=d["dialogs"], name="Звонки"), secondary_y=False)
        fig.add_trace(go.Scatter(x=d[title_col], y=d["cum_share"], name="Накопл. доля, %", mode="lines+markers"), secondary_y=True)
        fig.update_layout(title_text=f"Pareto: разговоры по {title.lower()}", xaxis_tickangle=30)
        fig.update_yaxes(title_text="Звонки", secondary_y=False)
        fig.update_yaxes(title_text="Доля, %", secondary_y=True, range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Как подтемы перетекают в тему (Sankey)")
        st.caption("Толстая линия = больше разговоров. Слева — подтемы, справа — укрупнённая тема.")
        if not idx_df.empty:
            # Универсальный выбор колонок для idx_df
            idx_title_col = pick_title_col(idx_df)
            idx_id_col = pick_id_col(idx_df)
            
            g = idx_df.groupby(["theme","subtheme", idx_id_col, idx_title_col])['dialog_id'].nunique().reset_index(name="dialogs")
            subs = g.apply(lambda r: f"{r['theme']} / {r['subtheme']}", axis=1).unique().tolist()
            probs = g[idx_title_col].unique().tolist()
            nodes = subs + probs
            idx_map = {name: i for i, name in enumerate(nodes)}
            src = [idx_map[f"{r.theme} / {r.subtheme}"] for r in g.itertuples()]
            dst = [idx_map[getattr(r, idx_title_col)] for r in g.itertuples()]
            val = [int(r.dialogs) for r in g.itertuples()]
            sankey = go.Sankey(node=dict(label=nodes), link=dict(source=src, target=dst, value=val))
            st.plotly_chart(go.Figure(sankey), use_container_width=True)
        else:
            st.info("Нет индекса цитат для Sankey.")

        st.subheader("Насколько хорошо покрыта карта соответствий (Heatmap)")
        st.caption("Где ячейка пустая — карту можно обогатить (подтема ещё не привязана к теме).")
        if not idx_df.empty:
            cov = idx_df.groupby(["theme", idx_title_col])['dialog_id'].nunique().reset_index(name="dialogs")
            pivot = cov.pivot(index="theme", columns=idx_title_col, values="dialogs").fillna(0)
            fig = px.imshow(pivot, aspect="auto", color_continuous_scale="Blues", origin="lower")
            st.plotly_chart(fig, use_container_width=True)
        
        if not sum_df.empty and (sum_df[id_col]=="other_unmapped").any():
            unm = sum_df[sum_df[id_col]=="other_unmapped"].iloc[0]
            st.warning(f"Прочее/не сконсолидировано: {unm['share_dialogs_pct']}% звонков · {int(unm['mentions'])} фраз.")
            if not idx_df.empty:
                cand = (idx_df[idx_df[idx_id_col]=="other_unmapped"]
                        .groupby(["theme","subtheme"])['dialog_id'].nunique().reset_index(name="dialogs")
                        .sort_values("dialogs", ascending=False).head(15))
                st.caption("Это подсказки, что добавить в карту соответствий.")
                st.dataframe(cand, use_container_width=True)
        
        st.subheader(f"Карточки {title.lower()} — человеческим языком")
        for _, row in sum_df.sort_values("dialogs", ascending=False).iterrows():
            pid, title_text = row[id_col], row[title_col]
            with st.expander(f"{title_text} — {int(row['mentions'])} фраз · {int(row['dialogs'])} звонков ({row['share_dialogs_pct']}%)"):
                if not cards_df.empty and pid in set(cards_df.get(id_col, pd.Series()).values):
                    js = cards_df[cards_df[id_col] == pid].iloc[0]
                    st.markdown(f"**О чём речь.** {js.get('definition','')}")
                    st.markdown(f"**Почему это важно.** {js.get('why_it_matters','')}")
                    motifs = js.get("common_motifs", [])
                    if isinstance(motifs, str):
                        try: motifs = json.loads(motifs)
                        except Exception: motifs = [motifs]
                    if motifs: st.markdown("**Частые мотивы:** " + ", ".join(motifs))
                if not sub_df.empty:
                    st.markdown("**Подтемы (топ):**")
                    st.dataframe(sub_df[sub_df[id_col] == pid].head(10), use_container_width=True)
                if not idx_df.empty:
                    st.markdown("**Примеры фраз:**")
                    cols = ["dialog_id","turn_id","theme","subtheme","text_quote","confidence"]
                    st.dataframe(prettify_table(idx_df[idx_df[idx_id_col] == pid][cols]).rename(columns={"ID звонка":"dialog_id"}),
                                 use_container_width=True)
        st.download_button(f"⬇️ Скачать CSV со сводкой {title.lower()}", data=to_csv_bytes(sum_df), file_name=f"{prefix}_summary.csv", mime="text/csv")

with tab_problems_cons: render_consolidation("problems", "Сводка по проблемам", "🚫")
with tab_ideas_cons: render_consolidation("ideas", "Сводка по идеям", "💡")
with tab_signals_cons: render_consolidation("signals", "Сводка по сигналам", "📊")

# ===== Связи тем =====
with tab_links:
    st.info("""
    **Что здесь:** матрица показывает, какие темы встречаются **в одном и том же звонке**. 
    Чем темнее клетка, тем чаще пара встречается вместе.
    """)
    if df_f.empty:
        st.warning("Нет данных.")
    else:
        per_dialog = df_f.groupby(["dialog_id","theme"]).size().reset_index().drop(columns=0)
        themes = sorted(per_dialog["theme"].unique())
        idx_map = {t:i for i,t in enumerate(themes)}
        n = len(themes)
        mat = np.zeros((n,n), dtype=int)
        for _, g in per_dialog.groupby("dialog_id"):
            ts = sorted(set(g["theme"]))
            for i in range(len(ts)):
                for j in range(i, len(ts)):
                    a, b = idx_map[ts[i]], idx_map[ts[j]]
                    mat[a,b] += 1
                    if a!=b: mat[b,a] += 1
        fig = px.imshow(mat, x=themes, y=themes, aspect="auto", color_continuous_scale="Reds", origin="lower")
        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        pairs = []
        for i in range(n):
            for j in range(i+1, n):
                pairs.append((themes[i], themes[j], int(mat[i,j])))
        pairs_df = pd.DataFrame(pairs, columns=["Тема A","Тема B","Звонки вместе"]).sort_values("Звонки вместе", ascending=False).head(20)
        st.dataframe(pairs_df, use_container_width=True)

# ===== Качество =====
with tab_quality:
    st.info("""
    **Подсказка:** хорошая картина — когда большинство точек правее 0.6. 
    Если много слева, смотрим темы на RAW‑вкладках и корректируем правила/таксономию.
    """)
    if df_f.empty:
        st.warning("Нет данных.")
    else:
        fig = px.histogram(df_f, x="confidence", color="label_type", nbins=20, barmode="overlay", color_discrete_map=PALETTE)
        fig.update_layout(xaxis_title="Надёжность (0…1)", yaxis_title="Сколько фраз")
        st.plotly_chart(fig, use_container_width=True)
        low = (df_f["confidence"] < 0.6).mean()*100
        med = df_f["confidence"].median()
        p90 = df_f["confidence"].quantile(0.9)
        st.caption(f"Меньше 0.6: {low:.1f}%  •  Медиана: {med:.2f}  •  90-й перцентиль: {p90:.2f}")