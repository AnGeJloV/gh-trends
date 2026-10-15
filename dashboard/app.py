"""Радар IT-трендов — дашборд по открытым логам GitHub (GH Archive).

Человек 4 (BI). Дашборд только читает готовые агрегаты из ClickHouse:
витрины Spark-ETL (gh.mart_*) и результаты ML (gh.ml_repo_predictions,
gh.ml_model_metrics). Свои агрегаты не считает и ничего не пишет в БД.
"""
import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import clickhouse_connect

# ------------------------------------------------------------- настройки ---

DB = os.getenv("CLICKHOUSE_DB", "gh")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

ACCENT = "#7c5cff"
PALETTE = px.colors.qualitative.Plotly

# Единственная тема — тёмная (задана и в .streamlit/config.toml).
T = dict(template="plotly_dark", text="#e8ebf4", grid="rgba(255,255,255,.06)",
         muted="#9aa3b5", border="rgba(124,92,255,.25)", panel="#141926",
         card="linear-gradient(135deg, rgba(124,92,255,.16), rgba(124,92,255,.02))",
         sidebar="linear-gradient(180deg, #141926 0%, #0b0e14 100%)", app_bg="#0b0e14")


# ------------------------------------------------------------ подключение ---

@st.cache_resource(show_spinner="Подключаюсь к ClickHouse…")
def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD,
        database=DB,
    )


@st.cache_data(ttl=300, show_spinner="Выполняю запрос…")
def qdf(sql: str) -> pd.DataFrame:
    """SELECT -> DataFrame, с кэшем и мягкой деградацией до пустой таблицы."""
    try:
        return get_client().query_df(sql)
    except Exception as e:
        st.sidebar.warning(f"Запрос не выполнен: {e.__class__.__name__}: {e}")
        return pd.DataFrame()


def table_exists(table: str) -> bool:
    try:
        return get_client().query(f"EXISTS TABLE {DB}.{table}").result_rows[0][0] == 1
    except Exception:
        return False


def empty_state(text: str):
    st.info(f"🪄 {text}")


# ------------------------------------------------------------------- стили ---

st.set_page_config(
    page_title="Радар IT-трендов",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)


def build_css() -> str:
    """Кастомный CSS тёмной темы: скрывает служебные элементы,
    оформляет карточки и сайдбар."""
    return f"""
<style>
    .block-container {{ padding-top: 1.4rem; padding-bottom: 2rem; }}
    h1, h2, h3, h4 {{ letter-spacing: .3px; }}
    /* спрятать служебные элементы: Deploy, меню, декор-полосу */
    [data-testid="stToolbar"], [data-testid="stDecoration"],
    [data-testid="stStatusWidget"], #MainMenu {{ display: none !important; }}
    [data-testid="stHeader"] {{ background: transparent; }}
    /* сайдбар нельзя закрыть: прячем кнопку ««»; на случай свёрнутого
       состояния (мобильные) кнопку разворота наоборот показываем */
    [data-testid="stSidebarCollapseButton"] {{ display: none !important; }}
    [data-testid="stExpandSidebarButton"],
    [data-testid="collapsedControl"]
        {{ display: flex !important; visibility: visible !important; }}
    /* карточки-метрики */
    div[data-testid="stMetric"] {{
        background: {T['card']};
        border: 1px solid {T['border']};
        border-radius: 14px;
        padding: 16px 20px 12px 20px;
    }}
    div[data-testid="stMetric"] label {{ color: {T['muted']} !important; }}
    div[data-testid="stMetricValue"] {{ font-size: 1.65rem !important; color: {T['text']}; }}
    div[data-testid="stDataFrame"] {{ border-radius: 12px; overflow: hidden; }}
    section[data-testid="stSidebar"] {{ background: {T['sidebar']}; }}
    /* компактный сайдбар, чтобы не листать */
    section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p
        {{ margin-bottom: .15rem !important; }}
    section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{ gap: .3rem !important; }}
    section[data-testid="stSidebar"] hr {{ margin: .35rem 0 !important; }}
    section[data-testid="stSidebar"] h2 {{ font-size: 1.05rem !important; margin: 0 !important; }}
    section[data-testid="stSidebar"] [data-testid="stDateInputField"] input
        {{ font-size: .85rem !important; }}
    /* меню разделов: пилюли друг под другом, как в Power BI */
    section[data-testid="stSidebar"] [data-testid="stSegmentedControl"]
        [data-baseweb="button-group"]
        {{ flex-direction: column !important; align-items: stretch !important; }}
    section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button
        {{ justify-content: flex-start !important; padding-left: 14px !important;
           border-radius: 10px !important; width: 100% !important; }}
    /* контент сайдбара растянут по высоте экрана, кнопка темы — внизу */
    [data-testid="stSidebarContent"] {{ display: flex; flex-direction: column;
        min-height: calc(100vh - 70px); }}
    .side-flex {{ flex: 1 1 auto; min-height: 16px; }}
    /* фон приложения */
    [data-testid="stAppViewContainer"], [data-testid="stAppViewContainer"] .main
        {{ background: {T['app_bg']}; }}
</style>
"""


# ------------------------------------------------------------- глобальные ---

HAVE_MARTS = table_exists("mart_hourly_activity")
HAVE_PRED = table_exists("ml_repo_predictions")
HAVE_METRICS = table_exists("ml_model_metrics")

# Фактический диапазон данных читаем из ClickHouse — не хардкодим.
data_min = data_max = None
if HAVE_MARTS:
    bounds = qdf("SELECT min(hour) AS mn, max(hour) AS mx FROM " + DB + ".mart_hourly_activity")
    if not bounds.empty:
        data_min = pd.to_datetime(bounds["mn"].iloc[0]).date()
        data_max = pd.to_datetime(bounds["mx"].iloc[0]).date()

with st.sidebar:
    st.markdown("#### 🛰️ Радар IT-трендов")
    st.caption("Пульс open-source · [GH Archive](https://www.gharchive.org/)")

    NAV = ["🏠 Обзор", "💬 Языки", "📦 Репозитории", "🤖 ML-прогнозы", "ℹ️ О проекте"]
    page = st.segmented_control("Раздел", NAV,
                                default=st.session_state.get("nav_page", NAV[0]),
                                key="seg_nav", label_visibility="collapsed")
    page = page or st.session_state.get("nav_page", NAV[0])
    st.session_state.nav_page = page

    d_from, d_to = date.today() - timedelta(days=30), date.today()
    if data_min and data_max:
        period = st.date_input("Период данных", value=(data_min, data_max),
                               min_value=data_min, max_value=data_max,
                               format="DD.MM.YYYY")
        d_from, d_to = (period if isinstance(period, tuple) and len(period) == 2
                        else (data_min, data_max))
    else:
        st.info("Витрины пока пусты — поднимите пайплайн (`docker compose up -d`).")

    # распорка: прижимает низ сайдбара к низу экрана
    st.markdown('<div class="side-flex"></div>', unsafe_allow_html=True)
    st.markdown(build_css(), unsafe_allow_html=True)
    st.caption(f"ClickHouse `{CH_HOST}:{CH_PORT}` · БД `{DB}`")


def dt_cond(col: str = "hour") -> str:
    return (f"{col} >= toDateTime('{d_from} 00:00:00') "
            f"AND {col} < toDateTime('{d_to + timedelta(days=1)} 00:00:00')")


def style_fig(fig, height=380, legend_h=True):
    fig.update_layout(
        template=T["template"],
        height=height,
        margin=dict(l=10, r=10, t=46, b=10),
        legend=dict(x=1.02, y=1) if legend_h else {},
        hovermode="x unified",
        hoverlabel=dict(bgcolor=T["panel"], bordercolor=T["border"],
                        font=dict(color=T["text"])),
        font=dict(color=T["text"]),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor=T["grid"])
    fig.update_yaxes(gridcolor=T["grid"])
    return fig


def fmt(n) -> str:
    return f"{int(n):,}".replace(",", " ")


# =============================================================== ОБЗОР =====

if page == "🏠 Обзор":
    st.header("Обзор активности за период")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет. Запустите пайплайн: `docker compose up -d`.")
    else:
        kpi = qdf(f"""
            SELECT count() AS events,
                   uniqExact(actor_id) AS actors,
                   uniqExact(repo_id) AS repos,
                   countIf(event_type = 'WatchEvent') AS stars,
                   countIf(event_type = 'ForkEvent') AS forks,
                   countIf(event_type = 'PullRequestEvent') AS prs,
                   countIf(event_type = 'PushEvent') AS pushes
            FROM {DB}.events_parsed
            WHERE {dt_cond("created_at")}
        """)

        if kpi.empty or kpi["events"].iloc[0] == 0:
            empty_state("За выбранный период событий нет — расширьте диапазон дат в сайдбаре.")
        else:
            row = kpi.iloc[0]
            days = max((d_to - d_from).days + 1, 1)
            peak = qdf(f"SELECT max(event_count) AS peak FROM {DB}.mart_hourly_activity WHERE {dt_cond()}")
            c = st.columns(4)
            c[0].metric("Событий всего", fmt(row["events"]),
                        f"{int(row['events']) / days:,.0f} / день".replace(",", " "))
            c[1].metric("Активных авторов", fmt(row["actors"]))
            c[2].metric("Репозиториев", fmt(row["repos"]))
            c[3].metric("⬆️ Пушей", fmt(row["pushes"]))
            c2 = st.columns(4)
            c2[0].metric("⭐ Звёзд", fmt(row["stars"]))
            c2[1].metric("🍴 Форков", fmt(row["forks"]))
            c2[2].metric("🔀 Pull Request'ов", fmt(row["prs"]))
            c2[3].metric("Пик за час", fmt(peak["peak"].iloc[0]) if not peak.empty else "—",
                         "макс. событий в один час")

            left, right = st.columns([3, 2])

            with left:
                daily = qdf(f"""
                    SELECT toDate(hour) AS day, sum(event_count) AS events
                    FROM {DB}.mart_hourly_activity
                    WHERE {dt_cond()}
                    GROUP BY day ORDER BY day
                """)
                if not daily.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=daily["day"], y=daily["events"], fill="tozeroy",
                        mode="lines", line=dict(color=ACCENT, width=2.5, shape="spline"),
                        name="События",
                    ))
                    fig.update_layout(title="Активность по дням")
                    st.plotly_chart(style_fig(fig), width="stretch", theme=None)

            with right:
                mix = qdf(f"""
                    SELECT event_type, sum(event_count) AS events
                    FROM {DB}.mart_hourly_activity
                    WHERE {dt_cond()}
                    GROUP BY event_type ORDER BY events DESC
                """)
                if not mix.empty:
                    fig = px.pie(mix, names="event_type", values="events", hole=.55,
                                 title="Структура по типам событий")
                    # легенда справа вертикально — не налезает на заголовок
                    fig.update_traces(textposition="inside", textinfo="percent",
                                      hovertemplate="%{label}<br>%{value:,} событий (%{percent})<extra></extra>")
                    fig.update_layout(showlegend=True,
                                      legend=dict(orientation="v", x=1.0, y=0.5))
                    st.plotly_chart(style_fig(fig, 380, legend_h=False), width="stretch", theme=None)

            # Тепловая карта: день недели × час суток (UTC)
            heat = qdf(f"""
                SELECT toDayOfWeek(hour) AS dow, toHour(hour) AS h, sum(event_count) AS events
                FROM {DB}.mart_hourly_activity
                WHERE {dt_cond()}
                GROUP BY dow, h
            """)
            if not heat.empty:
                pivot = heat.pivot(index="dow", columns="h", values="events").fillna(0)
                pivot = pivot.reindex(index=range(1, 8), columns=range(24), fill_value=0)
                day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
                fig = px.imshow(
                    pivot, aspect="auto", color_continuous_scale="Viridis",
                    labels=dict(x="Час суток (UTC)", y="", color="Событий"),
                )
                fig.update_layout(title="Карта активности: когда мир коммитит")
                fig.update_yaxes(tickvals=list(range(7)), ticktext=day_names)
                fig.update_xaxes(tickvals=list(range(0, 24, 2)))
                st.plotly_chart(style_fig(fig, 330), width="stretch", theme=None)

# =============================================================== ЯЗЫКИ =====

if page == "💬 Языки":
    st.header("Популярность языков программирования")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет — запустите пайплайн.")
    else:
        # Считаем напрямую из events_parsed: витрина mart_language_activity
        # сейчас содержит некорректные star_count/pr_count (баг ETL).
        # Это по-прежнему только чтение — агрегацию делает ClickHouse.
        lang_stats = qdf(f"""
            SELECT language,
                   count() AS events,
                   uniqExact(actor_id) AS actors,
                   countIf(event_type = 'WatchEvent') AS stars,
                   countIf(event_type = 'ForkEvent') AS forks,
                   countIf(event_type = 'PullRequestEvent') AS prs
            FROM {DB}.events_parsed
            WHERE {dt_cond("created_at")} AND language != 'Unknown'
            GROUP BY language ORDER BY events DESC
            LIMIT 25
        """)
        top_langs = lang_stats
        # Звёзды и PR в разрезе языков не показываем: язык известен
        # только из ForkEvent (forkee.language), там звёзд и PR нет.

        if lang_stats.empty:
            empty_state("Данных по языкам нет за этот период.")
        else:
            sel = st.multiselect(
                "Языки для сравнения в трендах",
                options=top_langs["language"].tolist(),
                default=top_langs["language"].head(8).tolist(),
            )

            left, right = st.columns([2, 3])

            with left:
                fig = px.bar(
                    top_langs.head(15).sort_values("events"),
                    x="events", y="language", orientation="h",
                    color="events", color_continuous_scale="Purples",
                    title="Топ-15 языков по событиям",
                )
                fig.update_layout(coloraxis_showscale=False)
                fig.update_yaxes(title="")
                st.plotly_chart(style_fig(fig, 520, legend_h=False), width="stretch", theme=None)

            with right:
                if sel:
                    # список для IN — в круглых скобках, кавычки экранируем
                    langs = ", ".join("'" + l.replace("'", "") + "'" for l in sel)
                    trend = qdf(f"""
                        SELECT toDate(created_at) AS day, language,
                               count() AS events,
                               countIf(event_type = 'WatchEvent') AS stars
                        FROM {DB}.events_parsed
                        WHERE {dt_cond("created_at")} AND language IN ({langs})
                        GROUP BY day, language ORDER BY day
                    """)
                    if not trend.empty:
                        fig = px.line(
                            trend, x="day", y="events", color="language",
                            color_discrete_sequence=PALETTE,
                            title="Тренд активности (форков) по дням",
                        )
                        fig.update_traces(line=dict(width=2.5))
                        st.plotly_chart(style_fig(fig, 300), width="stretch", theme=None)
                else:
                    st.info("Выберите языки, чтобы построить тренды.")

            if not lang_stats.empty:
                tbl = lang_stats[["language", "events", "actors"]].head(15)
                tbl.columns = ["Язык", "Форки", "Авторы"]
                st.markdown("##### Сводная таблица по языкам")
                st.dataframe(
                    tbl, width="stretch", hide_index=True, height=420,
                    column_config={
                        "Форки": st.column_config.NumberColumn(format="%d"),
                        "Авторы": st.column_config.NumberColumn(format="%d"),
                    },
                )

# ========================================================== РЕПОЗИТОРИИ ====

if page == "📦 Репозитории":
    st.header("Репозитории: кто набирает популярность")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет — запустите пайплайн.")
    else:
        metric = st.segmented_control(
            "Метрика роста",
            ["⭐ Звёзды", "🍴 Форки", "⬆️ Пуши", "Все события"],
            default="⭐ Звёзды",
        )
        # Считаем из events_parsed: колонка commits_count в ETL не заполняется
        # (коммитов в данных нет), поэтому вместо коммитов — пуши (PushEvent).
        order_col = {"⭐ Звёзды": "stars", "🍴 Форки": "forks",
                     "⬆️ Пуши": "pushes", "Все события": "events"}[metric]
        metric_col = {"⭐ Звёзды": "⭐", "🍴 Форки": "🍴",
                      "⬆️ Пуши": "Пуши", "Все события": "События"}[metric]

        days = max((d_to - d_from).days + 1, 1)
        top_repos = qdf(f"""
            SELECT repo_name,
                   count() AS events,
                   countIf(event_type = 'WatchEvent') AS stars,
                   countIf(event_type = 'ForkEvent') AS forks,
                   countIf(event_type = 'PushEvent') AS pushes
            FROM {DB}.events_parsed
            WHERE {dt_cond("created_at")}
            GROUP BY repo_id, repo_name
            ORDER BY {order_col} DESC
            LIMIT 20
        """)

        if top_repos.empty:
            empty_state("Нет данных по репозиториям за период.")
        else:
            per_day_col = f"{metric_col}/день"
            top_repos[per_day_col] = (top_repos[order_col] / days).round(1)
            pretty = top_repos.rename(columns={
                "repo_name": "Репозиторий", "events": "События", "stars": "⭐",
                "forks": "🍴", "pushes": "Пуши", "per_day": per_day_col,
            })
            fig = px.bar(
                pretty.head(15), x=metric_col, y="Репозиторий", orientation="h",
                color=metric_col, color_continuous_scale="Purples",
                title=f"Топ-15 репозиториев: {metric.lower()} за период",
            )
            fig.update_layout(coloraxis_showscale=False)
            fig.update_yaxes(autorange="reversed", title="")
            fig.update_xaxes(title=None)
            st.plotly_chart(style_fig(fig, 560, legend_h=False), width="stretch", theme=None)

            st.markdown("##### Топ-20 в числах")
            st.dataframe(
                pretty[["Репозиторий", "События", "⭐", "🍴", "Пуши", per_day_col]],
                width="stretch", hide_index=True, height=520,
                column_config={
                    "События": st.column_config.NumberColumn(format="%d"),
                    "⭐": st.column_config.NumberColumn(format="%d"),
                    "🍴": st.column_config.NumberColumn(format="%d"),
                    "Пуши": st.column_config.NumberColumn(format="%d"),
                    per_day_col: st.column_config.NumberColumn(format="%.1f"),
                },
            )

            st.divider()
            st.markdown("##### 📈 Детально по одному репозиторию")
            repo_choice = st.selectbox("Репозиторий", top_repos["repo_name"].tolist())
            if repo_choice:
                safe = repo_choice.replace("'", "")
                detail = qdf(f"""
                    SELECT toDate(created_at) AS day,
                           count() AS events,
                           countIf(event_type = 'WatchEvent') AS stars,
                           countIf(event_type = 'ForkEvent') AS forks,
                           countIf(event_type = 'PushEvent') AS pushes
                    FROM {DB}.events_parsed
                    WHERE {dt_cond("created_at")} AND repo_name = '{safe}'
                    GROUP BY day ORDER BY day
                """)
                if not detail.empty:
                    fig = go.Figure()
                    traces = [("events", "События", ACCENT), ("stars", "⭐ Звёзды", "#f5c451"),
                              ("forks", "🍴 Форки", "#42d392"), ("pushes", "Пуши", "#4fc3f7")]
                    for col_, name, color in traces:
                        fig.add_trace(go.Scatter(
                            x=detail["day"], y=detail[col_], name=name,
                            mode="lines+markers", line=dict(color=color, width=2, shape="spline"),
                            marker=dict(size=4),
                        ))
                    fig.update_layout(title=f"Динамика: {repo_choice}")
                    st.plotly_chart(style_fig(fig), width="stretch", theme=None)

# ========================================================= ML-ПРОГНОЗЫ =====

if page == "🤖 ML-прогнозы":
    st.header("🤖 Прогноз заброшенности репозиториев")

    if not (HAVE_PRED or HAVE_METRICS):
        empty_state("ML-таблиц ещё нет: ml-trainer не запускался или данных меньше ~3 недель. "
                    "Запустите `docker compose --profile pipeline up -d`.")
    else:
        with st.expander("❓ Что это такое — простыми словами", expanded=True):
            st.markdown("""
Модель смотрит, **как репозиторий жил последние 14 дней** (коммиты, звёзды, число авторов,
затухание активности) и предсказывает: **будет ли в нём хоть какое-то движение в следующие 7 дней.**

- **🪦 «Заброшен»** — прогноз: движений не будет, репозиторий, скорее всего, забросят.
- **✅ «Активен»** — прогноз: жизнь продолжается.
- Вероятность 0–100% — уверенность модели. Порог подобран автоматически по максимуму F1,
  поэтому часть репозиториев с небольшой вероятностью тоже попадает в «рискованные».
""")

        if HAVE_METRICS:
            metrics = qdf(f"""
                SELECT model, roc_auc, pr_auc, f1, precision, recall,
                       precision_at_k, recall_at_k, k
                FROM {DB}.ml_model_metrics
                WHERE trained_at = (SELECT max(trained_at) FROM {DB}.ml_model_metrics)
            """)
            with st.expander("📈 Насколько модели доверять (метрики качества)"):
                if not metrics.empty:
                    best = metrics.sort_values("pr_auc", ascending=False).iloc[0]
                    mc = st.columns(4)
                    mc[0].metric("Лучшая модель", best["model"])
                    mc[1].metric("PR-AUC", f"{best['pr_auc']:.3f}",
                                 "умение находить рискованные")
                    mc[2].metric("ROC-AUC",
                                 f"{best['roc_auc']:.3f}" if pd.notna(best["roc_auc"]) else "—",
                                 "общее качество разделения")
                    mc[3].metric("Точность топ-списка",
                                 f"{best['precision_at_k']:.1%}" if pd.notna(best["precision_at_k"]) else "—",
                                 f"в списке K={int(best['k'])}")


        if HAVE_PRED:
            latest = qdf("SELECT max(snapshot_date) AS d FROM " + DB + ".ml_repo_predictions")
            if latest.empty or pd.isna(latest["d"].iloc[0]):
                empty_state("Прогнозов пока нет.")
            else:
                # max(snapshot_date) приходит как Timestamp -> '2026-09-29 00:00:00',
                # а колонка Date такой литерал не принимает: приводим к date.
                snap = pd.to_datetime(latest["d"].iloc[0]).date()
                st.subheader(f"Прогноз на снимок {snap}")

                stat = qdf(f"""
                    SELECT count() AS total,
                           countIf(prediction = 1) AS flagged,
                           countIf(prediction = 0) AS healthy,
                           countIf(label IS NOT NULL) AS known_labels
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}'
                """)
                if not stat.empty:
                    s = stat.iloc[0]
                    share_flag = s["flagged"] / s["total"] if s["total"] else 0
                    c = st.columns(4)
                    c[0].metric("Репозиториев всего", fmt(s["total"]))
                    c[1].metric("🪦 Прогноз «заброшен»", fmt(s["flagged"]), f"{share_flag:.0%} всех")
                    c[2].metric("✅ Прогноз «активен»", fmt(s["healthy"]))
                    c[3].metric("Факт уже проверен?", "нет" if not s["known_labels"] else fmt(s["known_labels"]),
                                "окно 7 дней ещё не прошло" if not s["known_labels"] else "факт известен")


                # Главный график: вся популяция репозиториев по вероятности,
                # раскрашена по классу прогноза; в заголовке — порог модели.
                thr_df = qdf(f"""
                    SELECT min(probability) AS thr
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}' AND prediction = 1
                """)
                threshold = float(thr_df["thr"].iloc[0]) if not thr_df.empty else 0.5

                dist = qdf(f"""
                    SELECT round(probability, 1) AS bucket, count() AS repos
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}'
                    GROUP BY bucket ORDER BY bucket
                """)
                if not dist.empty:
                    dist["Класс"] = (dist["bucket"] >= threshold).map(
                        {True: "🪦 Заброшен", False: "✅ Активен"})
                    fig = px.bar(
                        dist, x="bucket", y="repos", color="Класс",
                        color_discrete_map={"🪦 Заброшен": "#e45756", "✅ Активен": "#42d392"},
                        title=f"Все репозитории по вероятности (порог ≈ {threshold:.0%})",
                        labels={"bucket": "Вероятность заброшенности (шаг 0.1)", "repos": "Репозиториев"},
                    )
                    fig.update_layout(legend=dict(orientation="h", y=-0.25))
                    fig.update_xaxes(tickformat=".0%")
                    st.plotly_chart(style_fig(fig, 420), width="stretch", theme=None)

                st.caption("ℹ️ Язык репозитория в прогнозах почти всегда 'Unknown' — "
                           "языки в ETL заполняются только для форков, поэтому "
                           "разбивку по языкам здесь не показываем.")

                # Таблица вердиктов со случайной выборкой + фильтр
                verdict = qdf(f"""
                    SELECT repo_name, language, probability, prediction, label
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}'
                    ORDER BY cityHash64(toString(repo_id)) * probability
                    LIMIT 500
                """)
                if not verdict.empty:
                    verdict["prediction"] = verdict["prediction"].astype(int)
                    verdict["Прогноз"] = verdict["prediction"].map(
                        {1: "🪦 Заброшен", 0: "✅ Активен"})
                    verdict["Факт (7 дней)"] = verdict["label"].map(
                        lambda v: "⏳ пока неизвестен" if pd.isna(v)
                        else ("🪦 заброшен" if v == 1 else "✅ активен"))

                    st.divider()
                    st.subheader("Все прогнозы")
                    st.caption("Смешанная выборка из 500 репозиториев снимка — здесь видны и рискованные, "
                               "и живые. «Факт» появится, когда пройдёт неделя после снимка.")
                    flt = st.segmented_control("Показать", ["Все", "🪦 Заброшен", "✅ Активен"],
                                               default="Все")
                    if flt == "🪦 Заброшен":
                        verdict = verdict[verdict["prediction"] == 1]
                    elif flt == "✅ Активен":
                        verdict = verdict[verdict["prediction"] == 0]
                    show = verdict.rename(columns={"repo_name": "Репозиторий"})[
                        ["Репозиторий", "probability", "Прогноз", "Факт (7 дней)"]]
                    st.dataframe(
                        show,
                        width="stretch", hide_index=True, height=460,
                        column_config={
                            "probability": st.column_config.ProgressColumn(
                                "Вероятность заброшенности", min_value=0, max_value=1, format="percent"),
                            "Прогноз": st.column_config.TextColumn("Прогноз"),
                            "Факт (7 дней)": st.column_config.TextColumn("Факт (7 дней)"),
                        },
                    )

                    st.divider()
                    st.subheader("🔍 Проверить конкретный репозиторий")
                    repo_search = st.selectbox(
                        "Репозиторий",
                        options=verdict["repo_name"].tolist(),
                        index=None,
                        placeholder="Начните вводить owner/repo…",
                    )
                    if repo_search:
                        r = verdict[verdict["repo_name"] == repo_search].iloc[0]
                        vc1, vc2, vc3 = st.columns(3)
                        vc1.metric("Вероятность заброшенности", f"{r['probability']:.1%}")
                        vc2.metric("Прогноз модели",
                                   "🪦 Заброшен" if r["prediction"] == 1 else "✅ Активен")
                        if pd.isna(r["label"]):
                            vc3.metric("Факт", "⏳ неизвестен",
                                       "пройдёт неделя после снимка — появится")
                        else:
                            vc3.metric("Факт",
                                       "🪦 заброшен" if r["label"] == 1 else "✅ активен",
                                       "прогноз " + ("сбылся ✅" if (r["label"] == 1) == (r["prediction"] == 1) else "не сбылся ❌"))

# ============================================================ О ПРОЕКТЕ ====

if page == "ℹ️ О проекте":
    st.header("ℹ️ О проекте")
    st.markdown(f"""
**«Радар IT-трендов»** — учебный Big Data проект: анализ всех публичных событий GitHub
(коммиты, звёзды, форки, PR) из [GH Archive](https://www.gharchive.org/), чтобы понять,
какие языки и библиотеки растут, какие репозитории набирают популярность и когда
разработчики активнее всего.

**Архитектура пайплайна:**

GH Archive (.json.gz по часам) → скачивание (`scripts/download_data.py`)
→ MinIO (бакет raw) + ClickHouse `gh.events_raw` (сырое хранилище)
→ PySpark ETL (`etl/`): парсинг, чистка, витрины
→ витрины ClickHouse `gh.mart_*`
→ ML (`ml/`): прогноз заброшенности → `gh.ml_repo_predictions`, `gh.ml_model_metrics`
→ этот дашборд (Streamlit) — только чтение.

**Стек:** Docker Compose · ClickHouse · MinIO (S3) · Apache Spark · CatBoost/RF/LogReg · Streamlit + Plotly.

**Команда:** Data Engineer · Spark-разработчик · Data Scientist · BI (этот дашборд).
""")
    # Период не хардкодится: читается из ClickHouse при старте приложения,
    # поэтому после перезапуска пайплайна с другим периодом он обновится сам.
    if data_min and data_max:
        st.success(f"📅 Период данных в базе (из ClickHouse): **{data_min} — {data_max}** (UTC). "
                   f"Сейчас выбран фильтр: **{d_from} — {d_to}**.")
    else:
        st.warning("Период данных неизвестен: витрины пусты.")