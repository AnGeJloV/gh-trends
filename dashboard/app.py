"""Радар IT-трендов — дашборд по открытым логам GitHub (GH Archive).

Светлая тема в стиле референса (бирюзовый сайдбар, белые карточки).

Человек 4 (BI). Дашборд только читает готовые агрегаты из ClickHouse:
витрины Spark-ETL (gh.mart_*) и результаты ML (gh.ml_repo_predictions,
gh.ml_model_metrics). Свои агрегаты не считает и ничего не пишет в БД.
"""
import itertools
import os
import re
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import clickhouse_connect

# ------------------------------------------------------------- настройки ---

DB = os.getenv("CLICKHOUSE_DB", "gh")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

# ---- дизайн-система: один набор цветов на весь дашборд --------------------
# Светлая тема в стиле референса: бирюзово-голубой градиент, белые карточки.
C = dict(
    bg="#e8f0f6",        # фон страницы
    panel="#ffffff",     # карточки
    panel2="#f0f6fa",    # поля ввода, подложки
    border="rgba(15,43,70,.07)",
    text="#0f2b46",      # тёмно-синий (заголовки, цифры)
    muted="#6b7f95",
    teal="#14b8b0",      # главный акцент
    sky="#22b5ea",
    navy="#0e3b5e",
    blue="#3b82f6",
    cyan="#22b5ea",
    green="#2ecc8f",
    amber="#f5b942",
    red="#ef6b6b",
    pink="#f472b6",
)
# палитра для категорий (линии по языкам, типы событий)
COLORWAY = [C["teal"], C["navy"], C["sky"], C["amber"], C["green"],
            "#6366f1", C["pink"], "#fb923c", "#2dd4bf", C["red"],
            "#60a5fa", "#94a3b8"]
# непрерывные шкалы: от светло-бирюзового к тёмно-синему
SCALE_BARS = [[0, "#7fdcd5"], [0.5, "#14b8b0"], [1, "#0e5a8a"]]
SCALE_HEAT = [[0, "#eef7f9"], [0.25, "#b5e8e4"], [0.55, "#2bbfb8"],
              [0.8, "#1a8fb5"], [1, "#0e3b5e"]]


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


# ------------------------------------------------------------------- стили ---

st.set_page_config(
    page_title="Радар IT-трендов",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)


def build_css() -> str:
    css = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@600;700;800&display=swap');

/* ---------- база ---------- */
.stApp, .stApp p, .stApp label, .stApp h1, .stApp h2, .stApp h3, .stApp h4,
.stApp button, .stApp input, .stApp textarea, .stApp li, .stApp table
    { font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif; }
.stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"],
[data-testid="stAppViewContainer"] .main
    { background:
        radial-gradient(900px 420px at 92% -120px, rgba(34,181,234,.20), transparent 70%),
        radial-gradient(700px 380px at 8% -140px, rgba(20,184,176,.16), transparent 70%),
        $bg; }
.block-container { padding: 2rem 2.4rem 3rem 2.4rem; max-width: 1500px; }
[data-testid="stToolbar"], [data-testid="stDecoration"],
[data-testid="stStatusWidget"], #MainMenu { display: none !important; }
[data-testid="stHeader"] { background: transparent; }
/* служебный <style> не занимает место */
[data-testid="stElementContainer"]:has(> [data-testid="stMarkdown"] style) { display: none; }
/* сайдбар всегда открыт */
[data-testid="stSidebarCollapseButton"] { display: none !important; }
[data-testid="stExpandSidebarButton"], [data-testid="collapsedControl"]
    { display: flex !important; visibility: visible !important; }

/* ---------- заголовок страницы ---------- */
.page-head { display:flex; justify-content:space-between; align-items:flex-end;
    gap:16px; flex-wrap:wrap; margin: 0 0 1.5rem 0; }
.eyebrow { color:$teal; font-size:.74rem; font-weight:700; letter-spacing:.2em;
    text-transform:uppercase; margin-bottom:6px; }
.page-head h1 { font-family:'Montserrat','Inter',sans-serif; font-size: 1.9rem; font-weight: 800;
    letter-spacing: .01em; text-transform: uppercase;
    margin: 0; padding: 0; line-height: 1.15; color: $text; }
.page-head p { margin: .4rem 0 0 0; color: $muted; font-size: .95rem; }
.pill { display:inline-flex; align-items:center; gap:8px; padding: 9px 18px;
    border-radius: 999px; background: $panel; border: 1px solid $border;
    box-shadow: 0 6px 18px rgba(31,80,120,.08);
    color: $muted; font-size: .82rem; font-weight: 500; white-space: nowrap; }
.pill b { color: $text; font-weight: 700; }
.section-title { font-family:'Montserrat','Inter',sans-serif; font-size: 1.1rem; font-weight: 700;
    margin: 1.8rem 0 .2rem 0; color: $text; display:flex; align-items:center; gap:10px; }
.section-title::before { content:""; width:5px; height:20px; border-radius:4px;
    background: linear-gradient(180deg, $teal, $sky); }
.section-sub { color: $muted; font-size: .88rem; margin: 0 0 .9rem 15px; }

/* ---------- KPI-карточки ---------- */
.kpi-grid { display:grid; grid-template-columns: repeat(var(--cols,4), minmax(0,1fr));
    gap: 16px; margin: 0 0 16px 0; }
.kpi { position:relative; overflow:hidden; border-radius:20px; padding:18px 20px 22px 20px;
    background: $panel; border:1px solid $border; box-shadow: 0 10px 30px rgba(31,80,120,.08); }
.kpi::after { content:""; position:absolute; left:0; right:0; bottom:0; height:4px;
    background: linear-gradient(90deg, var(--c), color-mix(in srgb, var(--c) 25%, white)); }
.kpi-top { display:flex; align-items:center; gap:12px; margin-bottom: 12px; }
.kpi-ic { width:40px; height:40px; border-radius:50%; display:flex; align-items:center;
    justify-content:center; font-size:1.1rem;
    background: color-mix(in srgb, var(--c) 16%, white); }
.kpi-label { color:$muted; font-size:.74rem; font-weight:700;
    text-transform:uppercase; letter-spacing:.07em; line-height:1.25; }
.kpi-value { font-family:'Montserrat','Inter',sans-serif; font-size:1.75rem; font-weight:800;
    color:$text; white-space:nowrap; line-height:1.15; }
.kpi-sub { color:$muted; font-size:.8rem; margin-top:4px; }
@media (max-width: 1000px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0,1fr)); } }

/* ---------- карточки с графиками (st.container(key="card_*")) ---------- */
div[class*="st-key-card_"] { background:$panel; border:1px solid $border;
    border-radius:22px; padding: 20px 22px 14px 22px; box-shadow: 0 10px 30px rgba(31,80,120,.08); }
.chart-title { font-family:'Montserrat','Inter',sans-serif; font-size:1rem; font-weight:700; color:$text; }
.chart-sub { font-size:.82rem; color:$muted; margin: 3px 0 6px 0; }

/* ---------- подсказки и пустые состояния ---------- */
.callout { border-radius:14px; padding:14px 18px; border:1px solid $border;
    background: color-mix(in srgb, var(--c) 10%, white);
    border-left: 4px solid var(--c); color:$text; font-size:.92rem; line-height:1.55; }
.callout code, .empty code { background: rgba(15,43,70,.07); padding: 1px 6px;
    border-radius:6px; font-size:.85em; color:$text; }
.empty { text-align:center; padding: 42px 20px; border-radius:20px;
    border:1px dashed rgba(15,43,70,.2); background:$panel; color:$muted; }
.empty-ic { font-size:2rem; margin-bottom:8px; }

/* ---------- сайдбар: бирюзово-голубой градиент как в референсе ---------- */
section[data-testid="stSidebar"] { background: linear-gradient(170deg, #19c8b9 0%, #1ba9d6 55%, #2b82d4 100%);
    border-right: none; box-shadow: 8px 0 30px rgba(20,120,170,.20); }
[data-testid="stSidebarContent"], [data-testid="stSidebarHeader"] { background: transparent; }
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: .55rem !important; }
[data-testid="stSidebarContent"] { display:flex; flex-direction:column; min-height: calc(100vh - 70px); }
.side-flex { flex: 1 1 auto; min-height: 16px; }
.brand { display:flex; flex-direction:column; align-items:center; text-align:center; gap:6px; padding: 10px 4px 6px 4px; }
.brand-logo { width:68px; height:68px; border-radius:50%; display:flex; align-items:center;
    justify-content:center; font-size:2rem; background:#fff;
    box-shadow: 0 0 0 5px rgba(255,255,255,.28), 0 10px 24px rgba(8,60,100,.25); }
.brand-name { font-family:'Montserrat','Inter',sans-serif; font-weight:800; font-size:1.1rem; color:#fff; line-height:1.2; margin-top:8px; }
.brand-sub { font-size:.76rem; color:rgba(255,255,255,.85); }
.brand-sub a { color:#fff; text-decoration: underline; }
.side-label { font-size:.7rem; font-weight:700; letter-spacing:.16em; text-transform:uppercase;
    color:rgba(255,255,255,.85); margin: 18px 0 0 4px; }
section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
section[data-testid="stSidebar"] label p { color:#fff !important; font-weight:600; }
.conn { display:flex; align-items:center; gap:10px; padding:11px 14px; border-radius:14px;
    background:rgba(255,255,255,.16); border:1px solid rgba(255,255,255,.28);
    font-size:.76rem; color:rgba(255,255,255,.9); }
.conn code { background:none; color:#fff; padding:0; font-size:.76rem; }
.side-divider { height:1px; background:rgba(255,255,255,.30); margin: 12px 4px; }
section[data-testid="stSidebar"] hr { display:none; }
.dot { width:9px; height:9px; border-radius:50%; flex:none; background: var(--c);
    box-shadow: 0 0 0 3px rgba(255,255,255,.4); }

/* меню разделов — вертикальные «пилюли» */
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] [data-baseweb="button-group"]
    { flex-direction: column !important; align-items: stretch !important; gap: 6px !important; }
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button {
    justify-content:flex-start !important; width:100% !important; padding: 11px 16px !important;
    border-radius: 14px !important; border: 1px solid transparent !important;
    background: transparent !important; color: rgba(255,255,255,.92) !important; font-weight: 600 !important; }
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button p,
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button span { color: inherit !important; }
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button:hover
    { background: rgba(255,255,255,.16) !important; color: #fff !important; }
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button[kind="segmented_controlActive"],
section[data-testid="stSidebar"] [data-testid="stSegmentedControl"] [data-testid="stBaseButton-segmented_controlActive"] {
    background: #fff !important; border-color: #fff !important; color: $navy !important;
    box-shadow: 0 8px 18px rgba(8,60,100,.22) !important; }

/* ---------- элементы управления ---------- */
[data-baseweb="select"] > div, [data-baseweb="input"], [data-baseweb="base-input"]
    { background: $panel2 !important; border-color: $border !important; border-radius: 12px !important; }
[data-testid="stMain"] [data-testid="stSegmentedControl"] [data-baseweb="button-group"],
section.main [data-testid="stSegmentedControl"] [data-baseweb="button-group"] { gap: 6px !important; }
[data-testid="stMain"] [data-testid="stSegmentedControl"] button,
section.main [data-testid="stSegmentedControl"] button {
    border-radius: 999px !important; background: $panel !important; border: 1px solid $border !important;
    color: $muted !important; font-weight: 600 !important; box-shadow: 0 4px 12px rgba(31,80,120,.06) !important; }
[data-testid="stMain"] [data-testid="stSegmentedControl"] button p,
section.main [data-testid="stSegmentedControl"] button p { color: inherit !important; }
[data-testid="stMain"] [data-testid="stSegmentedControl"] button[kind="segmented_controlActive"],
[data-testid="stMain"] [data-testid="stSegmentedControl"] [data-testid="stBaseButton-segmented_controlActive"],
section.main [data-testid="stSegmentedControl"] [data-testid="stBaseButton-segmented_controlActive"] {
    background: linear-gradient(135deg, $teal, $sky) !important; border-color: transparent !important;
    color: #fff !important; }
[data-testid="stExpander"] details { background:$panel; border:1px solid $border;
    border-radius:18px; box-shadow: 0 10px 30px rgba(31,80,120,.06); }
[data-testid="stExpander"] summary { font-weight:600; color:$text; }
div[data-testid="stDataFrame"] { border-radius:14px; overflow:hidden; border:1px solid $border; }
hr { border-color: $border !important; }
</style>
"""
    # подставляем цвета из словаря C (длинные ключи первыми: panel2 раньше panel)
    for key in sorted(C, key=len, reverse=True):
        css = css.replace("$" + key, C[key])
    return css


# ---------------------------------------------------------- UI-компоненты ---

_card_counter = itertools.count()


def card():
    """Контейнер-«карточка» (стилизуется по ключу card_N в build_css)."""
    return st.container(key=f"card_{next(_card_counter)}")


def inline_md(text: str) -> str:
    """`код` -> <code>код</code>, чтобы работало внутри HTML-блоков."""
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", text)


def page_head(title: str, sub: str, show_period: bool = True):
    pill = ""
    if show_period and data_min and data_max:
        pill = (f'<div class="pill">📅 <b>{d_from:%d.%m.%Y}</b> — <b>{d_to:%d.%m.%Y}</b> · UTC</div>')
    st.markdown(f'<div class="page-head"><div><div class="eyebrow">Радар IT-трендов · GH Archive</div>'
                f'<h1>{title}</h1><p>{sub}</p></div>{pill}</div>',
                unsafe_allow_html=True)


def section(title: str, sub: str = ""):
    html = f'<div class="section-title">{title}</div>'
    if sub:
        html += f'<div class="section-sub">{sub}</div>'
    st.markdown(html, unsafe_allow_html=True)


def chart_head(title: str, sub: str = ""):
    html = f'<div class="chart-title">{title}</div>'
    if sub:
        html += f'<div class="chart-sub">{sub}</div>'
    st.markdown(html, unsafe_allow_html=True)


def kpi_grid(items, cols: int = 4):
    """items: (иконка, подпись, значение, пояснение, цвет)."""
    cells = ""
    for icon, label, value, sub, color in items:
        cells += (f'<div class="kpi" style="--c:{C[color]}">'
                  f'<div class="kpi-top"><div class="kpi-ic">{icon}</div>'
                  f'<div class="kpi-label">{label}</div></div>'
                  f'<div class="kpi-value">{value}</div>'
                  f'<div class="kpi-sub">{sub or "&nbsp;"}</div></div>')
    st.markdown(f'<div class="kpi-grid" style="--cols:{cols}">{cells}</div>',
                unsafe_allow_html=True)


def callout(text: str, color: str = "teal"):
    st.markdown(f'<div class="callout" style="--c:{C[color]}">{inline_md(text)}</div>',
                unsafe_allow_html=True)


def empty_state(text: str, icon: str = "🪄"):
    st.markdown(f'<div class="empty"><div class="empty-ic">{icon}</div>{inline_md(text)}</div>',
                unsafe_allow_html=True)


def style_fig(fig, height=380, legend="top", hover="x unified"):
    """Единое оформление всех графиков. legend: 'top' | 'right' | None."""
    if legend == "top":
        leg = dict(orientation="h", x=0, y=1.14, xanchor="left", yanchor="bottom",
                   font=dict(size=12, color=C["text"]), bgcolor="rgba(0,0,0,0)")
    elif legend == "right":
        leg = dict(orientation="v", x=1.02, y=.5, yanchor="middle",
                   font=dict(size=12, color=C["text"]), bgcolor="rgba(0,0,0,0)")
    else:
        leg = {}
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=4, r=4, t=34 if legend == "top" else 8, b=4),
        colorway=COLORWAY,
        showlegend=legend is not None,
        legend=leg,
        hovermode=hover,
        hoverlabel=dict(bgcolor="#ffffff", bordercolor=C["teal"],
                        font=dict(color=C["text"], size=13)),
        font=dict(color=C["muted"], size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="rgba(15,43,70,.07)", zeroline=False,
                     linecolor="rgba(15,43,70,.12)", tickfont=dict(color=C["muted"]))
    fig.update_yaxes(gridcolor="rgba(15,43,70,.07)", zeroline=False,
                     linecolor="rgba(0,0,0,0)", tickfont=dict(color=C["muted"]))
    return fig


def show(fig):
    st.plotly_chart(fig, width="stretch", theme=None, config={"displayModeBar": False})


def fmt(n) -> str:
    return f"{int(n):,}".replace(",", " ")


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

P_OVERVIEW, P_LANGS, P_REPOS, P_ML = (
    "📊 Обзор", "💬 Языки", "📦 Репозитории", "🤖 ML-прогнозы")
NAV = [P_OVERVIEW, P_LANGS, P_REPOS, P_ML]

with st.sidebar:
    st.markdown(build_css(), unsafe_allow_html=True)
    st.markdown(
        '<div class="brand"><div class="brand-logo">🛰️</div>'
        '<div class="brand-name">Радар IT-трендов</div>'
        '<div class="brand-sub">Пульс open-source · '
        '<a href="https://www.gharchive.org/" target="_blank">GH Archive</a></div>'
        '</div>', unsafe_allow_html=True)

    st.markdown('<div class="side-label">Меню</div>', unsafe_allow_html=True)
    page = st.segmented_control("Раздел", NAV,
                                default=st.session_state.get("nav_page", NAV[0]),
                                key="seg_nav", label_visibility="collapsed")
    page = page or st.session_state.get("nav_page", NAV[0])
    st.session_state.nav_page = page

    # разделитель, как в референсе: основная группа меню / нижняя группа
    st.markdown('<div class="side-divider"></div>', unsafe_allow_html=True)

    st.markdown('<div class="side-label">Фильтр</div>', unsafe_allow_html=True)
    d_from, d_to = date.today() - timedelta(days=30), date.today()
    if data_min and data_max:
        period = st.date_input("Период данных", value=(data_min, data_max),
                               min_value=data_min, max_value=data_max,
                               format="DD.MM.YYYY")
        d_from, d_to = (period if isinstance(period, tuple) and len(period) == 2
                        else (data_min, data_max))
    else:
        callout("Витрины пока пусты — поднимите пайплайн (`docker compose up -d`).", "amber")

    # распорка: прижимает низ сайдбара к низу экрана
    st.markdown('<div class="side-flex"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="conn" style="--c:{C["green"] if HAVE_MARTS else C["amber"]}">'
        f'<span class="dot"></span><div>ClickHouse<br><code>{CH_HOST}:{CH_PORT}</code> · БД <code>{DB}</code></div></div>',
        unsafe_allow_html=True)


def dt_cond(col: str = "hour") -> str:
    return (f"{col} >= toDateTime('{d_from} 00:00:00') "
            f"AND {col} < toDateTime('{d_to + timedelta(days=1)} 00:00:00')")


# =============================================================== ОБЗОР =====

if page == P_OVERVIEW:
    page_head("Обзор активности", "Что происходило на GitHub за выбранный период")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет. Запустите пайплайн: `docker compose up -d`.", "📭")
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
            empty_state("За выбранный период событий нет — расширьте диапазон дат в сайдбаре.", "🔍")
        else:
            row = kpi.iloc[0]
            days = max((d_to - d_from).days + 1, 1)
            peak = qdf(f"SELECT max(event_count) AS peak FROM {DB}.mart_hourly_activity WHERE {dt_cond()}")

            kpi_grid([
                ("⚡", "Событий всего", fmt(row["events"]),
                 f"{int(row['events']) / days:,.0f} в день".replace(",", " "), "teal"),
                ("👩‍💻", "Активных авторов", fmt(row["actors"]), "уникальные пользователи", "cyan"),
                ("📦", "Репозиториев", fmt(row["repos"]), "с активностью в периоде", "blue"),
                ("⬆️", "Пушей", fmt(row["pushes"]), "PushEvent", "green"),
            ])
            kpi_grid([
                ("⭐", "Звёзд", fmt(row["stars"]), "WatchEvent", "amber"),
                ("🍴", "Форков", fmt(row["forks"]), "ForkEvent", "pink"),
                ("🔀", "Pull Request'ов", fmt(row["prs"]), "PullRequestEvent", "teal"),
                ("🔥", "Пик за час", fmt(peak["peak"].iloc[0]) if not peak.empty else "—",
                 "макс. событий в один час", "red"),
            ])

            left, right = st.columns([3, 2])

            with left, card():
                chart_head("Активность по дням", "Сколько событий GitHub приходится на каждый день")
                daily = qdf(f"""
                    SELECT toDate(hour) AS day, sum(event_count) AS events
                    FROM {DB}.mart_hourly_activity
                    WHERE {dt_cond()}
                    GROUP BY day ORDER BY day
                """)
                if daily.empty:
                    empty_state("Нет данных по дням.", "📉")
                else:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=daily["day"], y=daily["events"], fill="tozeroy",
                        mode="lines", line=dict(color=C["teal"], width=3, shape="spline"),
                        fillcolor="rgba(20,184,176,.18)", name="События",
                        hovertemplate="%{y:,} событий<extra></extra>",
                    ))
                    show(style_fig(fig, 360, legend=None))

            with right, card():
                chart_head("Структура по типам событий", "Доля каждого типа в общем потоке")
                mix = qdf(f"""
                    SELECT event_type, sum(event_count) AS events
                    FROM {DB}.mart_hourly_activity
                    WHERE {dt_cond()}
                    GROUP BY event_type ORDER BY events DESC
                """)
                if mix.empty:
                    empty_state("Нет данных по типам событий.", "🥧")
                else:
                    fig = go.Figure(go.Pie(
                        labels=mix["event_type"], values=mix["events"], hole=.66,
                        sort=False, textposition="inside", textinfo="percent",
                        marker=dict(colors=COLORWAY, line=dict(color=C["panel"], width=2)),
                        hovertemplate="%{label}<br>%{value:,} событий (%{percent})<extra></extra>",
                    ))
                    fig.add_annotation(
                        text=f"<b>{fmt(mix['events'].sum())}</b>", x=0.5, y=0.54,
                        xref="paper", yref="paper", showarrow=False,
                        font=dict(size=24, color=C["text"]))
                    fig.add_annotation(
                        text="событий", x=0.5, y=0.44, xref="paper", yref="paper",
                        showarrow=False, font=dict(size=12, color=C["muted"]))
                    show(style_fig(fig, 360, legend="right", hover="closest"))

            # Тепловая карта: день недели × час суток (UTC)
            with card():
                chart_head("Когда мир коммитит",
                           "Активность по дням недели и часам суток (UTC): чем ярче, тем больше событий")
                heat = qdf(f"""
                    SELECT toDayOfWeek(hour) AS dow, toHour(hour) AS h, sum(event_count) AS events
                    FROM {DB}.mart_hourly_activity
                    WHERE {dt_cond()}
                    GROUP BY dow, h
                """)
                if heat.empty:
                    empty_state("Нет данных для карты активности.", "🗺️")
                else:
                    pivot = heat.pivot(index="dow", columns="h", values="events").fillna(0)
                    pivot = pivot.reindex(index=range(1, 8), columns=range(24), fill_value=0)
                    day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
                    fig = go.Figure(go.Heatmap(
                        z=pivot.values, x=[f"{h:02d}" for h in range(24)], y=day_names,
                        colorscale=SCALE_HEAT, xgap=3, ygap=3,
                        colorbar=dict(thickness=10, len=.9, outlinewidth=0,
                                      tickfont=dict(color=C["muted"])),
                        hovertemplate="%{y}, %{x}:00 UTC<br>%{z:,} событий<extra></extra>",
                    ))
                    fig.update_yaxes(autorange="reversed", showgrid=False)
                    fig.update_xaxes(showgrid=False, title="Час суток (UTC)")
                    show(style_fig(fig, 320, legend=None, hover="closest"))

# =============================================================== ЯЗЫКИ =====

if page == P_LANGS:
    page_head("Языки программирования", "Какие языки сейчас на пике активности")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет — запустите пайплайн.", "📭")
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
            empty_state("Данных по языкам нет за этот период.", "💬")
        else:
            sel = st.multiselect(
                "Языки для сравнения в трендах",
                options=top_langs["language"].tolist(),
                default=top_langs["language"].head(8).tolist(),
            )

            left, right = st.columns([2, 3])

            with left, card():
                chart_head("Топ-15 языков", "По числу событий (форков) за период")
                d = top_langs.head(15).sort_values("events")
                fig = go.Figure(go.Bar(
                    x=d["events"], y=d["language"], orientation="h",
                    marker=dict(color=d["events"], colorscale=SCALE_BARS, line=dict(width=0)),
                    text=[fmt(v) for v in d["events"]], textposition="outside",
                    textfont=dict(color=C["muted"], size=11), cliponaxis=False,
                    hovertemplate="%{y}: %{x:,}<extra></extra>",
                ))
                fig.update_xaxes(range=[0, float(d["events"].max()) * 1.2], showticklabels=False,
                                 showgrid=False)
                fig.update_yaxes(title="", showgrid=False, tickfont=dict(color=C["text"]))
                show(style_fig(fig, 520, legend=None, hover="closest"))

            with right, card():
                chart_head("Тренд активности по дням", "Форки по выбранным языкам")
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
                    if trend.empty:
                        empty_state("Нет данных по выбранным языкам.", "📉")
                    else:
                        fig = go.Figure()
                        for lang in sel:
                            t = trend[trend["language"] == lang]
                            if t.empty:
                                continue
                            fig.add_trace(go.Scatter(
                                x=t["day"], y=t["events"], name=lang, mode="lines",
                                line=dict(width=2.5, shape="spline"),
                                hovertemplate="%{y:,}<extra>" + lang + "</extra>"))
                        show(style_fig(fig, 520))
                else:
                    empty_state("Выберите языки, чтобы построить тренды.", "👆")

            with card():
                chart_head("Сводная таблица по языкам", "Топ-15: форки и число уникальных авторов")
                tbl = lang_stats[["language", "events", "actors"]].head(15)
                tbl.columns = ["Язык", "Форки", "Авторы"]
                st.dataframe(
                    tbl, width="stretch", hide_index=True, height=420,
                    column_config={
                        "Форки": st.column_config.ProgressColumn(
                            "Форки", format="%d", min_value=0, max_value=int(tbl["Форки"].max())),
                        "Авторы": st.column_config.NumberColumn(format="%d"),
                    },
                )

# ========================================================== РЕПОЗИТОРИИ ====

if page == P_REPOS:
    page_head("Репозитории", "Кто набирает популярность")

    if not HAVE_MARTS:
        empty_state("Витрин ещё нет — запустите пайплайн.", "📭")
    else:
        metric = st.segmented_control(
            "Метрика роста",
            ["⭐ Звёзды", "🍴 Форки", "⬆️ Пуши", "Все события"],
            default="⭐ Звёзды",
        )
        metric = metric or "⭐ Звёзды"
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
            empty_state("Нет данных по репозиториям за период.", "📦")
        else:
            per_day_col = f"{metric_col}/день"
            top_repos[per_day_col] = (top_repos[order_col] / days).round(1)
            pretty = top_repos.rename(columns={
                "repo_name": "Репозиторий", "events": "События", "stars": "⭐",
                "forks": "🍴", "pushes": "Пуши", "per_day": per_day_col,
            })

            with card():
                chart_head(f"Топ-15 репозиториев: {metric.lower()}", "За выбранный период")
                d = pretty.head(15)
                fig = go.Figure(go.Bar(
                    x=d[metric_col], y=d["Репозиторий"], orientation="h",
                    marker=dict(color=d[metric_col], colorscale=SCALE_BARS, line=dict(width=0)),
                    text=[fmt(v) for v in d[metric_col]], textposition="outside",
                    textfont=dict(color=C["muted"], size=11), cliponaxis=False,
                    hovertemplate="%{y}: %{x:,}<extra></extra>",
                ))
                fig.update_xaxes(range=[0, float(d[metric_col].max()) * 1.15], showticklabels=False,
                                 showgrid=False, title=None)
                fig.update_yaxes(autorange="reversed", title="", showgrid=False,
                                 tickfont=dict(color=C["text"]))
                show(style_fig(fig, 560, legend=None, hover="closest"))

            with card():
                chart_head("Топ-20 в числах", "Все метрики роста рядом")
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

            section("📈 Детально по одному репозиторию", "Динамика по дням: события, звёзды, форки, пуши")
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
                with card():
                    chart_head(f"Динамика: {repo_choice}")
                    if detail.empty:
                        empty_state("По этому репозиторию нет данных за период.", "📉")
                    else:
                        fig = go.Figure()
                        traces = [("events", "События", C["teal"]), ("stars", "⭐ Звёзды", C["amber"]),
                                  ("forks", "🍴 Форки", C["green"]), ("pushes", "Пуши", C["cyan"])]
                        for col_, name, color in traces:
                            fig.add_trace(go.Scatter(
                                x=detail["day"], y=detail[col_], name=name,
                                mode="lines+markers", line=dict(color=color, width=2.5, shape="spline"),
                                marker=dict(size=5),
                            ))
                        show(style_fig(fig, 380))

# ========================================================= ML-ПРОГНОЗЫ =====

if page == P_ML:
    page_head("Прогноз заброшенности", "ML-модель предсказывает, какие репозитории скоро «затихнут»",
              show_period=False)

    if not (HAVE_PRED or HAVE_METRICS):
        empty_state("ML-таблиц ещё нет: ml-trainer не запускался или данных меньше ~3 недель. "
                    "Запустите `docker compose --profile pipeline up -d`.", "🤖")
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
                    kpi_grid([
                        ("🏆", "Лучшая модель", str(best["model"]), "по PR-AUC", "teal"),
                        ("🎯", "PR-AUC", f"{best['pr_auc']:.3f}", "умение находить рискованные", "cyan"),
                        ("📐", "ROC-AUC",
                         f"{best['roc_auc']:.3f}" if pd.notna(best["roc_auc"]) else "—",
                         "общее качество разделения", "blue"),
                        ("✅", "Точность топ-списка",
                         f"{best['precision_at_k']:.1%}" if pd.notna(best["precision_at_k"]) else "—",
                         f"в списке K={int(best['k'])}", "green"),
                    ])
                else:
                    empty_state("Метрик пока нет.", "📈")

        if HAVE_PRED:
            latest = qdf("SELECT max(snapshot_date) AS d FROM " + DB + ".ml_repo_predictions")
            if latest.empty or pd.isna(latest["d"].iloc[0]):
                empty_state("Прогнозов пока нет.", "🔮")
            else:
                # max(snapshot_date) приходит как Timestamp -> '2026-09-29 00:00:00',
                # а колонка Date такой литерал не принимает: приводим к date.
                snap = pd.to_datetime(latest["d"].iloc[0]).date()
                section(f"Прогноз на снимок {snap:%d.%m.%Y}", "Свежий срез репозиториев и вердикт модели")

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
                    kpi_grid([
                        ("📦", "Репозиториев всего", fmt(s["total"]), "в снимке", "teal"),
                        ("🪦", "Прогноз «заброшен»", fmt(s["flagged"]), f"{share_flag:.0%} всех", "red"),
                        ("✅", "Прогноз «активен»", fmt(s["healthy"]), f"{1 - share_flag:.0%} всех", "green"),
                        ("🔎", "Факт уже проверен?",
                         "нет" if not s["known_labels"] else fmt(s["known_labels"]),
                         "окно 7 дней ещё не прошло" if not s["known_labels"] else "факт известен",
                         "amber"),
                    ])

                # Главный график: вся популяция репозиториев по вероятности,
                # раскрашена по классу прогноза; порог модели — пунктиром.
                thr_df = qdf(f"""
                    SELECT min(probability) AS thr
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}' AND prediction = 1
                """)
                threshold = (float(thr_df["thr"].iloc[0])
                             if not thr_df.empty and pd.notna(thr_df["thr"].iloc[0]) else 0.5)

                dist = qdf(f"""
                    SELECT round(probability, 1) AS bucket, count() AS repos
                    FROM {DB}.ml_repo_predictions FINAL
                    WHERE snapshot_date = '{snap}'
                    GROUP BY bucket ORDER BY bucket
                """)
                if not dist.empty:
                    gcol, dcol = st.columns([2, 3])
                    with gcol, card():
                        chart_head("Доля «заброшенных»", "Прогноз модели по всему снимку")
                        if stat.empty or not s["total"]:
                            empty_state("Нет данных для расчёта доли.", "🎯")
                        else:
                            fig = go.Figure(go.Pie(
                                values=[s["flagged"], s["healthy"]],
                                labels=["🪦 Заброшен", "✅ Активен"],
                                hole=.78, sort=False, direction="clockwise", rotation=0,
                                textinfo="none",
                                marker=dict(colors=[C["red"], "#dcebf3"],
                                            line=dict(color=C["panel"], width=3)),
                                hovertemplate="%{label}<br>%{value:,} репозиториев (%{percent})<extra></extra>",
                            ))
                            fig.add_annotation(
                                text=f"<b>{share_flag:.0%}</b>", x=0.5, y=0.54, xref="paper", yref="paper",
                                showarrow=False, font=dict(size=40, color=C["text"]))
                            fig.add_annotation(
                                text="прогноз «заброшен»", x=0.5, y=0.4, xref="paper", yref="paper",
                                showarrow=False, font=dict(size=12, color=C["muted"]))
                            show(style_fig(fig, 400, legend=None, hover="closest"))
                    with dcol, card():
                        chart_head("Все репозитории по вероятности заброшенности",
                                   f"Порог модели ≈ {threshold:.0%}: справа от него — «заброшен»")
                        is_flag = dist["bucket"] >= threshold
                        fig = go.Figure()
                        for mask, name, color in ((~is_flag, "✅ Активен", C["green"]),
                                                  (is_flag, "🪦 Заброшен", C["red"])):
                            part = dist[mask]
                            fig.add_trace(go.Bar(
                                x=part["bucket"], y=part["repos"], name=name,
                                marker=dict(color=color, line=dict(width=0)),
                                hovertemplate="%{x:.0%}: %{y:,} репозиториев<extra></extra>"))
                        fig.add_vline(x=threshold, line_dash="dash", line_color=C["amber"], line_width=1.5)
                        fig.update_layout(barmode="stack", bargap=.15)
                        fig.update_xaxes(tickformat=".0%", title="Вероятность заброшенности (шаг 0.1)",
                                         showgrid=False)
                        fig.update_yaxes(title="Репозиториев")
                        show(style_fig(fig, 400, hover="closest"))

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

                    section("Все прогнозы",
                            "Смешанная выборка из 500 репозиториев снимка — здесь видны и рискованные, "
                            "и живые. «Факт» появится, когда пройдёт неделя после снимка.")
                    flt = st.segmented_control("Показать", ["Все", "🪦 Заброшен", "✅ Активен"],
                                               default="Все")
                    if flt == "🪦 Заброшен":
                        verdict = verdict[verdict["prediction"] == 1]
                    elif flt == "✅ Активен":
                        verdict = verdict[verdict["prediction"] == 0]
                    show_tbl = verdict.rename(columns={"repo_name": "Репозиторий"})[
                        ["Репозиторий", "probability", "Прогноз", "Факт (7 дней)"]]
                    with card():
                        st.dataframe(
                            show_tbl,
                            width="stretch", hide_index=True, height=460,
                            column_config={
                                "probability": st.column_config.ProgressColumn(
                                    "Вероятность заброшенности", min_value=0, max_value=1, format="percent"),
                                "Прогноз": st.column_config.TextColumn("Прогноз"),
                                "Факт (7 дней)": st.column_config.TextColumn("Факт (7 дней)"),
                            },
                        )

                    section("🔍 Проверить конкретный репозиторий",
                            "Выберите репозиторий из таблицы выше, чтобы увидеть вердикт крупно")
                    repo_search = st.selectbox(
                        "Репозиторий",
                        options=verdict["repo_name"].tolist(),
                        index=None,
                        placeholder="Начните вводить owner/repo…",
                    )
                    if repo_search:
                        r = verdict[verdict["repo_name"] == repo_search].iloc[0]
                        flagged = r["prediction"] == 1
                        if pd.isna(r["label"]):
                            fact_val, fact_sub, fact_col = "⏳ неизвестен", "пройдёт неделя после снимка — появится", "amber"
                        else:
                            fact_val = "🪦 заброшен" if r["label"] == 1 else "✅ активен"
                            ok = (r["label"] == 1) == flagged
                            fact_sub = "прогноз " + ("сбылся ✅" if ok else "не сбылся ❌")
                            fact_col = "green" if ok else "red"
                        kpi_grid([
                            ("🎲", "Вероятность заброшенности", f"{r['probability']:.1%}",
                             "уверенность модели", "teal"),
                            ("🔮", "Прогноз модели", "🪦 Заброшен" if flagged else "✅ Активен",
                             "на ближайшие 7 дней", "red" if flagged else "green"),
                            ("🧾", "Факт", fact_val, fact_sub, fact_col),
                        ], cols=3)
