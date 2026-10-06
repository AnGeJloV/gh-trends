"""Извлечение признаков и меток из ClickHouse для задачи
«прогноз заброшенности репозитория» (Человек 3).

Схема работы:
1. По истории событий определяем доступный диапазон дат.
2. Раскладываем «снимки» (snapshots) с шагом SNAPSHOT_STRIDE_DAYS:
   для каждого снимка t1 окно наблюдения W1 = [t1 - OBS, t1),
   окно исхода W2 = [t1, t1 + FUTURE).
3. Тяжёлую агрегацию делаем SQL-ом в ClickHouse (uniqExact, countIf),
   в Python приходит только компактная таблица «1 строка = 1 репозиторий».

Метки: abandoned = 1, если в W2 не было ни одного события.
Снимки, у которых окно W2 выходит за пределы данных, помечаются
label_available=False и используются только для прогноза (не для обучения).
"""
from datetime import datetime, timedelta

import clickhouse_connect
import pandas as pd

import config


def get_client(database: str | None = None) -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=database or config.CLICKHOUSE_DB,
    )


def get_data_bounds(client) -> tuple[datetime, datetime]:
    """Первое и последнее событие в таблице-источнике."""
    row = client.query(
        f"SELECT min(created_at), max(created_at) FROM {config.CLICKHOUSE_DB}.{config.SOURCE_TABLE}"
    ).result_rows[0]
    if row[0] is None:
        raise RuntimeError(
            f"Таблица {config.CLICKHOUSE_DB}.{config.SOURCE_TABLE} пуста. "
            "Сначала запустите ETL (Человек 2): docker compose up --build spark-etl"
        )
    return row[0], row[1]


def _window_sql(t1: datetime, start: datetime, min_events: int) -> str:
    """Агрегат по репозиториям за окно наблюдения."""
    db = config.CLICKHOUSE_DB
    return f"""
    SELECT
        repo_id,
        argMax(repo_name, created_at) AS repo_name,
        argMax(language, created_at) AS language,
        count() AS events_total,
        countIf(event_type = 'PushEvent') AS pushes,
        sumIf(commits_count, event_type = 'PushEvent') AS commits_total,
        countIf(event_type = 'WatchEvent') AS stars,
        countIf(event_type = 'ForkEvent') AS forks,
        countIf(event_type = 'PullRequestEvent') AS prs,
        uniqExact(actor_id) AS actors,
        uniqExact(toDate(created_at)) AS active_days,
        dateDiff('hour', min(created_at), max(created_at)) + 1 AS activity_span_hours,
        dateDiff('hour', max(created_at), toDateTime('{t1:%Y-%m-%d %H:%M:%S}')) AS hours_since_last,
        countIf(created_at < toDateTime('{start + (t1 - start) / 2:%Y-%m-%d %H:%M:%S}')) AS events_first_half,
        countIf(created_at >= toDateTime('{start + (t1 - start) / 2:%Y-%m-%d %H:%M:%S}')) AS events_second_half
    FROM {db}.{config.SOURCE_TABLE}
    WHERE created_at >= toDateTime('{start:%Y-%m-%d %H:%M:%S}')
      AND created_at < toDateTime('{t1:%Y-%m-%d %H:%M:%S}')
    GROUP BY repo_id
    HAVING events_total >= {min_events}
    """


def _top_actor_sql(t1: datetime, start: datetime) -> str:
    """Доля самого активного автора и число активных авторов за окно."""
    db = config.CLICKHOUSE_DB
    return f"""
    SELECT
        repo_id,
        max(n) AS top_actor_events,
        sum(n) AS actor_events_total
    FROM (
        SELECT repo_id, actor_id, count() AS n
        FROM {db}.{config.SOURCE_TABLE}
        WHERE created_at >= toDateTime('{start:%Y-%m-%d %H:%M:%S}')
          AND created_at < toDateTime('{t1:%Y-%m-%d %H:%M:%S}')
        GROUP BY repo_id, actor_id
    )
    GROUP BY repo_id
    """


def _label_sql(t1: datetime, t2: datetime) -> str:
    db = config.CLICKHOUSE_DB
    return f"""
    SELECT repo_id, count() AS future_events
    FROM {db}.{config.SOURCE_TABLE}
    WHERE created_at >= toDateTime('{t1:%Y-%m-%d %H:%M:%S}')
      AND created_at < toDateTime('{t2:%Y-%m-%d %H:%M:%S}')
    GROUP BY repo_id
    """


def build_dataset(
    client,
    obs_days: int = config.OBS_WINDOW_DAYS,
    future_days: int = config.FUTURE_WINDOW_DAYS,
    stride_days: int = config.SNAPSHOT_STRIDE_DAYS,
    min_events: int = config.MIN_EVENTS_IN_OBS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Собирает датасет «снимок x репозиторий».

    Возвращает (train_df, predict_df):
    - train_df: снимки с полностью известным окном исхода (есть метка);
    - predict_df: последний снимок, для которого окно исхода ещё не наступило
      (используется для выгрузки прогнозов в ClickHouse для дашборда).
    """
    first_event, last_event = get_data_bounds(client)

    # Снимки: конец окна наблюдения t1. Обучающие — у которых помещается и окно исхода.
    train_ends, predict_ends = [], []
    t1 = (first_event + timedelta(days=obs_days)).replace(hour=0, minute=0, second=0)
    while t1 <= last_event:
        if t1 + timedelta(days=future_days) <= last_event:
            train_ends.append(t1)
        else:
            predict_ends.append(t1)
        t1 += timedelta(days=stride_days)

    frames = []
    for t1 in train_ends + predict_ends:
        start = t1 - timedelta(days=obs_days)
        feats = client.query_df(_window_sql(t1, start, min_events))
        if feats.empty:
            continue
        top_actor = client.query_df(_top_actor_sql(t1, start))
        feats = feats.merge(top_actor, on="repo_id", how="left")

        has_label = t1 in train_ends
        if has_label:
            labels = client.query_df(_label_sql(t1, t1 + timedelta(days=future_days)))
            feats = feats.merge(labels, on="repo_id", how="left")
            feats["future_events"] = feats["future_events"].fillna(0)
        else:
            feats["future_events"] = pd.NA

        feats["snapshot_date"] = t1.date()
        feats["label_available"] = has_label
        frames.append(feats)

    if not frames:
        raise RuntimeError(
            "Не удалось собрать ни одного снимка: данных слишком мало для окон "
            f"W1={obs_days}д / W2={future_days}д. Уменьшите окна (ML_OBS_WINDOW_DAYS / "
            "ML_FUTURE_WINDOW_DAYS) или догрузите данные."
        )

    df = pd.concat(frames, ignore_index=True)

    # --- производные признаки ---
    eps = 1e-9
    df["top_actor_share"] = df["top_actor_events"] / (df["events_total"] + eps)
    df["events_per_active_day"] = df["events_total"] / (df["active_days"] + eps)
    df["commits_per_push"] = df["commits_total"] / (df["pushes"] + eps)
    df["trend_ratio"] = (df["events_second_half"] + 1) / (df["events_first_half"] + 1)
    df["events_per_actor"] = df["events_total"] / (df["actors"] + eps)

    # метка
    df["abandoned"] = 0
    labeled = df["label_available"]
    df.loc[labeled, "abandoned"] = (df.loc[labeled, "future_events"] == 0).astype(int)

    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    train_df = df[df["label_available"]].copy()
    predict_df = df[~df["label_available"]].copy()
    return train_df, predict_df


# Числовые признаки, идущие в модель (плюс категориальный language)
FEATURE_COLUMNS = [
    "events_total", "pushes", "commits_total", "stars", "forks", "prs",
    "actors", "active_days", "activity_span_hours", "hours_since_last",
    "events_first_half", "events_second_half",
    "top_actor_share", "events_per_active_day", "commits_per_push",
    "trend_ratio", "events_per_actor",
]
CATEGORICAL_COLUMNS = ["language"]