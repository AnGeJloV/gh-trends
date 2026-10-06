"""Самотест ML-пайплайна на синтетических данных (без интернета).

Генерирует правдоподобный поток событий GitHub Archive (Push/Watch/Fork/PR)
с тремя «режимами» репозиториев — здоровые, затухающие и всплески — кладёт
их во временную БД gh_ml_smoke с той же схемой, что и gh.events_parsed,
и запускает полный конвейер признаков и обучения. По завершении БД удаляется,
на реальных таблицах ничего не остаётся.

Запуск: python ml/train.py --smoke
Ожидание: ROC-AUC > 0.7 у улучшенных моделей (сигнал затухания синтетический).
"""
import contextlib
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import config

SMOKE_DB = "gh_ml_smoke"
SMOKE_DAYS = 90
N_REPOS = 300

LANGUAGES = ["Python", "JavaScript", "TypeScript", "Go", "Rust", "Java",
             "C++", "C#", "Ruby", "Unknown"]

CREATE_TABLE_SQL = """
CREATE TABLE {db}.events_parsed (
    event_id String,
    event_type LowCardinality(String),
    actor_id Int64,
    actor_login String,
    repo_id Int64,
    repo_name String,
    language LowCardinality(String),
    commits_count Int32,
    action Nullable(String),
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY (event_type, repo_id, created_at, event_id)
"""


def generate_events(seed: int = 42) -> pd.DataFrame:
    """Синтетический поток событий по N_REPOS репозиториям за SMOKE_DAYS дней."""
    rng = np.random.default_rng(seed)
    start = datetime(2026, 7, 1).replace(hour=0)
    rows = []

    for repo_id in range(1, N_REPOS + 1):
        repo_name = f"synthetic/repo-{repo_id:04d}"
        language = rng.choice(LANGUAGES, p=[0.2, 0.15, 0.12, 0.1, 0.08, 0.08, 0.07, 0.07, 0.05, 0.08])
        base_rate = rng.uniform(1.0, 25.0)          # событий в день
        n_actors = int(rng.integers(1, 40))
        actor_ids = rng.integers(1, 100_000, size=n_actors)
        # "вес" каждого актора: обычно есть мейнтейнер с большинством событий
        actor_weights = rng.pareto(1.2, size=n_actors) + 0.1
        actor_weights /= actor_weights.sum()

        mode = rng.choice(["healthy", "dying", "spiky"], p=[0.55, 0.35, 0.10])
        decay_start = int(rng.integers(10, SMOKE_DAYS - 25)) if mode == "dying" else None
        spike_day = int(rng.integers(5, SMOKE_DAYS - 5)) if mode == "spiky" else None

        for day in range(SMOKE_DAYS):
            rate = base_rate
            if mode == "dying":
                # экспоненциальное затухание после decay_start
                if day >= decay_start:
                    rate = base_rate * np.exp(-(day - decay_start) / 2.0)
            elif mode == "spiky":
                rate = base_rate * (6.0 if day == spike_day else 0.7)
            else:
                rate = base_rate * (1.3 if day % 7 in (0, 6) else 1.0)

            n_events = rng.poisson(rate)
            for _ in range(n_events):
                etype = rng.choice(["PushEvent", "WatchEvent", "PullRequestEvent", "ForkEvent"],
                                   p=[0.5, 0.32, 0.13, 0.05])
                actor_idx = rng.choice(n_actors, p=actor_weights)
                actor_id = int(actor_ids[actor_idx])
                commits = int(rng.integers(1, 6)) if etype == "PushEvent" else 0
                rows.append((
                    uuid.uuid4().hex, etype, actor_id, f"user_{actor_id}",
                    repo_id, repo_name, language, commits,
                    "started" if etype == "WatchEvent" else None,
                    start + timedelta(days=day,
                                      hours=int(rng.integers(0, 24)),
                                      minutes=int(rng.integers(0, 60))),
                ))

    df = pd.DataFrame(rows, columns=[
        "event_id", "event_type", "actor_id", "actor_login", "repo_id",
        "repo_name", "language", "commits_count", "action", "created_at",
    ])
    return df


@contextlib.contextmanager
def smoke_environment(seed: int = 42):
    """Создаёт БД gh_ml_smoke с данными, отдаёт клиент на неё, удаляет БД."""
    import features as F

    admin = F.get_client()  # клиент без привязки к БД — для DDL
    admin.command(f"DROP DATABASE IF EXISTS {SMOKE_DB}")
    admin.command(f"CREATE DATABASE {SMOKE_DB}")
    admin.command(CREATE_TABLE_SQL.format(db=SMOKE_DB))

    df = generate_events(seed)
    # вставляем в базу проекта-двойника, но config переключаем на неё
    saved_db = config.CLICKHOUSE_DB
    config.CLICKHOUSE_DB = SMOKE_DB
    try:
        # insert через клиент, привязанный к тестовой БД
        smoke_client = F.get_client(database=SMOKE_DB)
        smoke_client.insert_df("events_parsed", df)
        print(f"[smoke] сгенерировано {len(df):,} событий по {df['repo_id'].nunique()} репо "
              f"за {SMOKE_DAYS} дней -> БД {SMOKE_DB}")
        yield smoke_client
    finally:
        config.CLICKHOUSE_DB = saved_db
        admin.command(f"DROP DATABASE IF EXISTS {SMOKE_DB}")
        admin.close()
        print(f"[smoke] БД {SMOKE_DB} удалена, реальных данных не тронули")


if __name__ == "__main__":
    with smoke_environment() as client:
        print(client.query("SELECT count() FROM events_parsed").result_rows)