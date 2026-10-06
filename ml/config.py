"""Конфигурация ML-модуля (Человек 3 — Data Scientist).

Параметры подключения совпадают с etl/config.py, но по умолчанию
ориентированы на локальный запуск (ClickHouse поднят через docker compose
и проброшен на localhost:8123). Всё переопределяется переменными окружения.
"""
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "gh")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

# --- Параметры задачи «прогноз заброшенности репозитория» ---
# Окно наблюдения W1: по активности за последние OBS_WINDOW_DAYS дней
# строим признаковое описание репозитория на момент t1.
OBS_WINDOW_DAYS = int(os.getenv("ML_OBS_WINDOW_DAYS", "14"))
# Окно исхода W2: репозиторий считается «заброшенным», если в следующие
# FUTURE_WINDOW_DAYS дней после t1 у него не было ни одного события.
FUTURE_WINDOW_DAYS = int(os.getenv("ML_FUTURE_WINDOW_DAYS", "7"))
# Снимки делаются каждые SNAPSHOT_STRIDE_DAYS дней.
SNAPSHOT_STRIDE_DAYS = int(os.getenv("ML_SNAPSHOT_STRIDE_DAYS", "7"))
# Репозиторий попадает в выборку, только если в окне наблюдения у него
# не меньше MIN_EVENTS_IN_OBS событий (шум-фильтр от мёртвых записей).
MIN_EVENTS_IN_OBS = int(os.getenv("ML_MIN_EVENTS", "3"))

# Таблица-источник (заполняется Spark-ETL'ом Человека 2)
SOURCE_TABLE = os.getenv("ML_SOURCE_TABLE", "events_parsed")

# Куда пишем результаты для дашборда Человека 4
PREDICTIONS_TABLE = os.getenv("ML_PREDICTIONS_TABLE", "ml_repo_predictions")
METRICS_TABLE = os.getenv("ML_METRICS_TABLE", "ml_model_metrics")