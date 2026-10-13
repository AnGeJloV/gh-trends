# Одноразовый сервис загрузки данных (Человек 1).
# Контекст сборки — КОРЕНЬ репозитория (нужен etl/init_clickhouse.sql):
# собирается через docker-compose, вручную собирать не требуется.
#
# Что делает контейнер (один прогон, затем выходит):
#   1. init_db.py       — создаёт таблицы в ClickHouse (идемпотентно)
#   2. download_data.py — скачивает архивы GH Archive в /app/data/raw (volume)
#   3. load_raw.py      — заливает архивы в MinIO и события в gh.events_raw
#
# Период скачивания настраивается переменными окружения (см. docker-compose.yml):
#   DOWNLOAD_FROM / DOWNLOAD_TO  — период дат (если DOWNLOAD_TO пуст — берутся часы)
#   DOWNLOAD_HOURS / DOWNLOAD_START_HOUR — N часов от стартового часа UTC

FROM python:3.12-slim-bookworm

WORKDIR /app

COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ /app/scripts/
COPY etl/init_clickhouse.sql /app/etl/init_clickhouse.sql

# Данные скачиваются в примонтированный volume ./data -> /app/data,
# поэтому при перезапуске уже скачанное не качается заново.
CMD ["sh", "-c", "\
    python scripts/init_db.py && \
    if [ -n \"$DOWNLOAD_TO\" ]; then \
        python scripts/download_data.py --from \"$DOWNLOAD_FROM\" --to \"$DOWNLOAD_TO\"; \
    else \
        python scripts/download_data.py --from \"$DOWNLOAD_FROM\" --hours \"$DOWNLOAD_HOURS\" --hour \"$DOWNLOAD_START_HOUR\"; \
    fi && \
    if ls data/raw/*.json.gz >/dev/null 2>&1; then \
        python scripts/load_raw.py; \
    else \
        echo 'Новых файлов нет — всё уже загружено в ClickHouse, пропускаю load_raw'; \
    fi"]
