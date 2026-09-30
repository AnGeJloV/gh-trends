# Загрузка скачанных архивов GH Archive в инфраструктуру:
#   1) .json.gz файлы -> MinIO, бакет raw (сырое озеро для Spark)
#   2) события из файлов -> ClickHouse, таблица gh.events_raw
#
# Запуск (после scripts/download_data.py):
#   python scripts/load_raw.py
#
# Уже загруженные файлы пропускаются — скрипт можно запускать повторно,
# он докачает только новое. Проверка выполняется по колонке source_file.
#
# Настройки подключения можно переопределить переменными окружения
# (по умолчанию всё как в docker-compose.yml):
#   CH_HOST, CH_PORT, CH_USER, CH_PASSWORD   — ClickHouse (default/clickhouse)
#   MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD — MinIO (minioadmin/minioadmin)

import argparse
import gzip
import json
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import boto3
import clickhouse_connect
from botocore.config import Config

# --- настройки по умолчанию (совпадают с docker-compose.yml) ---
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin")
BUCKET = "raw"

BATCH = 50000  # строк в одной вставке в ClickHouse

FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-(\d{1,2})\.json\.gz$")


def create_table(client) -> None:
    client.command("CREATE DATABASE IF NOT EXISTS gh")
    client.command("""
        CREATE TABLE IF NOT EXISTS gh.events_raw (
            event_date  Date,
            event_hour  UInt8,
            source_file String,
            raw         String
        ) ENGINE = MergeTree
        ORDER BY (event_date, event_hour)
    """)


def already_loaded(client) -> set[str]:
    """Файлы, события которых уже лежат в ClickHouse."""
    res = client.query("SELECT DISTINCT source_file FROM gh.events_raw")
    return {row[0] for row in res.result_rows}


def upload_to_minio(s3, path: Path) -> None:
    """Кладёт файл в бакет raw, если его там ещё нет."""
    try:
        s3.head_object(Bucket=BUCKET, Key=path.name)
        return  # уже загружен
    except s3.exceptions.ClientError:
        pass
    print(f"  MinIO: загружаю {path.name} ({path.stat().st_size / 1e6:.1f} МБ)")
    s3.upload_file(str(path), BUCKET, path.name)


def read_events(path: Path):
    """Распаковывает .json.gz и превращает в строки для ClickHouse."""
    m = FILENAME_RE.match(path.name)
    if not m:
        print(f"  Пропуск: имя файла не похоже на YYYY-MM-DD-H.json.gz: {path.name}")
        return []
    day = date.fromisoformat(m.group(1))
    hour = int(m.group(2))
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:  # пропускаем пустые строки, если попадутся
                rows.append((day, hour, path.stem, line))
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Загружает скачанные архивы в MinIO и ClickHouse")
    p.add_argument("--dir", default="data/raw", help="папка с .json.gz (по умолчанию data/raw)")
    p.add_argument("--file", default=None, help="загрузить только один конкретный файл (имя или путь)")
    args = p.parse_args()

    in_dir = Path(args.dir)
    if args.file:
        files = [Path(args.file)]
        if not files[0].exists():  # возможно, передали только имя
            files = [in_dir / args.file]
    else:
        files = sorted(in_dir.glob("*.json.gz"))
    if not files:
        sys.exit(f"Нет .json.gz файлов в {in_dir}. Сначала запусти scripts/download_data.py")

    # --- подключение к ClickHouse ---
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )
    create_table(client)
    loaded = already_loaded(client)

    # --- подключение к MinIO ---
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
    except s3.exceptions.ClientError:
        sys.exit(f"Бакет {BUCKET} не найден в {MINIO_ENDPOINT}. Подними инфраструктуру: docker compose up -d")

    total_rows = 0
    skipped_files = 0
    for path in files:
        if not path.exists():
            print(f"Пропуск: файл не найден: {path}")
            skipped_files += 1
            continue
        if path.stem in loaded:
            print(f"{path.name}: уже в ClickHouse, пропускаю")
            skipped_files += 1
            continue

        print(path.name)
        upload_to_minio(s3, path)

        rows = read_events(path)
        if not rows:
            continue
        for i in range(0, len(rows), BATCH):
            client.insert(
                "gh.events_raw",
                rows[i : i + BATCH],
                column_names=["event_date", "event_hour", "source_file", "raw"],
            )
        loaded.add(path.stem)
        total_rows += len(rows)
        print(f"  ClickHouse: {len(rows):,} событий загружено")

    total = client.query("SELECT count() FROM gh.events_raw").result_rows[0][0]
    print(f"\nГотово. Загружено сейчас: {total_rows:,} событий, пропущено файлов: {skipped_files}.")
    print(f"Всего в gh.events_raw: {total:,}.")
    print("Проверить можно в веб-клиенте: http://localhost:8123/play")


if __name__ == "__main__":
    main()
