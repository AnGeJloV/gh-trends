# Применяет DDL из etl/init_clickhouse.sql к ClickHouse (идемпотентно:
# все CREATE ... IF NOT EXISTS, повторный запуск ничего не ломает).
#
# Зачем отдельный шаг: монтирование SQL в /docker-entrypoint-initdb.d
# срабатывает только при ПЕРВОМ старте контейнера с пустым volume.
# У тех, кто уже поднимал ClickHouse, volume не пуст — и таблицы витрин
# не создадутся. Этот скрипт гарантирует схему на любом состоянии.
#
# Запуск: python scripts/init_db.py
# Настройки подключения — переменные окружения (как в load_raw.py):
#   CH_HOST, CH_PORT, CH_USER, CH_PASSWORD

import os
import sys
from pathlib import Path

import clickhouse_connect

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse")

DDL_FILE = Path(__file__).resolve().parent.parent / "etl" / "init_clickhouse.sql"


def split_statements(sql: str) -> list[str]:
    """Делит файл на отдельные statement'ы по ';' в конце строки."""
    statements = []
    for chunk in sql.split(";"):
        # убираем строки-комментарии и пустоту
        lines = [l for l in chunk.splitlines() if l.strip() and not l.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


def main() -> None:
    if not DDL_FILE.exists():
        sys.exit(f"Не найден DDL-файл: {DDL_FILE}")

    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )

    statements = split_statements(DDL_FILE.read_text(encoding="utf-8"))
    for stmt in statements:
        client.command(stmt)
        # первая строка statement'а — для лога
        print(f"  OK: {stmt.splitlines()[0]} ...")

    res = client.query("SELECT name FROM system.tables WHERE database = 'gh' ORDER BY name")
    tables = [row[0] for row in res.result_rows]
    print(f"Схема готова. Таблицы в gh: {', '.join(tables) if tables else '(пока нет)'}")


if __name__ == "__main__":
    main()
