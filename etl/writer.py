from pyspark.sql import DataFrame
import base64
import urllib.request

import config

def _truncate_table(table_name: str) -> None:
    """TRUNCATE TABLE напрямую через HTTP-интерфейс ClickHouse.

    Почему не Spark .mode("overwrite") + .option("truncate"): у Spark нет
    встроенного диалекта ClickHouse, и для незнакомых диалектов truncate
    не срабатывает — Spark делает DROP TABLE + CREATE, и таблица создаётся
    без движков/ORDER BY (ClickHouse падает с Code 42). Прямой TRUNCATE
    сохраняет схему и движки (SummingMergeTree и т.д.), а данные
    перезаписываются начисто — повторный прогон ETL не создаёт дублей.
    """
    url = f"http://{config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}/?query=TRUNCATE%20TABLE%20{config.CLICKHOUSE_DB}.{table_name}"
    token = base64.b64encode(
        f"{config.CLICKHOUSE_USER}:{config.CLICKHOUSE_PASSWORD}".encode()
    ).decode()
    req = urllib.request.Request(url, data=b"", method="POST")
    req.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(req, timeout=120) as resp:
        resp.read()

def write_to_clickhouse(df: DataFrame, table_name: str, mode: str = "overwrite"):
    # mode="overwrite" = TRUNCATE + запись начисто (идемпотентный ETL);
    # mode="append" = простая дозапись.
    if mode == "overwrite":
        _truncate_table(table_name)
    (
        df.write
        .format("jdbc")
        .option("url", config.CLICKHOUSE_URL)
        .option("dbtable", f"{config.CLICKHOUSE_DB}.{table_name}")
        .option("user", config.CLICKHOUSE_USER)
        .option("password", config.CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "50000")
        .mode("append")
        .save()
    )
