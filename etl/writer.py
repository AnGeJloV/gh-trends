from pyspark.sql import DataFrame
import config

def write_to_clickhouse(df: DataFrame, table_name: str, mode: str = "append"):
    (
        df.write
        .format("jdbc")
        .option("url", config.CLICKHOUSE_URL)
        .option("dbtable", f"{config.CLICKHOUSE_DB}.{table_name}")
        .option("user", config.CLICKHOUSE_USER)
        .option("password", config.CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "50000")
        .mode(mode)
        .save()
    )