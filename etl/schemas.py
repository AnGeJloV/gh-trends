from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

RAW_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("forkee", StructType([
            StructField("language", StringType(), True)
        ]), True),
        StructField("pull_request", StructType([
            StructField("base", StructType([
                StructField("repo", StructType([
                    StructField("language", StringType(), True)
                ]), True)
            ]), True)
        ]), True)
    ]), True),
    StructField("created_at", StringType(), True)
])