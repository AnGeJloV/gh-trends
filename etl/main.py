from pyspark.sql import SparkSession
from schemas import RAW_SCHEMA
from cleaner import parse_and_clean_events
from marts import build_language_mart, build_repo_mart, build_hourly_mart
from writer import write_to_clickhouse
import config

def get_spark_session() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "com.clickhouse:clickhouse-jdbc:0.6.3"
    ]
    return (
        SparkSession.builder
        .appName("GH-Trends-Spark-ETL")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def main():
    print("--> Starting Spark Session...")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"--> Reading raw .json.gz from MinIO: {config.DATA_INPUT_PATH}")
    raw_df = spark.read.schema(RAW_SCHEMA).json(config.DATA_INPUT_PATH)

    print("--> Cleaning and parsing events...")
    events_parsed = parse_and_clean_events(raw_df).cache()

    print("--> Building marts...")
    lang_mart = build_language_mart(events_parsed)
    repo_mart = build_repo_mart(events_parsed)
    hourly_mart = build_hourly_mart(events_parsed)

    print("--> Writing to ClickHouse (events_parsed)...")
    write_to_clickhouse(events_parsed, "events_parsed")

    print("--> Writing to ClickHouse (marts)...")
    write_to_clickhouse(lang_mart, "mart_language_activity")
    write_to_clickhouse(repo_mart, "mart_repo_activity")
    write_to_clickhouse(hourly_mart, "mart_hourly_activity")

    print("--> SUCCESS! Spark ETL completed.")
    spark.stop()

if __name__ == "__main__":
    main()