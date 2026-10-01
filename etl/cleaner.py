from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, current_timestamp, to_timestamp

TARGET_EVENTS = ["PushEvent", "WatchEvent", "ForkEvent", "PullRequestEvent"]

def parse_and_clean_events(df: DataFrame) -> DataFrame:
    # 1. Фильтруем 4 целевых события
    filtered_df = df.filter(col("type").isin(TARGET_EVENTS))

    # 2. Удаляем дубликаты по id события
    deduped_df = filtered_df.dropDuplicates(["id"])

    # 3. Приводим timestamp
    with_ts_df = deduped_df.withColumn("created_at", to_timestamp(col("created_at")))

    # 4. Фильтруем NULL в критических полях
    clean_df = with_ts_df.filter(
        col("id").isNotNull() &
        col("repo.id").isNotNull() &
        col("repo.name").isNotNull() &
        col("created_at").isNotNull() &
        (col("repo.name") != "")
    )

    # 5. Отсекаем выбросы: неадекватные даты и бот-пуши (> 10 000 коммитов)
    valid_dates_df = clean_df.filter(
        (col("created_at") <= current_timestamp()) &
        (col("created_at") >= lit("2020-01-01 00:00:00"))
    )

    valid_commits_df = valid_dates_df.filter(
        (col("type") != "PushEvent") |
        ((col("type") == "PushEvent") & (coalesce(col("payload.size"), lit(1)) <= 10000))
    )

    # 6. Распаковываем в плоскую структуру для таблицы events_parsed
    return valid_commits_df.select(
        col("id").cast("String").alias("event_id"),
        col("type").alias("event_type"),
        col("actor.id").alias("actor_id"),
        col("actor.login").alias("actor_login"),
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("repo_name"),
        coalesce(
            col("payload.pull_request.base.repo.language"),
            col("payload.forkee.language"),
            lit("Unknown")
        ).alias("language"),
        coalesce(col("payload.size"), lit(0)).alias("commits_count"),
        col("payload.action").alias("action"),
        col("created_at")
    )