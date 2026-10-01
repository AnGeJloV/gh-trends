from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc, count, countDistinct, sum as _sum, when

def build_language_mart(events_df: DataFrame) -> DataFrame:
    """Витрина: Активность по языкам"""
    return events_df.filter(col("language") != "Unknown") \
        .groupBy(
            date_trunc("hour", col("created_at")).alias("hour"),
            col("language")
        ).agg(
            count("event_id").alias("total_events"),
            countDistinct("actor_id").alias("unique_actors"),
            _sum(when(col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
            _sum(when(col("event_type") == "WatchEvent", 1).otherwise(0)).alias("star_count"),
            _sum(when(col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
            _sum(when(col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_count")
        )

def build_repo_mart(events_df: DataFrame) -> DataFrame:
    """Витрина: Активность по репозиториям"""
    return events_df.groupBy(
        date_trunc("hour", col("created_at")).alias("hour"),
        col("repo_id"),
        col("repo_name")
    ).agg(
        count("event_id").alias("total_events"),
        countDistinct("actor_id").alias("unique_actors"),
        _sum(when(col("event_type") == "WatchEvent", 1).otherwise(0)).alias("stars_gained"),
        _sum(when(col("event_type") == "ForkEvent", 1).otherwise(0)).alias("forks_gained"),
        _sum(col("commits_count")).alias("total_commits")
    )

def build_hourly_mart(events_df: DataFrame) -> DataFrame:
    """Витрина: Активность по часам"""
    return events_df.groupBy(
        date_trunc("hour", col("created_at")).alias("hour"),
        col("event_type")
    ).agg(
        count("event_id").alias("event_count"),
        countDistinct("actor_id").alias("unique_users"),
        countDistinct("repo_id").alias("unique_repos")
    )