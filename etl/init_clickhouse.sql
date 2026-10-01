CREATE DATABASE IF NOT EXISTS gh;

CREATE TABLE IF NOT EXISTS gh.events_parsed (
    event_id String,
    event_type LowCardinality(String),
    actor_id Int64,
    actor_login String,
    repo_id Int64,
    repo_name String,
    language LowCardinality(String),
    commits_count Int32,
    action Nullable(String),
    created_at DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY (event_type, repo_id, created_at, event_id);

CREATE TABLE IF NOT EXISTS gh.mart_language_activity (
    hour DateTime,
    language LowCardinality(String),
    total_events UInt64,
    unique_actors UInt64,
    push_count UInt32,
    star_count UInt32,
    pr_count UInt32,
    fork_count UInt32
) ENGINE = SummingMergeTree()
ORDER BY (hour, language);

CREATE TABLE IF NOT EXISTS gh.mart_repo_activity (
    hour DateTime,
    repo_id Int64,
    repo_name String,
    total_events UInt64,
    unique_actors UInt64,
    stars_gained UInt32,
    forks_gained UInt32,
    total_commits UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, repo_id);

CREATE TABLE IF NOT EXISTS gh.mart_hourly_activity (
    hour DateTime,
    event_type LowCardinality(String),
    event_count UInt64,
    unique_users UInt64,
    unique_repos UInt64
) ENGINE = SummingMergeTree()
ORDER BY (hour, event_type);