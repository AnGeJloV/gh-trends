# Зона Человека 2 (Spark-разработчик) Дмитрий: PySpark-скрипты парсинга и витрин.

Модуль отвечает за второй этап конвейера: чтение сырых данных из Data Lake (MinIO), извлечение целевых событий, очистку данных от аномалий, расчет аналитических витрин и пакетную выгрузку результатов в аналитическое хранилище ClickHouse.

---

## 📌 Зона ответственности и выполненные задачи

- [x] **PySpark-парсер событий**: реализована фильтрация и извлечение данных для 4 ключевых типов событий GitHub Archive: `PushEvent`, `WatchEvent`, `ForkEvent`, `PullRequestEvent`.
- [x] **Очистка данных (Data Quality)**:
  - Дедупликация событий по первичному ключу (`event_id`).
  - Фильтрация пустых (`NULL`) значений в критически важных полях (`id`, `repo.id`, `created_at`, `repo.name`).
  - Фильтрация выбросов и аномалий: отсечение событий с некорректными датами и бот-спама (аномальные пуши более 10 000 коммитов).
- [x] **Построение аналитических витрин данных**:
  - `mart_language_activity`: почасовая динамика активности по языкам программирования.
  - `mart_repo_activity`: часовая активность и прирост метрик по репозиториям.
  - `mart_hourly_activity`: общее распределение нагрузки и типов событий по часам.
- [x] **Выгрузка в ClickHouse**: пакетная загрузка очищенных событий (`gh.events_parsed`) и витрин через JDBC-драйвер.
- [x] **Контейнеризация**: сборка автономного Docker-образа с предзагрузкой JAR-зависимостей для запуска через `DockerOperator` в Airflow.

---

## 📂 Структура модуля etl/

```text
etl/
├── Dockerfile           # Сборка контейнера на базе Debian Bookworm с Java 17
├── requirements.txt     # Python-зависимости (pyspark)
├── config.py            # Настройки подключения к MinIO (S3A) и ClickHouse
├── schemas.py           # Строгая схема StructType для сырых JSON
├── cleaner.py           # Фильтрация целевых событий, очистка и распаковка payload
├── marts.py             # Расчет трех аналитических витрин
├── writer.py            # Модуль пакетной записи в ClickHouse через JDBC
├── main.py              # Точка входа: сборка SparkSession и запуск конвейера
└── README.md            # Документация модуля
```

---

## ⚙️ Логика обработки и Data Quality

1. **Чтение без разархивации**: PySpark читает сжатые дампы `*.json.gz` напрямую из MinIO по протоколу `s3a://` (бакет `raw`). Применение строгой схемы исключает долгий этап автоматического вывода типов (`inferSchema`) и снижает нагрузку на RAM.
2. **Парсинг вложенных структур**: из `payload` извлекаются параметры коммитов, статус создания PR, а также язык программирования (коалесцируется из `pull_request.base.repo.language` и `forkee.language`).
3. **Очистка**:
   - `dropDuplicates(["id"])` устраняет повторы при повторных выгрузках.
   - Отсекаются битые записи без идентификаторов репозиториев и времени.
   - Спам-фильтр отсекает единичные аномальные пуши размером более 10 000 коммитов.

---

## 📊 Целевые таблицы в ClickHouse

| Таблица / Витрина | Описание | Движок ClickHouse |
| :--- | :--- | :--- |
| **`gh.events_parsed`** | Плоская таблица очищенных событий (автор, репозиторий, язык) | `ReplacingMergeTree` |
| **`gh.mart_language_activity`** | Агрегаты активности по языкам за каждый час (пуши, звезды, PR, форки) | `SummingMergeTree` |
| **`gh.mart_repo_activity`** | Агрегаты активности по каждому репозиторию (звезды, форки, коммиты) | `SummingMergeTree` |
| **`gh.mart_hourly_activity`** | Сводная нагрузка по типам событий и уникальным пользователям по часам | `SummingMergeTree` |

---

## 🚀 Инструкция по запуску

### 1. Сборка Docker-образа
Сборка выполняет предзагрузку Java-пакетов (`hadoop-aws`, `aws-java-sdk-bundle`, `clickhouse-jdbc`), чтобы контейнер стартовал мгновенно:

```bash
docker build -t gh-trends-etl:latest ./etl
```

### 2. Запуск пайплайна
Запуск в единой Docker-сети проекта:

```bash
docker run --rm --network gh-trends_default gh-trends-etl:latest
```

---

## 🔄 Оркестрация (Airflow / DockerOperator)

Модуль полностью готов к запуску через `DockerOperator`:

```python
from airflow.providers.docker.operators.docker import DockerOperator

spark_etl = DockerOperator(
    task_id="spark_etl_run",
    image="gh-trends-etl:latest",
    api_version="auto",
    auto_remove=True,
    command="python main.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="gh-trends_default",
    environment={
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_BUCKET": "raw",
        "CLICKHOUSE_HOST": "clickhouse",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_DB": "gh",
        "CLICKHOUSE_USER": "default",
        "CLICKHOUSE_PASSWORD": "clickhouse"
    }
)
```