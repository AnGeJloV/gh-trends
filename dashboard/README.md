# Зона Человека 4 (BI): Streamlit-дашборд

## Что делать

1. Написать `dashboard/app.py` (Streamlit) и `dashboard/Dockerfile`.
2. Открыть источники данных — всё уже посчитано в ClickHouse (`gh`):
   - витрины: `gh.mart_language_activity`, `gh.mart_repo_activity`, `gh.mart_hourly_activity`
   - ML: `gh.ml_repo_predictions` (топ репо под риском), `gh.ml_model_metrics` (метрики моделей)
   - готовые SQL-запросы под виджеты — в [../ml/README.md](../ml/README.md)
   - подключение из контейнера: хост `clickhouse`, порт 8123, user `default`,
     пароль `clickhouse` (всё уже передано в сервис через env в docker-compose.yml)
3. **Включить сервис дашборда в общий запуск:** в `docker-compose.yml` у сервиса
   `dashboard` удалить строку `profiles: ["dashboard"]` — после этого дашборд
   поднимается обычным `docker compose up -d` на http://localhost:8501.
   До этого момента он запускается только через
   `docker compose --profile dashboard up -d`.
4. Дашборд должен корректно показывать «данных пока нет», если таблицы
   пустые (например, ML-таблиц нет, если данные короче ~3 недель —
   ml-trainer тогда не обучается, это штатно).
