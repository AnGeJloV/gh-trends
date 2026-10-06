# Зона Человека 3 (Data Scientist) Евгений: подготовка признаков, обучение модели, метрики.

Модуль решает задачу **прогноза заброшенности репозитория** (Abandonment prediction): по активности репозитория за окно наблюдения предсказать, «умрёт» ли он в следующем окне. Модель и прогнозы отдаются в ClickHouse, откуда их подхватывает дашборд Человека 4.

---

## 📌 Постановка задачи

- **Объект**: репозиторий на момент времени `t1` (снимок, snapshot).
- **Окно наблюдения W1** (по умолчанию 14 дней до `t1`): из событий считаются признаки.
- **Окно исхода W2** (по умолчанию 7 дней после `t1`): метка — `abandoned = 1`, если в W2 у репозитория **не было ни одного события** (Push/Watch/Fork/PR).
- **Снимки** делаются с шагом 7 дней, поэтому один репозиторий даёт несколько обучающих примеров. Сплит train/test идёт **по репозиториям** (`GroupShuffleSplit`), чтобы снимки одного репо не утекали из train в test.

## 📊 Признаки (считаются SQL-ом в ClickHouse)

Все признаки считаются агрегатами над `gh.events_parsed` — в Python приходит только таблица «1 строка = 1 репозиторий на 1 снимок»:

| Признак | Смысл |
| :--- | :--- |
| `events_total`, `pushes`, `commits_total` | общая активность и интенсивность коммитов |
| `stars`, `forks`, `prs` | интерес сообщества |
| `actors`, `top_actor_share` | число контрибьюторов и концентрация на одном авторе |
| `active_days`, `events_per_active_day` | регулярность активности |
| `hours_since_last` | давность последнего события |
| `events_first_half` / `events_second_half`, `trend_ratio` | затухает ли активность внутри окна |
| `language` (one-hot) | категориальный признак |

## 🤖 Модели

| Модель | Роль |
| :--- | :--- |
| `logreg_baseline` | бейслайн: StandardScaler + LogisticRegression (class_weight=balanced) |
| `random_forest` | сравнение с бейслайном |
| `catboost` | улучшенная модель (градиентный бустинг, early stopping) |

Метрики: **ROC-AUC, PR-AUC, F1** (порог подбирается по максимуму F1), precision/recall.

Бизнес-метрики: **Precision@K / Recall@K** — насколько точен «список K репозиториев под риском», который уходит в дашборд (K = max(10, 5% выборки)).

## 📂 Структура

```text
ml/
├── config.py        # подключения к ClickHouse + окна W1/W2 (env-переменные ML_*)
├── features.py      # SQL-агрегация признаков и меток из gh.events_parsed
├── train.py         # обучение 3 моделей, метрики, экспорт результатов
├── smoke_test.py    # самотест на синтетических данных (без интернета)
├── requirements.txt
└── models/          # артефакты обучения (в git не попадают)
```

## 🚀 Инструкция по запуску

Предусловие: инфраструктура поднята (`docker compose up -d`), данные загружены и ETL отработал (`docker compose up --build spark-etl`) — иначе в `gh.events_parsed` пусто.

```bash
pip install -r ml/requirements.txt

# Полный прогон: обучение + выгрузка прогнозов и метрик в ClickHouse
python ml/train.py

# Только обучение и метрики, без записи в БД
python ml/train.py --no-export

# Самотест на синтетике: создаёт временную БД gh_ml_smoke, обучает модели, удаляет БД.
# Реальные данные не трогает, из интернета ничего не качает.
python ml/train.py --smoke
```

Если данных пока мало (меньше ~3 недель истории), окна можно сжать через переменные окружения:

```bash
ML_OBS_WINDOW_DAYS=7 ML_FUTURE_WINDOW_DAYS=3 ML_SNAPSHOT_STRIDE_DAYS=3 python ml/train.py
```

## 🤝 Стыковка с Человеком 4 (дашборд)

> После каждого запуска `python ml/train.py` эти таблицы обновляются — дашборд просто читает их из ClickHouse (тот же инстанс, что и у витрин Spark-ETL).

### Таблица `gh.ml_repo_predictions` — прогнозы по репозиториям

| Колонка | Тип | Что это для дашборда |
| :--- | :--- | :--- |
| `snapshot_date` | Date | дата снимка — фильтр по дате прогноза |
| `repo_id` | Int64 | идентификатор репо |
| `repo_name` | String | имя вида `owner/repo` — подписи на графиках |
| `language` | String | фильтр по языку |
| `probability` | Float32 | вероятность заброшенности 0..1 — шкала/раскраска |
| `prediction` | UInt8 | 1 = «прогноз: забросят» (порог подобран по max F1) — флаг-фильтр |
| `label` | Nullable(UInt8) | фактический исход, если окно исхода уже прошло (иначе NULL) — можно считать «точность прогноза» на дашборде |
| `model` | String | какая модель дала прогноз |

Примеры запросов:

```sql
-- Топ-20 репозиториев под риском заброшенности (основной виджет)
SELECT repo_name, language, probability
FROM gh.ml_repo_predictions FINAL
WHERE snapshot_date = (SELECT max(snapshot_date) FROM gh.ml_repo_predictions)
ORDER BY probability DESC
LIMIT 20;

-- Согласованность прогноза с фактом (для карточки «качество модели»)
SELECT count() AS total,
       countIf(prediction = 1) AS flagged,
       countIf(label = 1) AS actually_abandoned,
       countIf(prediction = 1 AND label = 1) AS hits
FROM gh.ml_repo_predictions
FINAL
WHERE snapshot_date = (SELECT max(snapshot_date) FROM gh.ml_repo_predictions);
```

### Таблица `gh.ml_model_metrics` — качество моделей

Одна строка на модель в каждом прогоне: `model, roc_auc, pr_auc, f1, precision, recall, precision_at_k, recall_at_k, k, trained_at`. Для дашборда берите последнюю дату:

```sql
SELECT model, roc_auc, pr_auc, precision_at_k, recall_at_k
FROM gh.ml_model_metrics
WHERE trained_at = (SELECT max(trained_at) FROM gh.ml_model_metrics)
ORDER BY pr_auc DESC;
```

### Нюанс ClickHouse

Обе таблицы на `ReplacingMergeTree` — при повторных запусках старые строки схлопываются не мгновенно. В запросах указывайте `FINAL` (как выше) или фильтр по последним `snapshot_date` / `trained_at`, иначе могут всплыть дубли из прошлых прогонов.

### Дополнительные идеи для виджетов

- Гистограмма распределения `probability` (сколько репо в «красной зоне»);
- разбивка топ-K рискованных репо по языкам;
- сравнение прогнозов между датами снимков (динамика риска).

## 🎯 Выходные таблицы в ClickHouse

| Таблица | Содержимое |
| :--- | :--- |
| `gh.ml_repo_predictions` | прогноз по каждому репо последнего снимка: `snapshot_date, repo_id, repo_name, language, probability, prediction, label, model` |
| `gh.ml_model_metrics` | метрики всех моделей + параметры запуска (история прогонов через ReplacingMergeTree) |

## ✅ Статус Этапа 3

- [x] Выбрана задача: прогноз заброшенности репозитория
- [x] Признаки: активность, коммиты, контрибьюторы, тренд затухания (см. таблицу выше)
- [x] Бейслайн: логистическая регрессия + метрики
- [x] Улучшенные модели: RandomForest, CatBoost + сравнение
- [ ] Полноценное обучение на реальных данных (ожидает полной загрузки периода 2026-09-01+ и полного прогона ETL)
