# Одноразовый сервис обучения ML-модели (Человек 3).
# Запускается после успешного завершения spark-etl: читает gh.events_parsed,
# обучает модели и пишет прогнозы/метрики в gh.ml_repo_predictions и
# gh.ml_model_metrics (их читает дашборд Человека 4).
#
# ВАЖНО: для обучения на реальных данных нужна история >= ~3 недель
# (окно наблюдения 14 дней + окно исхода 7 дней). Если данных мало,
# контейнер завершится с ошибкой — на работе дашборда это не скажется,
# просто ML-виджеты будут пустыми. Параметры окон: см. ml/config.py.

FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# MPLBACKEND на случай сохранения ROC/PR-кривых без дисплея
ENV MPLBACKEND=Agg

CMD ["python", "train.py"]
