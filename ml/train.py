"""Обучение модели «забросят ли репозиторий» (Человек 3 — Data Scientist).

Запуск (после того, как Spark-ETL наполнил gh.events_parsed):
    python ml/train.py                    # обучение + экспорт прогнозов в ClickHouse
    python ml/train.py --smoke            # самотест на синтетике (без реальных данных)
    python ml/train.py --no-export        # только обучение и метрики, ничего не пишет в БД

Что делает:
1. Собирает признаки из ClickHouse (features.build_dataset).
2. Делит выборку по репозиториям (один репозиторий не попадает
   одновременно в train и test — иначе снимки одного репо утекут в тест).
3. Обучает бейслайн (LogisticRegression + StandardScaler) и улучшенные
   модели (RandomForest, CatBoost), сравнивает метрики.
4. Считает бизнес-метрики: precision@K / recall@K — насколько точен
   «список K репозиториев под риском», который уходит на дашборд.
5. Сохраняет модель, отчёт (metrics.json, ROC/PR-кривые) и выгружает
   прогнозы в gh.ml_repo_predictions для Streamlit-дашборда Человека 4.
"""
import argparse
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, confusion_matrix,
                             f1_score, precision_score, recall_score,
                             roc_auc_score, roc_curve)
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import config
import features as F

RANDOM_STATE = 42
TOP_LANGUAGES = 15  # языки вне топ-N уходят в корзину "other"


# ---------------------------------------------------------------- данные ---

def prepare_matrix(df: pd.DataFrame):
    """Числовая матрина признаков + one-hot для языка."""
    lang = df["language"].fillna("Unknown")
    top = lang.value_counts().nlargest(TOP_LANGUAGES).index
    lang_bucket = lang.where(lang.isin(top), "other")
    lang_dummies = pd.get_dummies(lang_bucket, prefix="lang").astype(float)
    X = pd.concat([df[F.FEATURE_COLUMNS].astype(float), lang_dummies], axis=1)
    return X, df["abandoned"].astype(int), df


def group_split(X, y, groups, test_size=0.25):
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=RANDOM_STATE)
    train_idx, test_idx = next(splitter.split(X, y, groups))
    return train_idx, test_idx


# ------------------------------------------------------------- модели -----

def make_models():
    return {
        "logreg_baseline": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced",
                                       random_state=RANDOM_STATE)),
        ]),
        "random_forest": RandomForestClassifier(
            n_estimators=300, min_samples_leaf=3, class_weight="balanced",
            n_jobs=-1, random_state=RANDOM_STATE),
        "catboost": None,  # создаётся лениво, см. fit_catboost
    }


def fit_catboost(X_tr, y_tr, X_val=None, y_val=None):
    from catboost import CatBoostClassifier, Pool
    train_pool = Pool(X_tr, y_tr)
    eval_pool = Pool(X_val, y_val) if X_val is not None else None
    model = CatBoostClassifier(
        iterations=500, learning_rate=0.05, depth=6,
        auto_class_weights="Balanced", loss_function="Logloss",
        random_seed=RANDOM_STATE, verbose=0,
    )
    model.fit(train_pool, eval_set=eval_pool, early_stopping_rounds=50,
              verbose_eval=False)
    return model


# --------------------------------------------------------- метрики -------

def best_f1_threshold(y_true, proba):
    """Порог, дающий максимальный F1 на отложенной выборке."""
    candidates = np.unique(np.round(proba, 3))
    best_t, best_f = 0.5, -1.0
    for t in candidates:
        f = f1_score(y_true, (proba >= t).astype(int), zero_division=0)
        if f > best_f:
            best_f, best_t = f, float(t)
    return best_t, best_f


def business_metrics(y_true, proba, k):
    """Precision@K / Recall@K: качество «списка K репозиториев под риском»."""
    if k >= len(y_true):
        k = len(y_true)
    top = np.argsort(-proba)[:k]
    hits = int(np.asarray(y_true)[top].sum())
    total_pos = int(np.sum(y_true))
    return {
        "k": int(k),
        "precision_at_k": hits / k if k else None,
        "recall_at_k": hits / total_pos if total_pos else None,
    }


def evaluate(name, y_true, proba, k_frac=0.05):
    proba = np.asarray(proba)
    y_arr = np.asarray(y_true)
    threshold, best_f1 = best_f1_threshold(y_arr, proba)
    pred = (proba >= threshold).astype(int)
    k = max(10, int(len(y_arr) * k_frac))
    metrics = {
        "model": name,
        "roc_auc": float(roc_auc_score(y_arr, proba)) if len(np.unique(y_arr)) > 1 else None,
        "pr_auc": float(average_precision_score(y_arr, proba)),
        "f1_best": float(best_f1),
        "threshold": float(threshold),
        "precision": float(precision_score(y_arr, pred, zero_division=0)),
        "recall": float(recall_score(y_arr, pred, zero_division=0)),
    }
    metrics.update(business_metrics(y_arr, proba, k))
    metrics["confusion"] = confusion_matrix(y_arr, pred).tolist()
    return metrics


# ------------------------------------------------------- экспорт в CH ----

def ensure_output_tables(client):
    db = config.CLICKHOUSE_DB
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.{config.PREDICTIONS_TABLE} (
            snapshot_date Date,
            repo_id Int64,
            repo_name String,
            language LowCardinality(String),
            probability Float32,
            prediction UInt8,
            label Nullable(UInt8),
            model String
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (snapshot_date, repo_id)
    """)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.{config.METRICS_TABLE} (
            trained_at DateTime,
            model String,
            dataset_rows UInt32,
            abandoned_share Float32,
            roc_auc Nullable(Float32),
            pr_auc Float32,
            f1 Float32,
            precision Float32,
            recall Float32,
            precision_at_k Nullable(Float32),
            recall_at_k Nullable(Float32),
            k UInt32,
            params String
        ) ENGINE = ReplacingMergeTree(trained_at)
        ORDER BY (model, trained_at)
    """)


def export_predictions(client, predict_df, fitted_model, model_name, threshold):
    db = config.CLICKHOUSE_DB
    rows = predict_df.copy()
    X, _, _ = prepare_matrix(rows)
    proba = fitted_model.predict_proba(X)[:, 1]
    rows["probability"] = proba.astype("float32")
    rows["prediction"] = (proba >= threshold).astype("uint8")
    rows["label"] = rows["future_events"].map(lambda v: None if pd.isna(v) else int(v))
    rows["model"] = model_name
    out = rows[["snapshot_date", "repo_id", "repo_name", "language",
                "probability", "prediction", "label", "model"]].copy()
    out["snapshot_date"] = out["snapshot_date"].dt.date
    ensure_output_tables(client)
    client.insert_df(f"{db}.{config.PREDICTIONS_TABLE}", out)


def export_metrics(client, metrics_list, dataset_rows, abandoned_share):
    db = config.CLICKHOUSE_DB
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    rows = [{
        "trained_at": now,
        "model": m["model"],
        "dataset_rows": int(dataset_rows),
        "abandoned_share": float(abandoned_share),
        "roc_auc": None if m["roc_auc"] is None else float(m["roc_auc"]),
        "pr_auc": float(m["pr_auc"]),
        "f1": float(m["f1_best"]),
        "precision": float(m["precision"]),
        "recall": float(m["recall"]),
        "precision_at_k": None if m["precision_at_k"] is None else float(m["precision_at_k"]),
        "recall_at_k": None if m["recall_at_k"] is None else float(m["recall_at_k"]),
        "k": int(m["k"]),
        "params": json.dumps({k2: v for k2, v in m.items() if k2 not in
                              ("model", "confusion", "roc_auc", "pr_auc", "f1_best",
                               "threshold", "precision", "recall", "precision_at_k",
                               "recall_at_k", "k")}, ensure_ascii=False),
    } for m in metrics_list]
    ensure_output_tables(client)
    client.insert_df(f"{db}.{config.METRICS_TABLE}", pd.DataFrame(rows))


# ------------------------------------------------------------- main ------

def run_training(client, out_dir: Path, do_export: bool) -> dict:
    train_df, predict_df = F.build_dataset(client)
    share = float(train_df["abandoned"].mean())
    print(f"Датасет: {len(train_df)} строк (снимки x репо), "
          f"репозиториев: {train_df['repo_id'].nunique()}, доля 'abandoned': {share:.2%}")

    X, y, df = prepare_matrix(train_df)
    groups = train_df["repo_id"].to_numpy()
    tr_idx, te_idx = group_split(X, y, groups)
    X_tr, X_te = X.iloc[tr_idx], X.iloc[te_idx]
    y_tr, y_te = y.iloc[tr_idx], y.iloc[te_idx]
    print(f"Split по репозиториям: train={len(tr_idx)}, test={len(te_idx)} "
          f"(репо в test: {pd.unique(groups[te_idx]).size})")

    models = make_models()
    fitted = {}
    for name, model in models.items():
        if name == "catboost":
            continue
        model.fit(X_tr, y_tr)
        fitted[name] = model

    # CatBoost: часть train отдаём под early stopping
    n_val = max(1, int(len(tr_idx) * 0.15))
    cb = fit_catboost(X_tr.iloc[:-n_val], y_tr.iloc[:-n_val],
                      X_tr.iloc[-n_val:], y_tr.iloc[-n_val:])
    fitted["catboost"] = cb

    results = []
    for name, model in fitted.items():
        proba = model.predict_proba(X_te)[:, 1]
        results.append(evaluate(name, y_te, proba))

    # --- отчёт в консоль ---
    header = f"{'модель':<18}{'ROC-AUC':>9}{'PR-AUC':>9}{'F1':>7}{'Prec@K':>8}{'Rec@K':>8}"
    print("\n" + header)
    print("-" * len(header))
    for m in results:
        roc = f"{m['roc_auc']:.3f}" if m["roc_auc"] is not None else "n/a"
        pak = f"{m['precision_at_k']:.3f}" if m["precision_at_k"] is not None else "n/a"
        rak = f"{m['recall_at_k']:.3f}" if m["recall_at_k"] is not None else "n/a"
        print(f"{m['model']:<18}{roc:>9}{m['pr_auc']:>9.3f}{m['f1_best']:>7.3f}{pak:>8}{rak:>8}")

    best = max(results, key=lambda m: m["pr_auc"])
    print(f"\nЛучшая модель по PR-AUC: {best['model']} (порог {best['threshold']:.3f})")
    print(f"Confusion matrix (test): {best['confusion']}")

    out_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(fitted[best["model"]], out_dir / "model.joblib")
    (out_dir / "metrics.json").write_text(
        json.dumps({"dataset_rows": int(len(train_df)),
                    "abandoned_share": share,
                    "feature_columns": list(X.columns),
                    "results": results},
                   ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    _save_curves(fitted[best["model"]], X_te, y_te, out_dir)
    print(f"\nМодель и отчёт сохранены: {out_dir}/model.joblib, metrics.json")

    if do_export:
        export_metrics(client, results, len(train_df), share)
        print(f"Метрики выгружены в {config.CLICKHOUSE_DB}.{config.METRICS_TABLE}")
        if predict_df.empty:
            print("Прогнозы не выгружены: нет снимка с незакрытым окном исхода "
                  "(данные заканчиваются ровно на границе окна).")
        else:
            export_predictions(client, predict_df, fitted[best["model"]], best["model"], best["threshold"])
            print(f"Экспорт в ClickHouse: {len(predict_df)} прогнозов -> "
                  f"{config.CLICKHOUSE_DB}.{config.PREDICTIONS_TABLE}")

    return {"best": best["model"], "results": results}


def _save_curves(model, X_te, y_te, out_dir: Path):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        proba = model.predict_proba(X_te)[:, 1]
        fpr, tpr, _ = roc_curve(y_te, proba)
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.plot(fpr, tpr)
        ax.plot([0, 1], [0, 1], "--", color="gray", linewidth=0.8)
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.set_title("ROC curve (test)")
        fig.tight_layout()
        fig.savefig(out_dir / "roc_curve.png", dpi=120)
        plt.close(fig)
    except Exception as e:  # графики не критичны для пайплайна
        print(f"(не удалось сохранить ROC-кривую: {e})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true",
                        help="самотест на синтетических данных (создаёт и удаляет БД gh_ml_smoke)")
    parser.add_argument("--no-export", action="store_true",
                        help="не писать прогнозы и метрики в ClickHouse")
    parser.add_argument("--out-dir", default=str(Path(__file__).parent / "models"))
    args = parser.parse_args()

    if args.smoke:
        import smoke_test
        with smoke_test.smoke_environment() as client:
            run_training(client, Path(args.out_dir), do_export=False)
        return

    client = F.get_client()
    try:
        run_training(client, Path(args.out_dir), do_export=not args.no_export)
    finally:
        client.close()


if __name__ == "__main__":
    main()