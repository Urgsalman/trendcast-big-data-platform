"""Entraîne et évalue des régressions Spark ML en ordre chronologique."""

import argparse
import re
import sys
from datetime import datetime, timezone
from importlib.metadata import version as package_version
from pathlib import Path
from time import perf_counter

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.ml.features import CORE_FEATURES, EXTRA_FEATURES
from analytics.ml.runtime import (
    MODEL_ROOT,
    TARGET,
    ensure_new_hdfs_path,
    print_manifest,
    read_manifest,
    start_spark,
    write_manifest,
)

SEED = 345
INITIAL = {
    "LinearRegression": {"regParam": 0.0, "elasticNetParam": 0.0, "maxIter": 50},
    "DecisionTreeRegressor": {"maxDepth": 5, "minInstancesPerNode": 10, "maxBins": 32},
    "RandomForestRegressor": {"numTrees": 16, "maxDepth": 5, "minInstancesPerNode": 10},
}
GRID = {
    "LinearRegression": [
        {"regParam": 0.01, "elasticNetParam": 0.0, "maxIter": 60},
        {"regParam": 0.1, "elasticNetParam": 0.25, "maxIter": 80},
        {"regParam": 0.5, "elasticNetParam": 0.0, "maxIter": 80},
    ],
    "DecisionTreeRegressor": [
        {"maxDepth": 3, "minInstancesPerNode": 15, "maxBins": 32},
        {"maxDepth": 7, "minInstancesPerNode": 5, "maxBins": 32},
        {"maxDepth": 5, "minInstancesPerNode": 15, "maxBins": 48},
    ],
    "RandomForestRegressor": [
        {"numTrees": 12, "maxDepth": 4, "minInstancesPerNode": 15},
        {"numTrees": 24, "maxDepth": 6, "minInstancesPerNode": 10},
        {"numTrees": 32, "maxDepth": 7, "minInstancesPerNode": 5},
    ],
}
ALGORITHMS = {
    "LinearRegression": LinearRegression,
    "DecisionTreeRegressor": DecisionTreeRegressor,
    "RandomForestRegressor": RandomForestRegressor,
}


def split_temporally(frame):
    dates = [
        r.prediction_date
        for r in frame.select("prediction_date")
        .distinct()
        .orderBy("prediction_date")
        .collect()
    ]
    if len(dates) < 10:
        raise ValueError(
            "Au moins dix dates cibles sont nécessaires pour les trois partitions"
        )
    train_boundary = dates[int(len(dates) * 0.70)]
    test_boundary = dates[int(len(dates) * 0.85)]
    date = F.col("prediction_date")
    return {
        "train": frame.where(date < train_boundary),
        "validation": frame.where((date >= train_boundary) & (date < test_boundary)),
        "test": frame.where(date >= test_boundary),
    }


def split_summary(frame):
    return (
        frame.agg(
            F.count("*").alias("rows"),
            F.countDistinct("prediction_date").alias("days"),
            F.min("prediction_date").alias("first_target_date"),
            F.max("prediction_date").alias("last_target_date"),
        )
        .first()
        .asDict()
    )


def model_pipeline(name, params, features, missing):
    settings = dict(params, labelCol=TARGET)
    if name != "LinearRegression":
        settings["seed"] = SEED
    inputs = [f"imputed_{c}" if c in missing else c for c in features]
    stages = []
    if missing:
        stages.append(
            Imputer(
                strategy="median",
                inputCols=list(missing),
                outputCols=[f"imputed_{c}" for c in missing],
            )
        )
    stages.extend(
        [
            VectorAssembler(inputCols=inputs, outputCol="features"),
            ALGORITHMS[name](**settings),
        ]
    )
    return Pipeline(stages=stages)


def finite(column):
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) < F.lit(float("inf")))


def nonnegative_prediction(column):
    numeric = column.cast("double")
    return F.when(finite(numeric), F.greatest(F.lit(0.0), numeric))


def scored_with(frame, column):
    return frame.withColumn("predicted_views", nonnegative_prediction(column))


def metric_expressions():
    prediction = F.col("predicted_views")
    error = prediction - F.col(TARGET)
    return [
        F.count("*").alias("rows"),
        F.count(prediction).alias("finite_predictions"),
        F.avg(F.abs(error)).alias("mae"),
        F.sqrt(F.avg(error * error)).alias("rmse"),
        F.avg(error).alias("mean_error"),
    ]


def evaluate(frame):
    score = frame.agg(*metric_expressions()).first().asDict()
    if not score["rows"] or score["finite_predictions"] != score["rows"]:
        raise ValueError(
            f"Métriques invalides, prédictions manquantes ou non finies : {score}"
        )
    return score


def baseline_scores(frame):
    return {
        "Persistence": evaluate(scored_with(frame, F.col("latest_views"))),
        "WeeklyWithPersistenceFallback": evaluate(
            scored_with(frame, F.coalesce("weekly_views", "latest_views"))
        ),
    }


def fit_and_score(name, params, group, features, missing, train, validation):
    start = perf_counter()
    model = model_pipeline(name, params, features, missing).fit(train)
    seconds = perf_counter() - start
    metrics = evaluate(scored_with(model.transform(validation), F.col("prediction")))
    record = {
        "algorithm": name,
        "feature_set": group,
        "parameters": params,
        "fit_seconds": round(seconds, 3),
        "metrics": metrics,
        "features": list(features),
        "imputed_features": list(missing),
    }
    print_manifest("VALIDATION_TRIAL", record)
    return record, model


def performance(trial):
    return trial["metrics"]["mae"], trial["metrics"]["rmse"]


def test_score(trial, model, test):
    return {
        "algorithm": trial["algorithm"],
        "feature_set": trial["feature_set"],
        "parameters": trial["parameters"],
        "fit_seconds": trial["fit_seconds"],
        "metrics": evaluate(scored_with(model.transform(test), F.col("prediction"))),
    }


def feature_layouts(train, count):
    candidates = (*CORE_FEATURES, *EXTRA_FEATURES)
    counts = train.agg(*(F.count(c).alias(c) for c in candidates)).first().asDict()
    layouts = {
        "core": CORE_FEATURES,
        "enriched": tuple(c for c in candidates if counts[c]),
    }
    return {
        name: (columns, tuple(c for c in columns if counts[c] < count))
        for name, columns in layouts.items()
    }


def forecast_diagnostics(scored, train):
    median = train.agg(F.percentile_approx(TARGET, 0.5).alias("median")).first().median
    errors = scored.withColumn("error", F.col("predicted_views") - F.col(TARGET))
    quantiles = (
        errors.select(
            F.percentile_approx("error", [0.01, 0.05, 0.5, 0.95, 0.99]).alias(
                "error_quantiles"
            ),
            F.percentile_approx(F.abs("error"), [0.5, 0.9, 0.95, 0.99]).alias(
                "absolute_error_quantiles"
            ),
        )
        .first()
        .asDict()
    )
    by_volume = (
        errors.withColumn(
            "segment",
            F.when(F.col(TARGET) <= median, "at_or_below_train_median").otherwise(
                "above"
            ),
        )
        .groupBy("segment")
        .agg(*metric_expressions())
        .orderBy("segment")
        .collect()
    )
    by_page = (
        errors.groupBy("page_title")
        .agg(*metric_expressions())
        .orderBy("page_title")
        .collect()
    )
    largest_errors = (
        errors.orderBy(F.desc(F.abs("error")))
        .select(
            "page_title",
            "prediction_date",
            TARGET,
            "predicted_views",
            "latest_views",
            "error",
        )
        .take(10)
    )
    return {
        **quantiles,
        "training_median_target": median,
        "segments": [r.asDict() for r in by_volume],
        "pages": [r.asDict() for r in by_page],
        "largest_errors": [r.asDict() for r in largest_errors],
    }


def run_training(spark, dataset, output, version):
    source = read_manifest(spark, f"{dataset}/manifest")
    partitions = {
        k: v.cache()
        for k, v in split_temporally(spark.read.parquet(f"{dataset}/data")).items()
    }
    train, validation, test = (
        partitions[key] for key in ("train", "validation", "test")
    )
    try:
        splits = {name: split_summary(frame) for name, frame in partitions.items()}
        # Test labels are used only once the validation ranking and search are frozen.
        layouts = feature_layouts(train, splits["train"]["rows"])
        initial_trials = [
            fit_and_score(name, params, group, *layout, train, validation)
            for name, params in INITIAL.items()
            for group, layout in layouts.items()
        ]
        leaders = [
            min(
                (item for item in initial_trials if item[0]["algorithm"] == name),
                key=lambda item: performance(item[0]),
            )
            for name in INITIAL
        ]
        previous, previous_model = min(leaders, key=lambda item: performance(item[0]))
        algorithm, feature_set = previous["algorithm"], previous["feature_set"]
        tuned_trials = [
            fit_and_score(
                algorithm, params, feature_set, *layouts[feature_set], train, validation
            )
            for params in GRID[algorithm]
        ]
        challenger, challenger_model = min(
            tuned_trials, key=lambda item: performance(item[0])
        )
        best, model = min(
            [(previous, previous_model), (challenger, challenger_model)],
            key=lambda item: performance(item[0]),
        )
        validation_baselines = baseline_scores(validation)
        test_baselines = baseline_scores(test)
        winner = scored_with(model.transform(test), F.col("prediction")).cache()
        try:
            last_stage = model.stages[-1]
            importance = (
                dict(
                    zip(
                        best["features"],
                        last_stage.featureImportances.toArray().tolist(),
                    )
                )
                if algorithm != "LinearRegression"
                else None
            )
            metrics = {
                "version": version,
                "dataset_manifest": source,
                "dataset_uri": dataset,
                "model_uri": output,
                "trained_at_utc": datetime.now(timezone.utc).isoformat(),
                "spark_version": spark.version,
                "python_version": sys.version.split()[0],
                "numpy_version": package_version("numpy"),
                "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
                "master": spark.sparkContext.master,
                "seed": SEED,
                "splits": splits,
                "validation_baselines": validation_baselines,
                "validation_initial": [trial for trial, _ in initial_trials],
                "validation_tuning": [trial for trial, _ in tuned_trials],
                "test_baselines": test_baselines,
                "test_initial": [
                    test_score(trial, fitted, test) for trial, fitted in leaders
                ],
                "test_tuned_challenger": test_score(challenger, challenger_model, test),
                "selected": best,
                "selected_test": evaluate(winner),
                "weekly_baseline_availability": {
                    name: frame.where(F.col("weekly_views").isNotNull()).count()
                    for name, frame in partitions.items()
                },
                "test_diagnostics": forecast_diagnostics(winner, train),
                "feature_importance": importance,
            }
            model.write().save(f"{output}/pipeline")
            winner.select(
                "project",
                "page_title",
                "event_date",
                "prediction_date",
                TARGET,
                "latest_views",
                "predicted_views",
            ).write.parquet(f"{output}/test_predictions")
            write_manifest(spark, f"{output}/manifest", metrics)
            print_manifest("WIKIMEDIA_TRAINING", metrics)
        finally:
            winner.unpersist()
    finally:
        for frame in partitions.values():
            frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--output-root", default=MODEL_ROOT)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", args.version):
        parser.error("Version de modèle invalide")
    output = f"{args.output_root.rstrip('/')}/{args.version}"
    spark = start_spark("TrendCast Wikimedia - entraînement et tuning")
    try:
        ensure_new_hdfs_path(spark, output)
        run_training(spark, args.dataset.rstrip("/"), output, args.version)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
