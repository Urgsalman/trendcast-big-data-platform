"""Entraîne et évalue des régressions Spark ML en ordre chronologique."""

import argparse
import re
import sys
from datetime import date, datetime, timedelta, timezone
from importlib.metadata import version as package_version
from pathlib import Path
from time import perf_counter

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.ml.features import CORE_FEATURES, FEATURE_GROUPS
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
    "GBTRegressor": {
        "maxIter": 8,
        "maxDepth": 3,
        "stepSize": 0.15,
        "minInstancesPerNode": 10,
    },
}
V1_TREE = {"maxDepth": 7, "minInstancesPerNode": 5, "maxBins": 32}
GRID = {
    "LinearRegression": [
        {
            "regParam": 0.01,
            "elasticNetParam": 0,
            "maxIter": 60,
            "standardization": True,
        },
        {
            "regParam": 0.1,
            "elasticNetParam": 0.25,
            "maxIter": 80,
            "standardization": True,
        },
        {
            "regParam": 0.1,
            "elasticNetParam": 0,
            "maxIter": 50,
            "standardization": False,
        },
    ],
    "DecisionTreeRegressor": [
        {"maxDepth": 4, "minInstancesPerNode": 12, "maxBins": 32, "minInfoGain": 0.0},
        {"maxDepth": 7, "minInstancesPerNode": 5, "maxBins": 48, "minInfoGain": 5.0},
        {"maxDepth": 9, "minInstancesPerNode": 8, "maxBins": 64, "minInfoGain": 25.0},
    ],
    "RandomForestRegressor": [
        {
            "numTrees": 12,
            "maxDepth": 4,
            "minInstancesPerNode": 15,
            "featureSubsetStrategy": "sqrt",
            "subsamplingRate": 0.8,
        },
        {
            "numTrees": 24,
            "maxDepth": 6,
            "minInstancesPerNode": 8,
            "featureSubsetStrategy": "onethird",
            "subsamplingRate": 0.9,
        },
        {
            "numTrees": 20,
            "maxDepth": 7,
            "minInstancesPerNode": 5,
            "featureSubsetStrategy": "all",
            "subsamplingRate": 1.0,
        },
    ],
    "GBTRegressor": [
        {
            "maxIter": 8,
            "maxDepth": 2,
            "stepSize": 0.2,
            "subsamplingRate": 0.85,
            "minInstancesPerNode": 15,
            "maxBins": 32,
        },
        {
            "maxIter": 12,
            "maxDepth": 3,
            "stepSize": 0.1,
            "subsamplingRate": 0.9,
            "minInstancesPerNode": 10,
            "maxBins": 48,
        },
        {
            "maxIter": 10,
            "maxDepth": 4,
            "stepSize": 0.15,
            "subsamplingRate": 1.0,
            "minInstancesPerNode": 8,
            "maxBins": 32,
        },
    ],
}
ALGORITHMS = {
    "LinearRegression": LinearRegression,
    "DecisionTreeRegressor": DecisionTreeRegressor,
    "RandomForestRegressor": RandomForestRegressor,
    "GBTRegressor": GBTRegressor,
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


def model_pipeline(name, params, features, missing, label=TARGET):
    settings = dict(params, labelCol=label)
    if name != "LinearRegression":
        settings["seed"] = SEED
    inputs = [
        "page_vector" if c == "page_title" else f"imputed_{c}" if c in missing else c
        for c in features
    ]
    stages = []
    if missing:
        stages.append(
            Imputer(
                strategy="median",
                inputCols=list(missing),
                outputCols=[f"imputed_{c}" for c in missing],
            )
        )
    if "page_title" in features:
        stages.extend(
            [
                StringIndexer(
                    inputCol="page_title", outputCol="page_index", handleInvalid="keep"
                ),
                OneHotEncoder(
                    inputCol="page_index",
                    outputCol="page_vector",
                    handleInvalid="keep",
                    dropLast=False,
                ),
            ]
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


def seasonal():
    return F.coalesce("weekly_views", "latest_views")


def fit_target(frame, transform):
    if transform == "log1p":
        return frame.withColumn("fit_target", F.log1p(TARGET))
    if transform == "residual":
        return frame.withColumn("fit_target", F.col(TARGET) - seasonal())
    return frame


def prediction_with_recipe(frame, recipe=None):
    recipe = recipe or {}
    raw = F.col("prediction")
    transform = recipe.get("target_transform", "raw")
    if transform == "log1p":
        raw = F.expm1(raw)
    elif transform == "residual":
        raw = seasonal() + raw
    model, weekly, persistence = recipe.get("weights", [1.0, 0.0, 0.0])
    if weekly or persistence:
        raw = model * raw + weekly * seasonal() + persistence * F.col("latest_views")
    return scored_with(frame, raw)


def metric_expressions():
    prediction = F.col("predicted_views")
    error = prediction - F.col(TARGET)
    absolute = F.abs(error)
    target_total = F.sum(TARGET)
    variance = F.var_pop(TARGET)
    return [
        F.count("*").alias("rows"),
        F.count(prediction).alias("finite_predictions"),
        F.avg(absolute).alias("mae"),
        F.sqrt(F.avg(error * error)).alias("rmse"),
        F.avg(error).alias("mean_error"),
        F.when(target_total > 0, F.sum(absolute) / target_total).alias("wape"),
        F.when(variance > 0, 1 - F.avg(error * error) / variance).alias("r2"),
        F.percentile_approx(absolute, 0.5).alias("median_abs_error"),
        F.percentile_approx(absolute, 0.95).alias("p95_abs_error"),
    ]


def evaluate(frame):
    score = frame.agg(*metric_expressions()).first().asDict()
    if not score["rows"] or score["finite_predictions"] != score["rows"]:
        raise ValueError(
            f"Métriques invalides, prédictions manquantes ou non finies : {score}"
        )
    score["coverage"] = score["finite_predictions"] / score["rows"]
    return score


def baseline_scores(frame):
    return {
        "Persistence": evaluate(scored_with(frame, F.col("latest_views"))),
        "WeeklyWithPersistenceFallback": evaluate(scored_with(frame, seasonal())),
    }


def walk_forward_bounds(retrospective_start, availability_days, window_days=42):
    # Selection itself also runs before the first retrospective prediction.
    end = retrospective_start - timedelta(days=availability_days)
    return [
        (
            end - timedelta(days=window_days * remaining),
            end - timedelta(days=window_days * (remaining - 1)),
        )
        for remaining in (3, 2, 1)
    ]


def eligible_training(frame, cutoff):
    return frame.where(F.col("label_available_day") < F.lit(cutoff))


def configuration(
    name, feature_set, parameters, *, transform="raw", weights=(1.0, 0.0, 0.0)
):
    return {
        "algorithm": name,
        "feature_set": feature_set,
        "parameters": parameters,
        "target_transform": transform,
        "weights": list(weights),
    }


def feature_sets():
    def add(groups):
        return tuple(
            dict.fromkeys(
                (*CORE_FEATURES, *(c for g in groups for c in FEATURE_GROUPS[g]))
            )
        )

    return {
        "core": CORE_FEATURES,
        **{g: add(g) for g in FEATURE_GROUPS},
        "all": add(FEATURE_GROUPS),
        **{
            f"without_{g}": add(other for other in FEATURE_GROUPS if other != g)
            for g in FEATURE_GROUPS
        },
    }


def feature_counts(train):
    candidates = tuple(dict.fromkeys(c for fs in feature_sets().values() for c in fs))
    return (
        train.agg(
            F.count("*").alias("rows"), *(F.count(c).alias(c) for c in candidates)
        )
        .first()
        .asDict()
    )


def fit_candidate(config, train, counts):
    candidates = feature_sets()[config["feature_set"]]
    features = tuple(c for c in candidates if counts[c])
    if not features:
        raise ValueError("Aucune variable utilisable dans la fenêtre d'entraînement")
    missing = tuple(
        c for c in features if c != "page_title" and counts[c] < counts["rows"]
    )
    transform = config["target_transform"]
    timed = perf_counter()
    model = model_pipeline(
        config["algorithm"],
        config["parameters"],
        features,
        missing,
        label=TARGET if transform == "raw" else "fit_target",
    ).fit(fit_target(train, transform))
    return model, {
        "features": list(features),
        "imputed_features": list(missing),
        "fit_seconds": round(perf_counter() - timed, 3),
    }


def balanced_score(metrics, reference):
    return (
        metrics["mae"] / max(reference["mae"], 1e-9)
        + metrics["rmse"] / max(reference["rmse"], 1e-9)
    ) / 2


def evaluate_model(frame, model, config):
    start = perf_counter()
    score = evaluate(prediction_with_recipe(model.transform(frame), config))
    return {"metrics": score, "score_seconds": round(perf_counter() - start, 3)}


class ValidationFold:
    def __init__(self, frame, start, end):
        self.start = start
        self.end = end
        self.train = eligible_training(frame, start).cache()
        self.validation = frame.where(
            (F.col("prediction_date") >= start) & (F.col("prediction_date") < end)
        ).cache()
        self.summary = {
            "cutoff_exclusive": start,
            "validation_end_exclusive": end,
            "train": split_summary(self.train),
            "validation": split_summary(self.validation),
        }
        self.counts = feature_counts(self.train)
        if not self.counts["rows"] or not self.summary["validation"]["rows"]:
            raise ValueError(f"Fenêtre vide : {self.summary}")
        self.baselines = baseline_scores(self.validation)
        self.fits = {}
        self.trials = {}
        self.reference = self.evaluate(
            "v1_core", configuration("DecisionTreeRegressor", "core", V1_TREE)
        )

    def evaluate(self, name, config):
        if name not in self.trials:
            signature = (
                config["algorithm"],
                config["feature_set"],
                tuple(sorted(config["parameters"].items())),
                config["target_transform"],
            )
            if signature not in self.fits:
                self.fits[signature] = fit_candidate(config, self.train, self.counts)
            model, fit = self.fits[signature]
            record = {
                "id": name,
                "fold": self.start,
                **config,
                **fit,
                **evaluate_model(self.validation, model, config),
            }
            self.trials[name] = record
            print_manifest("WALK_FORWARD_TRIAL", record)
        return self.trials[name]

    def close(self):
        self.train.unpersist()
        self.validation.unpersist()


def screen_candidates(fold):
    """Select a bounded shortlist on the last development window."""
    configurations = {
        "v1_core": configuration("DecisionTreeRegressor", "core", V1_TREE),
    }

    def add(name, config):
        configurations[name] = config
        return fold.evaluate(name, config)

    def order(names):
        return sorted(
            names,
            key=lambda n: (
                balanced_score(fold.trials[n]["metrics"], fold.reference["metrics"]),
                n,
            ),
        )

    ablation = [f"ablation_{fs}" for fs in feature_sets() if fs != "core"]
    for name in ablation:
        add(
            name,
            configuration(
                "DecisionTreeRegressor", name.removeprefix("ablation_"), V1_TREE
            ),
        )
    best_group = configurations[order(ablation)[0]]["feature_set"]

    leaders = []
    for algorithm, parameters in INITIAL.items():
        tried = []
        for group in ("core", best_group):
            name = f"{algorithm}_{group}_initial"
            add(name, configuration(algorithm, group, parameters))
            tried.append(name)
        best_layout = configurations[order(tried)[0]]["feature_set"]
        for index, parameters in enumerate(GRID[algorithm], 1):
            name = f"{algorithm}_tune_{index}"
            add(name, configuration(algorithm, best_layout, parameters))
            tried.append(name)
        leaders.append(order(tried)[0])

    best_raw = order(leaders)[0]
    transforms = {}
    for transform in ("log1p", "residual"):
        tried = []
        for base in order(leaders)[:2]:
            name = f"{base}_{transform}"
            add(name, {**configurations[base], "target_transform": transform})
            tried.append(name)
        transforms[transform] = order(tried)[0]

    blends = {}
    for kind, weights in {
        "weekly_blend": [(w, 1 - w, 0.0) for w in (0.25, 0.5, 0.75)],
        "three_way": [(0.25, 0.5, 0.25), (0.5, 0.25, 0.25), (0.75, 0.125, 0.125)],
    }.items():
        tried = []
        for index, weight in enumerate(weights, 1):
            name = f"{kind}_{index}"
            add(name, {**configurations[best_raw], "weights": list(weight)})
            tried.append(name)
        blends[kind] = order(tried)[0]
    finalists = list(
        dict.fromkeys(["v1_core", *leaders, *transforms.values(), *blends.values()])
    )
    return configurations, finalists


def compare_folds(folds, configurations, finalists):
    summaries = []
    for name in finalists:
        trials = [fold.evaluate(name, configurations[name]) for fold in folds]
        ratios = [
            balanced_score(trial["metrics"], fold.reference["metrics"])
            for trial, fold in zip(trials, folds)
        ]
        maes = [trial["metrics"]["mae"] for trial in trials]
        rmses = [trial["metrics"]["rmse"] for trial in trials]
        value = sum(ratios) / len(ratios)
        summaries.append(
            {
                "id": name,
                **configurations[name],
                "folds": trials,
                "mean_balanced_score": value,
                "balanced_std": (sum((r - value) ** 2 for r in ratios) / len(ratios))
                ** 0.5,
                "windows_improved": sum(r < 1 for r in ratios),
                "mean_mae": sum(maes) / len(maes),
                "mean_rmse": sum(rmses) / len(rmses),
                "mae_std": (
                    sum((r - sum(maes) / len(maes)) ** 2 for r in maes) / len(maes)
                )
                ** 0.5,
                "rmse_std": (
                    sum((r - sum(rmses) / len(rmses)) ** 2 for r in rmses) / len(rmses)
                )
                ** 0.5,
            }
        )
    return sorted(summaries, key=lambda row: (row["mean_balanced_score"], row["id"]))


def forecast_diagnostics(scored, train):
    cutoffs = train.agg(
        F.percentile_approx(TARGET, [0.5, 0.9]).alias("traffic"),
        F.percentile_approx("relative_volatility_28", [0.5, 0.9]).alias("volatility"),
    ).first()

    def segment(column, thresholds):
        return (
            F.when(column <= thresholds[0], "low")
            .when(column <= thresholds[1], "mid")
            .otherwise("high")
        )

    by = {
        "page": F.col("page_title"),
        "weekday": F.dayofweek("prediction_date"),
        "month": F.date_format("prediction_date", "yyyy-MM"),
        "traffic": segment(F.col(TARGET), cutoffs.traffic),
        "volatility": segment(F.col("relative_volatility_28"), cutoffs.volatility),
    }
    errors = scored.withColumn("error", F.col("predicted_views") - F.col(TARGET))
    result = {
        key: [
            row.asDict()
            for row in errors.groupBy(expr.alias("group"))
            .agg(*metric_expressions())
            .orderBy("group")
            .collect()
        ]
        for key, expr in by.items()
    }
    result["thresholds"] = cutoffs.asDict()
    result["residuals"] = (
        errors.agg(
            F.percentile_approx("error", [0.01, 0.05, 0.5, 0.95, 0.99]).alias(
                "quantiles"
            ),
            F.count(F.when(F.col("error") < 0, 1)).alias("under"),
            F.count(F.when(F.col("error") > 0, 1)).alias("over"),
        )
        .first()
        .asDict()
    )
    result["largest_errors"] = [
        row.asDict()
        for row in errors.orderBy(F.desc(F.abs("error")))
        .select(
            "page_title",
            "prediction_date",
            TARGET,
            "predicted_views",
            "latest_views",
            "error",
        )
        .take(10)
    ]
    return result


def attach_reference(frame, original):
    keys = ["project", "page_title", "prediction_date"]
    reference = original.select(
        *keys,
        F.col("predicted_views").alias("v1_prediction"),
        F.col(TARGET).alias("v1_label"),
    )
    joined = frame.join(reference, keys, "full")
    if (
        joined.where(
            F.col(TARGET).isNull()
            | F.col("v1_label").isNull()
            | (F.col(TARGET) != F.col("v1_label"))
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Le périmètre rétrospectif et celui de la v1 diffèrent")
    return joined.drop("v1_label")


def run_training(spark, dataset, output, version, retrospective_start, reference_model):
    source = read_manifest(spark, f"{dataset}/manifest")
    delay = source["availability_days"]
    rows = spark.read.parquet(f"{dataset}/data").cache()
    folds = [
        ValidationFold(rows, start, end)
        for start, end in walk_forward_bounds(retrospective_start, delay)
    ]
    try:
        configs, finalists = screen_candidates(folds[-1])
        comparisons = compare_folds(folds, configs, finalists)
        reference = next(x for x in comparisons if x["id"] == "v1_core")
        weekly_mae = sum(
            f.baselines["WeeklyWithPersistenceFallback"]["mae"] for f in folds
        ) / len(folds)
        qualified = [
            t
            for t in comparisons
            if t["windows_improved"] >= 2
            and t["mean_balanced_score"] <= 0.98
            and t["mean_mae"] <= weekly_mae
            and t["mean_rmse"] <= reference["mean_rmse"]
        ]
        chosen = (
            qualified[0]
            if qualified
            else next(x for x in comparisons if x["id"] != "v1_core")
        )
        config = configs[chosen["id"]]
        print_manifest(
            "WIKIMEDIA_SELECTION_FROZEN",
            {"selected": chosen["id"], "promoted": bool(qualified)},
        )
        # The retrospective labels are read for evaluation only after the choice is frozen.
        refit = eligible_training(rows, retrospective_start).cache()
        counts = feature_counts(refit)
        retrospective = attach_reference(
            rows.where(F.col("prediction_date") >= retrospective_start),
            spark.read.parquet(f"{reference_model}/test_predictions"),
        ).cache()
        try:
            candidate, fitted = fit_candidate(config, refit, counts)
            winner = prediction_with_recipe(
                candidate.transform(retrospective), config
            ).cache()
            try:
                refit_models = {}
                for name in finalists:
                    if name == chosen["id"]:
                        refit_models[name] = {"metrics": evaluate(winner), **fitted}
                    else:
                        model, fit = fit_candidate(configs[name], refit, counts)
                        refit_models[name] = {
                            **evaluate_model(retrospective, model, configs[name]),
                            **fit,
                        }
                original = scored_with(retrospective, F.col("v1_prediction"))
                importance = None
                if config["algorithm"] != "LinearRegression":
                    transformed = candidate.transform(refit.limit(1))
                    attrs = (
                        transformed.schema["features"]
                        .metadata.get("ml_attr", {})
                        .get("attrs", {})
                    )
                    names = {
                        a["idx"]: a["name"] for group in attrs.values() for a in group
                    }
                    importance = {
                        names.get(i, f"feature_{i}"): float(v)
                        for i, v in enumerate(
                            candidate.stages[-1].featureImportances.toArray()
                        )
                    }
                metrics = {
                    "version": version,
                    "dataset_manifest": source,
                    "dataset_uri": dataset,
                    "model_uri": output if qualified else reference_model,
                    "reference_model": reference_model,
                    "deployment_approved": bool(qualified),
                    "selected": {"id": chosen["id"], **config, **fitted},
                    "trained_at_utc": datetime.now(timezone.utc).isoformat(),
                    "spark_version": spark.version,
                    "python_version": sys.version.split()[0],
                    "numpy_version": package_version("numpy"),
                    "java_version": spark._jvm.java.lang.System.getProperty(
                        "java.version"
                    ),
                    "master": spark.sparkContext.master,
                    "seed": SEED,
                    "folds": [{**f.summary, "baselines": f.baselines} for f in folds],
                    "screening": list(folds[-1].trials.values()),
                    "walk_forward": comparisons,
                    "refit_window": split_summary(refit),
                    "retrospective_window": split_summary(retrospective),
                    "retrospective_status": "known historical test, used for comparison only",
                    "retrospective_baselines": baseline_scores(retrospective),
                    "retrospective_v1": evaluate(original),
                    "retrospective_candidates": refit_models,
                    "candidate_diagnostics": forecast_diagnostics(winner, refit),
                    "v1_diagnostics": forecast_diagnostics(original, refit),
                    "feature_importance": importance,
                    "weekly_validation_mae": weekly_mae,
                }
                if qualified:
                    candidate.write().save(f"{output}/pipeline")
                winner.select(
                    "project",
                    "page_title",
                    "event_date",
                    "prediction_date",
                    TARGET,
                    "latest_views",
                    "weekly_views",
                    "v1_prediction",
                    "predicted_views",
                ).write.parquet(f"{output}/test_predictions")
                write_manifest(spark, f"{output}/manifest", metrics)
                print_manifest("WIKIMEDIA_TRAINING", metrics)
            finally:
                winner.unpersist()
        finally:
            retrospective.unpersist()
            refit.unpersist()
    finally:
        for fold in folds:
            fold.close()
        rows.unpersist()


def evaluate_frozen_model(spark, dataset, model_uri, after):
    model_manifest = read_manifest(spark, f"{model_uri}/manifest")
    if not model_manifest.get("deployment_approved", True):
        raise ValueError("Cette version n'a pas été retenue pour l'inférence")
    last = (
        model_manifest.get("retrospective_window") or model_manifest["splits"]["test"]
    )
    if after < date.fromisoformat(last["last_target_date"]):
        raise ValueError("La date minimale doit exclure toute la période déjà utilisée")
    dataset_manifest = read_manifest(spark, f"{dataset}/manifest")
    fields = ("project", "pages", "availability_days")
    if any(
        dataset_manifest[k] != model_manifest["dataset_manifest"][k] for k in fields
    ):
        raise ValueError(
            "Le panel et la règle de disponibilité doivent correspondre au modèle"
        )
    frame = spark.read.parquet(f"{dataset}/data").where(
        F.col("prediction_date") > after
    )
    model = PipelineModel.load(f"{model_uri}/pipeline")
    predictions = prediction_with_recipe(
        model.transform(frame), model_manifest["selected"]
    )
    print_manifest(
        "WIKIMEDIA_FROZEN_EVALUATION",
        {
            "version": model_manifest["version"],
            "window": split_summary(frame),
            "baselines": baseline_scores(frame),
            "model": evaluate(predictions),
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--output-root", default=MODEL_ROOT)
    parser.add_argument("--version")
    parser.add_argument(
        "--retrospective-start", type=date.fromisoformat, default=date(2026, 9, 30)
    )
    parser.add_argument("--reference-model", default=f"{MODEL_ROOT}/hala-20261125-v1")
    parser.add_argument(
        "--evaluate-model", help="Modèle figé à évaluer sur de nouvelles dates"
    )
    parser.add_argument(
        "--after", type=date.fromisoformat, help="Dernière date cible déjà exposée"
    )
    args = parser.parse_args()
    if args.evaluate_model and (args.after is None or args.version):
        parser.error("--evaluate-model exige --after et ne crée pas de version")
    if not args.evaluate_model and not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9._-]*", args.version or ""
    ):
        parser.error("Version de modèle invalide")
    spark = start_spark("TrendCast Wikimedia - entraînement et tuning")
    try:
        dataset = args.dataset.rstrip("/")
        if args.evaluate_model:
            evaluate_frozen_model(
                spark, dataset, args.evaluate_model.rstrip("/"), args.after
            )
        else:
            output = f"{args.output_root.rstrip('/')}/{args.version}"
            ensure_new_hdfs_path(spark, output)
            run_training(
                spark,
                dataset,
                output,
                args.version,
                args.retrospective_start,
                args.reference_model.rstrip("/"),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
