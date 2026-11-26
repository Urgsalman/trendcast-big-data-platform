"""Politique candidate Wikimedia J+1, avec une décision UTC par lot."""

import argparse
import math
from datetime import datetime, timedelta, timezone

from pyspark.ml import PipelineModel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from analytics.ml.features import (
    CORE_FEATURES,
    HISTORY_DAYS,
    HISTORY_SCHEMA,
    calendar_references,
    canonical_observations,
    key_statistics,
    require_schema,
    select_panel,
    temporal_features,
    valid_observation,
)
from analytics.ml.predict_sample import OUTPUT_COLUMNS
from analytics.ml.runtime import (
    GOLD_ROOT,
    KEY,
    SERIES,
    print_manifest,
    read_manifest,
    start_spark,
)
from analytics.ml.train_models import prediction_with_recipe

OUTPUT_EXTENSIONS = (
    "historical_reference",
    "trend_score",
    "trend_direction",
    "trend_rank",
    "latest_observation_date",
    "availability_days_used",
    "fallback_used",
    "eligible_pages_count",
)
DECISION_KEY = (*SERIES, "prediction_date")
OBSERVATION_SCHEMA = {**HISTORY_SCHEMA, "available_at": T.TimestampType()}
PIPELINES = {
    1: ("pipelines/j1_log", "log1p", "RandomForestRegressionModel"),
    2: ("pipelines/j2_v2", "raw", "DecisionTreeRegressionModel"),
}
POLICY_VERSION = "hala-j1-20261126-v1"
REPLAY_CUTOFF = datetime(2026, 11, 22, 23, 59, 59, tzinfo=timezone.utc)
DEMO_FIELDS = (
    *DECISION_KEY,
    "latest_observation_date",
    *CORE_FEATURES,
    "actual_views",
    "predicted_views",
    "absolute_error",
    "historical_reference",
    "trend_direction",
    "trend_score",
    "trend_rank",
    "model_version",
    "route",
)


class ModelPackageMissing(FileNotFoundError):
    """Le paquet nécessaire à l'inférence n'est pas installé."""


def check_package_files(spark, root):
    path_type = spark._jvm.org.apache.hadoop.fs.Path
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    required = ["manifest", *(f"{path}/metadata" for path, _, _ in PIPELINES.values())]
    for name in required:
        path = path_type(f"{root}/{name}")
        if not path.getFileSystem(configuration).exists(path):
            raise ModelPackageMissing(
                f"Paquet {POLICY_VERSION} absent ou incomplet à {root} (manque {name}).\n"
                "Depuis la racine de hala, exécuter :\n"
                "  .\\analytics\\ml\\install_final_model.ps1\n"
                "Installation et transfert du ZIP : "
                "https://github.com/Urgsalman/trendcast-big-data-platform/blob/hala/"
                "analytics/ml/PASSATION_CANDIDAT_J1.md#installation-des-modèles"
            )


def validate_manifest(manifest):
    panel = manifest["panel"]
    rules = manifest["decision"]
    if (
        manifest["schema_version"] != 1
        or manifest["policy_version"] != POLICY_VERSION
        or not panel["project"]
        or not panel["pages"]
        or len(set(panel["pages"])) != len(panel["pages"])
        or tuple(manifest["feature_order"]) != CORE_FEATURES
        or rules["timezone"] != "UTC"
        or rules["history_days"] != HISTORY_DAYS
        or rules["ranking_last_age"] != 2
        or rules["label_delay_days"] != 2
        or rules["direction_band"] != 0.10
        or rules["cutoff"] != "available_at < prediction_timestamp"
        or rules["direction_method"] != "relative_model_prediction"
        or rules["ranking_method"] != "normalized_persistence"
        or set(manifest["routes"]) != {"1", "2"}
    ):
        raise ValueError("Contrat de prévision candidat incompatible")
    for age, (path, transform, _) in PIPELINES.items():
        route = manifest["routes"][str(age)]
        if (
            route["pipeline"] != path
            or not route["model_version"]
            or route["recipe"]["target_transform"] != transform
            or route["recipe"]["weights"] != [1.0, 0.0, 0.0]
        ):
            raise ValueError(f"Recette incompatible pour la route J−{age}")


def forecasts(decision, *, extensions=False):
    """Publier uniquement les prévisions effectives, dans le schéma existant."""
    columns = (*OUTPUT_COLUMNS, *OUTPUT_EXTENSIONS) if extensions else OUTPUT_COLUMNS
    return decision.where(F.col("abstention_reason").isNull()).select(*columns)


def relative_growth(value, reference):
    return F.round(
        (F.col(value) - F.col(reference)) / F.greatest(F.lit(1.0), F.col(reference)), 12
    )


class WikimediaCandidate:
    """Charge les deux estimateurs une fois et produit un journal par décision."""

    def __init__(self, spark, manifest, pipelines):
        self.spark = spark
        self.manifest = manifest
        self.pipelines = pipelines

    @classmethod
    def load(cls, spark, uri):
        root = uri.rstrip("/")
        check_package_files(spark, root)
        manifest = read_manifest(spark, f"{root}/manifest")
        validate_manifest(manifest)
        pipelines = {}
        for age, (path, _, estimator) in PIPELINES.items():
            model = PipelineModel.load(f"{root}/{path}")
            if (
                len(model.stages) != 2
                or type(model.stages[0]).__name__ != "VectorAssembler"
                or tuple(model.stages[0].getInputCols()) != CORE_FEATURES
                or type(model.stages[1]).__name__ != estimator
            ):
                raise ValueError(f"Pipeline Spark incompatible pour la route J−{age}")
            pipelines[age] = model
        return cls(spark, manifest, pipelines)

    def evaluate(self, history, prediction_timestamp):
        """Évaluer le panel complet sur un historique durable, accessible à la coupure."""
        if prediction_timestamp.utcoffset() is None:
            raise ValueError("prediction_timestamp doit inclure un fuseau horaire")
        if self.spark.conf.get("spark.sql.session.timeZone") != "UTC":
            raise ValueError("La session Spark doit utiliser UTC")
        if history.isStreaming:
            raise ValueError("Fournir un instantané batch depuis foreachBatch")
        require_schema(history, OBSERVATION_SCHEMA)
        cutoff = prediction_timestamp.astimezone(timezone.utc)
        ref = cutoff.date()
        day1, day2 = ref - timedelta(days=1), ref - timedelta(days=2)
        panel = self.manifest["panel"]
        references = calendar_references(
            self.spark, panel["project"], panel["pages"], ref, ref
        )
        known = select_panel(history, panel["project"], panel["pages"]).where(
            valid_observation()
            & F.col("event_date").between(ref - timedelta(days=HISTORY_DAYS), day1)
            & (F.col("available_at") >= F.date_add("event_date", 1).cast("timestamp"))
            & (F.col("available_at") < F.lit(cutoff))
        )
        stats = key_statistics(known)
        first = known.groupBy(*KEY).agg(F.min("available_at").alias("available_at"))
        clean = canonical_observations(stats)
        history_info = (
            clean.join(first, list(KEY))
            .groupBy(*SERIES)
            .agg(
                F.max(F.struct("event_date", "available_at")).alias("latest"),
                F.min(F.when(F.col("event_date") == day1, F.col("available_at"))).alias(
                    "j1_available_at"
                ),
                F.min(F.when(F.col("event_date") == day2, F.col("available_at"))).alias(
                    "j2_available_at"
                ),
                F.median(F.when(F.col("event_date") <= day2, F.col("views"))).alias(
                    "historical_reference"
                ),
            )
        )
        conflicts = (
            stats.where(
                F.col("event_date").isin(day1, day2)
                & (F.col("min_views") != F.col("max_views"))
            )
            .groupBy(*SERIES)
            .agg(F.count("*").alias("recent_conflicts"))
        )
        features = temporal_features(references, clean, availability_days=1).select(
            *DECISION_KEY, *CORE_FEATURES
        )
        # Les deux estimateurs n'utilisent que ces quatre colonnes. Quand J−1
        # manque, le dernier total et son âge sont déjà ceux du calcul à J−2.
        scored = []
        for age, model in self.pipelines.items():
            route = self.manifest["routes"][str(age)]
            rows = model.transform(features.where(F.col("latest_age") == age))
            scored.append(
                prediction_with_recipe(rows, route["recipe"])
                .select(*DECISION_KEY, "predicted_views")
                .withColumn("model_version", F.lit(route["model_version"]))
            )
        estimates = scored[0].unionByName(scored[1])
        snapshot = (
            features.join(history_info, list(SERIES), "left")
            .join(conflicts, list(SERIES), "left")
            .join(estimates, list(DECISION_KEY), "left")
        )
        return self._journal(snapshot, cutoff, day1, day2)

    def _journal(self, snapshot, cutoff, day1, day2):
        valid = F.col("predicted_views").isNotNull()
        age = F.col("latest_age")
        score = F.when(valid, relative_growth("latest_views", "historical_reference"))
        volume_growth = relative_growth("predicted_views", "historical_reference")
        band = self.manifest["decision"]["direction_band"]
        trend_direction = (
            F.when(volume_growth < -band, "baisse")
            .when(volume_growth > band, "hausse")
            .when(volume_growth.isNotNull(), "stable")
        )
        reason = (
            F.when(age.isin(1, 2) & ~valid, "invalid_model_prediction")
            .when(
                ~valid & (F.col("recent_conflicts") > 0), "conflicting_recent_receipts"
            )
            .when(~valid & age.isNotNull(), "no_admissible_j1_or_j2")
            .when(~valid, "no_admissible_history")
        )
        group = Window.partitionBy("project", "prediction_date")
        ranked = group.orderBy(F.col("trend_score").desc_nulls_last())
        return (
            snapshot.withColumn("trend_score", score)
            .withColumn("trend_direction", trend_direction)
            .withColumn(
                "trend_rank",
                F.when(F.col("trend_score").isNotNull(), F.dense_rank().over(ranked)),
            )
            .withColumn("eligible_pages_count", F.count("trend_score").over(group))
            .withColumn("latest_observation_date", F.col("latest.event_date"))
            .withColumn("latest_available_at", F.col("latest.available_at"))
            .withColumn("prediction_timestamp", F.lit(cutoff))
            .withColumn("observation_date_j1", F.lit(day1))
            .withColumn("observation_date_j2", F.lit(day2))
            .withColumn("availability_days_used", F.when(valid, age))
            .withColumn("fallback_used", valid & (age == 2))
            .withColumn(
                "route",
                F.when(valid & (age == 1), "J-1")
                .when(valid, "J-2")
                .otherwise("abstention"),
            )
            .withColumn("abstention_reason", reason)
            .withColumn("model_version", F.when(valid, F.col("model_version")))
            .withColumn("policy_version", F.lit(self.manifest["policy_version"]))
            .select(
                *OUTPUT_COLUMNS,
                *OUTPUT_EXTENSIONS,
                *CORE_FEATURES,
                "policy_version",
                "route",
                "abstention_reason",
                "latest_available_at",
                "observation_date_j1",
                "j1_available_at",
                "observation_date_j2",
                "j2_available_at",
            )
        )


def replay_all_j1(spark, policy, gold_uri, cutoff):
    """Simuler des reçus et évaluer des prévisions déjà matérialisées."""
    panel = policy.manifest["panel"]
    gold = select_panel(spark.read.parquet(gold_uri), panel["project"], panel["pages"])
    reference = cutoff.date()
    target = reference + timedelta(days=1)
    past = gold.where(
        F.col("event_date").between(
            reference - timedelta(days=HISTORY_DAYS), reference - timedelta(days=1)
        )
    ).select(
        *HISTORY_SCHEMA,
        F.date_add("event_date", 1).cast("timestamp").alias("available_at"),
    )
    decision = policy.evaluate(past, cutoff)
    # Couper la lignée Spark avant que les labels ne soient introduits.
    predictions = decision.collect()
    if any(row.route != "J-1" for row in predictions):
        raise ValueError(
            "Le Gold ne couvre pas les observations de J−1 requises pour ce replay."
        )
    labels = canonical_observations(
        key_statistics(gold.where(F.col("event_date") == target))
    ).select(
        *SERIES,
        F.col("event_date").alias("prediction_date"),
        F.col("views").alias("actual_views"),
    )
    evaluated = (
        spark.createDataFrame(predictions, decision.schema)
        .join(labels, list(DECISION_KEY), "left")
        .withColumn(
            "absolute_error", F.abs(F.col("predicted_views") - F.col("actual_views"))
        )
    )
    report = evaluated.select(*DEMO_FIELDS).orderBy(*SERIES).collect()
    if len(report) != len(panel["pages"]) or any(
        row.actual_views is None for row in report
    ):
        raise ValueError(
            "Une ou plusieurs valeurs Gold de J+1 sont absentes ou conflictuelles."
        )
    return report


def daily_metrics(rows):
    errors = [row.predicted_views - row.actual_views for row in rows]
    actual_sum = sum(row.actual_views for row in rows)
    mean_actual = actual_sum / len(rows)
    abs_total = sum(abs(error) for error in errors)
    squared = sum(error**2 for error in errors)
    variation = sum((row.actual_views - mean_actual) ** 2 for row in rows)
    return {
        "n": len(rows),
        "mae": abs_total / len(rows),
        "rmse": math.sqrt(squared / len(rows)),
        "r2": 1 - squared / variation if variation else None,
        "wape": abs_total / actual_sum if actual_sum else None,
        "bias": sum(errors) / len(rows),
    }


def show_robustness(spark, policy, package, cutoff):
    schema = T.StructType(
        [T.StructField(name, dtype) for name, dtype in OBSERVATION_SCHEMA.items()]
    )
    history = (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(f"{package}/replay/receipts.csv")
    )
    decisions = policy.evaluate(history, cutoff).persist()
    try:
        decisions.groupBy("route").count().orderBy("route").show()
        decisions.select(
            "page_title",
            "prediction_date",
            "latest_observation_date",
            "route",
            "predicted_views",
            "trend_direction",
            "trend_rank",
            "abstention_reason",
        ).orderBy("route", "trend_rank", "page_title").show(
            len(policy.manifest["panel"]["pages"]), truncate=False
        )
    finally:
        decisions.unpersist()


def main():
    parser = argparse.ArgumentParser(
        description="Démonstration du replay historique avec disponibilités simulées."
    )
    parser.add_argument(
        "--package", required=True, help="Racine HDFS du paquet candidat"
    )
    parser.add_argument(
        "--decision-utc",
        required=True,
        help="Instant ISO 8601 du replay, avec fuseau UTC",
    )
    parser.add_argument(
        "--scenario", choices=("full-j1", "robustness"), default="full-j1"
    )
    parser.add_argument(
        "--gold", default=f"{GOLD_ROOT}/ml_dataset", help="URI du Gold Wikimedia"
    )
    args = parser.parse_args()
    try:
        cutoff = datetime.fromisoformat(args.decision_utc.replace("Z", "+00:00"))
        if cutoff.utcoffset() != timedelta(0):
            raise ValueError
    except ValueError:
        parser.error("--decision-utc doit être un instant ISO 8601 en UTC")
    if cutoff != REPLAY_CUTOFF:
        parser.error("Le replay publié utilise la décision 2026-11-22T23:59:59Z")

    spark = start_spark("TrendCast - démonstration historique J+1")
    try:
        package = args.package.rstrip("/")
        policy = WikimediaCandidate.load(spark, package)
        print_manifest(
            "TRENDCAST_REPLAY",
            {
                "availability": "simulated_historical_replay",
                "scenario": args.scenario,
                "policy": policy.manifest["policy_version"],
                "decision_utc": cutoff.isoformat(),
                "target": cutoff.date() + timedelta(days=1),
            },
        )
        for age, pipeline in policy.pipelines.items():
            estimator = pipeline.stages[-1]
            print_manifest(
                "TRENDCAST_MODEL",
                {
                    "age_days": age,
                    "version": policy.manifest["routes"][str(age)]["model_version"],
                    "estimator": type(estimator).__name__,
                    "parameters": {
                        param.name: value
                        for param, value in estimator.extractParamMap().items()
                    },
                },
            )
        if args.scenario == "robustness":
            show_robustness(spark, policy, package, cutoff)
        else:
            rows = replay_all_j1(spark, policy, args.gold, cutoff)
            n = len(rows)
            display = spark.createDataFrame(rows, list(DEMO_FIELDS)).orderBy(*SERIES)
            display.select(
                *DECISION_KEY, "latest_observation_date", *CORE_FEATURES
            ).show(n, truncate=False)
            display.select(
                "page_title",
                "actual_views",
                "predicted_views",
                "absolute_error",
                "historical_reference",
                "trend_direction",
                "trend_score",
                "trend_rank",
                "model_version",
                "route",
            ).show(n, truncate=False)
            print_manifest(
                "TRENDCAST_ROUTES",
                {"requested": n, "J-1": n, "J-2": 0, "abstention": 0, "coverage": 1.0},
            )
            print_manifest("TRENDCAST_DAILY_METRICS", daily_metrics(rows))
            print_manifest("TRENDCAST_PREDICTIONS", [row.asDict() for row in rows])
    except ModelPackageMissing as error:
        parser.exit(2, f"{error}\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
