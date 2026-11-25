"""Recharge un modèle et vérifie le contrat d'inférence sur les données réelles."""

import argparse
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.ml.features import (
    HISTORY_DAYS,
    HISTORY_SCHEMA,
    calendar_references,
    canonical_observations,
    key_statistics,
    require_schema,
    select_panel,
    temporal_features,
)
from analytics.ml.runtime import SERIES, print_manifest, read_manifest, start_spark
from analytics.ml.train_models import finite, prediction_with_recipe

OUTPUT_COLUMNS = (
    *SERIES,
    "prediction_date",
    "predicted_views",
    "model_version",
    "prediction_timestamp",
)


def load_forecaster(spark, model_uri):
    model_uri = model_uri.rstrip("/")
    manifest = read_manifest(spark, f"{model_uri}/manifest")
    return PipelineModel.load(f"{model_uri}/pipeline"), manifest


def forecast_for_reference(
    spark,
    model,
    manifest,
    history,
    reference_date,
    *,
    prediction_timestamp=None,
):
    """Score the saved panel using receipts known before the forecast deadline.

    Each incoming observation needs a receipt time in UTC (available_at).
    Use the same reference date for a batch. Return no row for a page lacking
    an eligible observation. Schedule the call before the target begins.
    """
    require_schema(history, {**HISTORY_SCHEMA, "available_at": T.TimestampType()})
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("La session Spark d'inférence doit utiliser UTC")
    policy = manifest["dataset_manifest"]
    project, pages = policy["project"], policy["pages"]
    submitted = prediction_timestamp or datetime.now(timezone.utc)
    if submitted.utcoffset() is None:
        raise ValueError("prediction_timestamp doit inclure un fuseau horaire")
    deadline = datetime.combine(
        reference_date + timedelta(days=1), time.min, timezone.utc
    )
    if submitted >= deadline:
        raise ValueError("L'instant de décision doit précéder le début du jour cible")
    first_day = reference_date - timedelta(days=HISTORY_DAYS)
    last_day = reference_date - timedelta(days=policy["availability_days"])
    available = select_panel(history, project, pages).where(
        F.col("event_date").between(first_day, last_day)
        & (F.col("available_at") < F.lit(submitted))
    )
    clean = canonical_observations(key_statistics(available))
    references = calendar_references(
        spark, project, pages, reference_date, reference_date
    )
    features = temporal_features(references, clean, policy["availability_days"]).where(
        F.col("latest_views").isNotNull()
    )
    return (
        prediction_with_recipe(model.transform(features), manifest.get("selected"))
        .where(F.col("predicted_views").isNotNull())
        .withColumn("model_version", F.lit(manifest["version"]))
        .withColumn("prediction_timestamp", F.lit(submitted))
        .select(*OUTPUT_COLUMNS)
    )


def verify_saved_model(spark, model_uri, *, gold_uri=None, reference_predictions=None):
    model, manifest = load_forecaster(spark, model_uri)
    policy = manifest["dataset_manifest"]
    stored = spark.read.parquet(
        reference_predictions or f"{model_uri}/test_predictions"
    )
    target_day = stored.agg(F.max("prediction_date").alias("day")).first().day
    reference = target_day - timedelta(days=1)
    gold = spark.read.parquet(gold_uri or policy["gold"]).where(
        F.col("event_date").between(policy["first_input_day"], policy["last_input_day"])
    )
    # Replay the backtest policy. Production uses recorded receipt timestamps.
    replay = gold.withColumn(
        "available_at",
        F.date_add("event_date", policy["availability_days"]).cast("timestamp"),
    )
    submitted = datetime.combine(target_day, time.min, timezone.utc) - timedelta(
        seconds=1
    )
    predictions = forecast_for_reference(
        spark, model, manifest, replay, reference, prediction_timestamp=submitted
    )
    expected = stored.where(F.col("prediction_date") == target_day).select(
        *SERIES, "prediction_date", F.col("predicted_views").alias("expected")
    )
    joined = predictions.join(expected, [*SERIES, "prediction_date"], "full").cache()
    try:
        valid = finite(F.col("predicted_views")) & finite(F.col("expected"))
        deviation = F.abs(F.col("predicted_views") - F.col("expected"))
        stats = (
            joined.agg(
                F.count("*").alias("rows"),
                F.count(F.when(valid, 1)).alias("pairs_with_finite_predictions"),
                F.max(deviation).alias("max_absolute_difference"),
            )
            .first()
            .asDict()
        )
        if (
            not stats["rows"]
            or stats["pairs_with_finite_predictions"] != stats["rows"]
            or stats["max_absolute_difference"] > 1e-8
        ):
            raise RuntimeError(f"Échec de la parité après rechargement : {stats}")
        print_manifest(
            "WIKIMEDIA_RELOAD",
            {
                "model_version": manifest["version"],
                "model_uri": model_uri,
                "target_date": target_day,
                **stats,
            },
        )
        predictions.orderBy(*SERIES).show(5, truncate=False)
    finally:
        joined.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--load-only", action="store_true")
    parser.add_argument(
        "--gold", help="Entrée Gold de référence pour le test de parité"
    )
    parser.add_argument(
        "--reference-predictions", help="Prédictions de test déjà produites"
    )
    args = parser.parse_args()
    if args.load_only and (args.gold or args.reference_predictions):
        parser.error("--load-only ne requiert ni Gold ni prédictions de référence")
    model_uri = args.model.rstrip("/")
    spark = start_spark("TrendCast Wikimedia - rechargement et parité")
    try:
        if args.load_only:
            model, manifest = load_forecaster(spark, model_uri)
            print_manifest(
                "WIKIMEDIA_MODEL_LOADED",
                {
                    "model_version": manifest["version"],
                    "model_uri": model_uri,
                    "stages": [type(stage).__name__ for stage in model.stages],
                    "features": manifest["selected"]["features"],
                },
            )
        else:
            verify_saved_model(
                spark,
                model_uri,
                gold_uri=args.gold,
                reference_predictions=args.reference_predictions,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
