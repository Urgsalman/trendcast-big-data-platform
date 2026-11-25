"""Vérifie le contrat Wikimedia sur de petites fixtures Spark distribuées."""

import argparse
import math
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import pstdev

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from analytics.ml.features import (
    attach_labels,
    canonical_observations,
    key_statistics,
    temporal_features,
)
from analytics.ml.predict_sample import forecast_for_reference
from analytics.ml.runtime import (
    TARGET,
    ensure_new_hdfs_path,
    print_manifest,
    start_spark,
)
from analytics.ml.train_models import (
    GRID,
    INITIAL,
    configuration,
    eligible_training,
    evaluate,
    evaluate_model,
    fit_candidate,
    prediction_with_recipe,
    scored_with,
    split_summary,
    walk_forward_bounds,
)


def verify_calendar(spark):
    def day(number):
        return date(2026, 1, number)

    reference = day(12)
    events = spark.createDataFrame(
        [
            ("en.wikipedia", "A", day(n), v)
            for n, v in (
                (5, 70),
                (6, 60),
                (8, 80),
                (10, 100),
                (10, 100),
                (11, 500),
                (12, 600),
                (13, 130),
                (15, 200),
                (15, 250),
                (20, 200),
                (21, -1),
            )
        ]
        + [("en.wikipedia", "B", day(10), 900), ("fr.wikipedia", "A", day(10), 800)],
        "project string, page_title string, event_date date, views long",
    )
    try:
        key_statistics(events.withColumn("views", F.col("views").cast("string")))
    except ValueError:
        pass
    else:
        raise AssertionError("Le schéma doit refuser les vues de type string")
    clean = canonical_observations(key_statistics(events)).cache()
    try:
        assert clean.where("project = 'en.wikipedia' and page_title = 'A'").count() == 8
        references = spark.createDataFrame(
            [
                ("en.wikipedia", "A", reference),
                ("en.wikipedia", "A", day(13)),
                ("en.wikipedia", "B", reference),
                ("fr.wikipedia", "A", reference),
            ],
            "project string, page_title string, event_date date",
        )
        indexed = {
            (row.project, row.page_title, row.event_date): row
            for row in attach_labels(
                temporal_features(references, clean, 2), clean
            ).collect()
        }
        record = indexed["en.wikipedia", "A", reference]
        assert record.prediction_date == day(13) and record[TARGET] == 130
        assert record.latest_views == 100 and record.latest_age == 2
        assert record.views_lag_7 == 70 and record.weekly_views == 60
        assert record.views_lag_1 is None and record.views_lag_14 is None
        assert record.observed_days_7 == 4 and record.mean_7 == 77.5
        assert math.isclose(record.std_7, pstdev([70, 60, 80, 100]))
        assert record.views_lag_3 is None and record.change_1 is None
        assert record.acceleration is None
        assert record.mean_3 == 90 and record.median_7 == 70
        assert record.same_weekday_count == 1 and record.same_weekday_mean == 60
        assert record.observed_days_28 == 4 and math.isclose(record.mean_28, 77.5)
        assert indexed["en.wikipedia", "A", day(13)][TARGET] is None
        assert indexed["en.wikipedia", "B", reference].latest_views == 900
        assert indexed["fr.wikipedia", "A", reference].latest_views == 800
    finally:
        clean.unpersist()


def verify_training(spark, path):
    synthetic = (
        spark.range(80)
        .select(
            F.date_add(
                F.lit("2026-01-01").cast("date"), (F.col("id") / 4).cast("int")
            ).alias("prediction_date"),
            (F.col("id") % 20).cast("double").alias("latest_views"),
            F.lit(2).alias("latest_age"),
            F.lit(1.0).alias("weekday_sin"),
            F.lit(0.0).alias("weekday_cos"),
            (2 * (F.col("id") % 20) + 3).alias(TARGET),
        )
        .withColumn("label_available_day", F.date_add("prediction_date", 2))
    )
    cutoff = date(2026, 1, 15)
    train = eligible_training(synthetic, cutoff)
    validation = synthetic.where(
        F.col("prediction_date").between(cutoff, date(2026, 1, 17))
    )
    test = synthetic.where(F.col("prediction_date") >= date(2026, 1, 18))
    train_info, val_info, test_info = map(split_summary, (train, validation, test))
    assert train_info["last_target_date"] < val_info["first_target_date"]
    assert val_info["last_target_date"] < test_info["first_target_date"]
    assert [s["rows"] for s in (train_info, val_info, test_info)] == [48, 12, 12]
    assert train.agg(F.max("label_available_day")).first()[0] < cutoff
    bounds = walk_forward_bounds(date(2026, 9, 30), 2)
    assert len(bounds) == 3 and all(
        end - start == timedelta(days=42) for start, end in bounds
    )
    assert bounds[-1][1] + timedelta(days=2) == date(2026, 9, 30)

    def fit(name, parameters):
        config = configuration(name, "core", parameters)
        counts = dict.fromkeys(
            ("latest_views", "latest_age", "weekday_sin", "weekday_cos", "rows"), 48
        )
        model, fit_details = fit_candidate(config, train, counts)
        return {
            **config,
            **fit_details,
            **evaluate_model(validation, model, config),
        }, model

    candidates = [fit(name, params) for name, params in INITIAL.items()]
    previous = min(candidates, key=lambda item: item[0]["metrics"]["mae"])
    assert previous[0]["metrics"]["mae"] < 1e-7
    challenger = fit("LinearRegression", GRID["LinearRegression"][0])
    assert challenger[0]["parameters"] != previous[0]["parameters"]
    selected, model = min(
        (previous, challenger), key=lambda item: item[0]["metrics"]["mae"]
    )
    assert selected["metrics"]["rows"] == 12
    model.write().save(path)
    loaded = PipelineModel.load(path)
    scores = evaluate(scored_with(loaded.transform(test), F.col("prediction")))
    assert scores["rmse"] < 1e-7 and scores["finite_predictions"] == 12
    exact = spark.createDataFrame([(1, 4.0), (4, 0.0)], [TARGET, "predicted_views"])
    values = evaluate(exact)
    assert values["mae"] == 3.5 and math.isclose(values["rmse"], math.sqrt(12.5))
    assert values["wape"] == 1.4 and values["coverage"] == 1.0
    hybrid = spark.createDataFrame(
        [(4.0, None, 10), (4.0, 20, 10)],
        "prediction double, weekly_views long, latest_views long",
    )
    predictions = (
        prediction_with_recipe(
            hybrid,
            {
                "target_transform": "residual",
                "weights": [0.5, 0.25, 0.25],
            },
        )
        .select("predicted_views")
        .collect()
    )
    assert [x.predicted_views for x in predictions] == [12.0, 19.5]
    inverted = (
        prediction_with_recipe(
            hybrid.withColumn("prediction", F.lit(math.log1p(17))),
            {"target_transform": "log1p"},
        )
        .first()
        .predicted_views
    )
    assert math.isclose(inverted, 17)
    return loaded


def verify_receipts(spark, model, scratch):
    reference = date(2026, 1, 12)
    deadline = datetime(2026, 1, 13, tzinfo=timezone.utc)
    history = spark.createDataFrame(
        [
            ("en.wikipedia", "A", date(2026, 1, 8), 4, deadline - timedelta(days=1)),
            ("en.wikipedia", "A", date(2026, 1, 8), 4, deadline - timedelta(hours=2)),
            (
                "en.wikipedia",
                "A",
                date(2026, 1, 8),
                99,
                deadline - timedelta(seconds=1),
            ),
            (
                "en.wikipedia",
                "A",
                date(2026, 1, 10),
                10,
                deadline + timedelta(seconds=1),
            ),
            ("en.wikipedia", "A", date(2026, 1, 11), 20, deadline - timedelta(days=1)),
            (
                "en.wikipedia",
                "excluded",
                date(2026, 1, 8),
                90,
                deadline - timedelta(days=1),
            ),
        ],
        "project string, page_title string, event_date date, views long, available_at timestamp",
    )
    manifest = {
        "version": "fixture",
        "dataset_manifest": {
            "project": "en.wikipedia",
            "pages": ["A", "B"],
            "availability_days": 2,
        },
    }
    values = forecast_for_reference(
        spark,
        model,
        manifest,
        history,
        reference,
        prediction_timestamp=deadline - timedelta(seconds=1),
    ).collect()
    assert len(values) == 1 and values[0].page_title == "A"
    assert math.isclose(values[0].predicted_views, 11, abs_tol=1e-7)
    assert values[0].prediction_date == date(2026, 1, 13)

    stream_input = scratch + "-stream-fixture"
    checkpoint = scratch + "-stream-checkpoint"
    ensure_new_hdfs_path(spark, stream_input)
    ensure_new_hdfs_path(spark, checkpoint)
    history.coalesce(1).write.parquet(stream_input)
    batches = []

    def score_batch(batch, _batch_id):
        rows = forecast_for_reference(
            spark,
            model,
            manifest,
            batch,
            reference,
            prediction_timestamp=deadline - timedelta(seconds=1),
        ).collect()
        batches.extend(rows)

    query = (
        spark.readStream.schema(history.schema)
        .parquet(stream_input)
        .writeStream.foreachBatch(score_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    try:
        assert query.awaitTermination(120)
        assert batches == values
    finally:
        if query.isActive:
            query.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch-model", required=True)
    args = parser.parse_args()
    spark = start_spark("TrendCast ML - tests distribués")
    try:
        ensure_new_hdfs_path(spark, args.scratch_model)
        verify_calendar(spark)
        model = verify_training(spark, args.scratch_model)
        verify_receipts(spark, model, args.scratch_model)
        print_manifest(
            "WIKIMEDIA_TESTS",
            {
                "calendar": "ok",
                "training": "ok",
                "receipts": "ok",
                "foreachBatch": "ok",
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
