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
    evaluate,
    fit_and_score,
    performance,
    scored_with,
    split_summary,
    split_temporally,
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
        assert indexed["en.wikipedia", "A", day(13)][TARGET] is None
        assert indexed["en.wikipedia", "B", reference].latest_views == 900
        assert indexed["fr.wikipedia", "A", reference].latest_views == 800
    finally:
        clean.unpersist()


def verify_training(spark, path):
    synthetic = spark.range(80).select(
        F.date_add(
            F.lit("2026-01-01").cast("date"), (F.col("id") / 4).cast("int")
        ).alias("prediction_date"),
        (F.col("id") % 20).cast("double").alias("latest_views"),
        (2 * (F.col("id") % 20) + 3).alias(TARGET),
    )
    splits = split_temporally(synthetic)
    train, validation, test = (splits[k] for k in ("train", "validation", "test"))
    train_info, val_info, test_info = map(split_summary, (train, validation, test))
    assert train_info["last_target_date"] < val_info["first_target_date"]
    assert val_info["last_target_date"] < test_info["first_target_date"]
    assert [s["rows"] for s in (train_info, val_info, test_info)] == [56, 12, 12]

    def fit(name, parameters):
        return fit_and_score(
            name, parameters, "fixture", ("latest_views",), (), train, validation
        )

    candidates = [fit(name, params) for name, params in INITIAL.items()]
    previous = min(candidates, key=lambda item: performance(item[0]))
    assert previous[0]["metrics"]["mae"] < 1e-7
    challenger = fit("LinearRegression", GRID["LinearRegression"][0])
    assert challenger[0]["parameters"] != previous[0]["parameters"]
    selected, model = min((previous, challenger), key=lambda item: performance(item[0]))
    assert selected["metrics"]["rows"] == 12
    model.write().save(path)
    loaded = PipelineModel.load(path)
    scores = evaluate(scored_with(loaded.transform(test), F.col("prediction")))
    assert scores["rmse"] < 1e-7 and scores["finite_predictions"] == 12
    exact = spark.createDataFrame([(1, 4.0), (4, 0.0)], [TARGET, "predicted_views"])
    values = evaluate(exact)
    assert values["mae"] == 3.5 and math.isclose(values["rmse"], math.sqrt(12.5))
    return loaded


def verify_receipts(spark, model):
    reference = date(2026, 1, 12)
    deadline = datetime(2026, 1, 13, tzinfo=timezone.utc)
    history = spark.createDataFrame(
        [
            ("en.wikipedia", "A", date(2026, 1, 8), 4, deadline - timedelta(days=1)),
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch-model", required=True)
    args = parser.parse_args()
    spark = start_spark("TrendCast ML - tests distribués")
    try:
        ensure_new_hdfs_path(spark, args.scratch_model)
        verify_calendar(spark)
        model = verify_training(spark, args.scratch_model)
        verify_receipts(spark, model)
        print_manifest(
            "WIKIMEDIA_TESTS", {"calendar": "ok", "training": "ok", "receipts": "ok"}
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
