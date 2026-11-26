"""Vérifier le paquet candidat importé sur Spark distribué avec le replay Gold."""

import argparse
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from analytics.ml.candidate import WikimediaCandidate, forecasts
from analytics.ml.predict_sample import OUTPUT_COLUMNS
from analytics.ml.runtime import ensure_new_hdfs_path, print_manifest, start_spark

CUTOFF = datetime(2026, 11, 22, 23, 59, 59, tzinfo=timezone.utc)
TARGET = CUTOFF.date() + timedelta(days=1)
RECEIPT_SCHEMA = "project string, page_title string, event_date date, views long, available_at timestamp"
EXPECTED_SCHEMA = (
    "delay int, candidate string, page_title string, prediction_date date, "
    "prediction double, median_history double"
)


def replay_input(spark, path, schema):
    if path.endswith(".csv"):
        return (
            spark.read.schema(schema)
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(path)
        )
    return spark.read.parquet(path)


def fixture(base, pages):
    d1 = CUTOFF.date() - timedelta(days=1)
    existing = base.where(
        (F.col("event_date") == d1) & (F.col("page_title") == pages[12])
    )
    # Une valeur avant la fin du jour source doit rester inadmissible.
    early = existing.withColumn(
        "available_at", F.lit(CUTOFF - timedelta(days=1))
    ).withColumn("views", F.col("views") + 1)
    alien = existing.withColumn("page_title", F.lit("__unknown_page__"))
    must_abstain = existing.withColumn("page_title", F.lit(pages[7]))
    conflict = must_abstain.withColumn("views", F.col("views") + 1)
    assert existing.count() == 1
    return (
        base.unionByName(early)
        .unionByName(alien)
        .unionByName(must_abstain)
        .unionByName(conflict)
    )


def verify(spark, args):
    ensure_new_hdfs_path(spark, args.output)
    candidate = WikimediaCandidate.load(spark, args.package)
    pages = sorted(candidate.manifest["panel"]["pages"])
    replay = f"{args.package.rstrip('/')}/replay"
    base = replay_input(spark, args.history or f"{replay}/receipts.csv", RECEIPT_SCHEMA)
    history = fixture(base, pages)
    decision = candidate.evaluate(history, CUTOFF).cache()
    try:
        batch = decision.orderBy("page_title").collect()
        selected = forecasts(decision)
        assert tuple(selected.columns) == OUTPUT_COLUMNS
        assert selected.count() == 15
        assert len(batch) == len(pages) == 20
        by_page = {row.page_title: row for row in batch}
        assert list(by_page) == pages
        assert {p for p in pages if by_page[p].route == "J-2"} == set(pages[:5])
        assert {p for p in pages if by_page[p].route == "abstention"} == set(
            pages[5:10]
        )
        assert by_page[pages[5]].abstention_reason == "no_admissible_history"
        assert by_page[pages[7]].abstention_reason == "conflicting_recent_receipts"
        assert by_page[pages[9]].abstention_reason == "no_admissible_j1_or_j2"
        assert by_page[pages[10]].historical_reference is None
        assert by_page[pages[10]].trend_direction is None
        assert by_page[pages[10]].trend_rank is None
        assert by_page[pages[11]].j1_available_at is not None
        assert all(row.eligible_pages_count == 14 for row in batch)
        assert all(row.prediction_date == TARGET for row in batch)
        assert all(
            row.predicted_views is None for row in batch if row.route == "abstention"
        )
        fit = replay_input(
            spark, args.predictions or f"{replay}/expected.csv", EXPECTED_SCHEMA
        ).where(
            (F.col("prediction_date") == TARGET)
            & F.col("candidate").isin("log", "v2", "pers")
        )
        expected = {
            (row.delay, row.candidate, row.page_title): row for row in fit.collect()
        }
        deviations = []
        distinct = sorted(
            {r.trend_score for r in batch if r.trend_score is not None}, reverse=True
        )
        for row in batch:
            if row.route == "abstention":
                continue
            age = row.availability_days_used
            spec = candidate.manifest["routes"][str(age)]
            assert row.model_version == spec["model_version"]
            assert row.latest_observation_date == CUTOFF.date() - timedelta(days=age)
            reference = expected[age, "log" if age == 1 else "v2", row.page_title]
            deviations.append(abs(row.predicted_views - reference.prediction))
            if row.historical_reference is None:
                continue
            median = expected[2, "v2", row.page_title].median_history
            assert row.historical_reference == median
            change = round((row.predicted_views - median) / max(1, median), 12)
            direction = (
                "hausse" if change > 0.1 else "baisse" if change < -0.1 else "stable"
            )
            assert row.trend_direction == direction
            last = expected[age, "pers", row.page_title].prediction
            assert math.isclose(
                row.trend_score, (last - median) / max(1, median), abs_tol=1e-11
            )
            assert row.trend_rank == distinct.index(row.trend_score) + 1
        assert max(deviations) < 1e-8
    finally:
        decision.unpersist()

    # Deux petits lots déclenchent le calcul sur le même instantané durable.
    source = f"{args.output}/triggers"
    spark.createDataFrame([(0,)], ["trigger_id"]).coalesce(1).write.parquet(source)
    spark.createDataFrame([(1,)], ["trigger_id"]).coalesce(1).write.mode(
        "append"
    ).parquet(source)
    durable_history = history
    seen = []

    def handle(microbatch, batch_id):
        assert microbatch.count() == 1
        output = (
            candidate.evaluate(durable_history, CUTOFF).orderBy("page_title").collect()
        )
        assert output == batch
        seen.append(batch_id)

    query = (
        spark.readStream.schema("trigger_id long")
        .option("maxFilesPerTrigger", 1)
        .parquet(source)
        .writeStream.foreachBatch(handle)
        .option("checkpointLocation", f"{args.output}/checkpoint")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination(180)
    assert sorted(seen) == [0, 1]
    print_manifest(
        "CANDIDATE_VERIFIED",
        {
            "application": spark.sparkContext.applicationId,
            "package": args.package,
            "target": TARGET,
            "requested": len(batch),
            "J-1": 10,
            "J-2": 5,
            "abstentions": 5,
            "ranked": 14,
            "batch_ids": seen,
            "volume_max_abs_delta": max(deviations),
            "exact_batch_foreachbatch": True,
            "replay_examples": [
                by_page[p].asDict() for p in [pages[0], pages[5], pages[10], pages[12]]
            ],
        },
    )


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for option in ("package", "output"):
        cli.add_argument(f"--{option}", required=True)
    cli.add_argument(
        "--history", help="Instantané du replay historique à la place du CSV livré"
    )
    cli.add_argument(
        "--predictions", help="Prévisions archivées à la place du CSV livré"
    )
    args = cli.parse_args()
    if bool(args.history) != bool(args.predictions):
        cli.error("--history et --predictions doivent être fournis ensemble")
    spark = start_spark("TrendCast - contrôle livraison candidate J1")
    try:
        verify(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
