"""Profilage distribué des exports Gold et du panel configuré."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.ml.features import (
    HISTORY_SCHEMA,
    canonical_observations,
    key_statistics,
    require_schema,
    select_panel,
)
from analytics.ml.runtime import (
    GOLD_ROOT,
    SERIES,
    ensure_new_hdfs_path,
    print_manifest,
    start_spark,
    write_manifest,
)

EXPECTED_EXPORT_ROWS = {
    "ml_dataset": 462_056,
    "daily_metrics": 3_977,
    "hackernews_metrics": 2,
    "google_trends_metrics": 2,
}
QUANTILES = [0.01, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


def count_when(condition):
    return F.count(F.when(condition, 1))


def profile_observations(frame):
    rows = (
        frame.agg(
            F.count("*").alias("rows"),
            F.countDistinct("project").alias("projects"),
            F.countDistinct(*SERIES).alias("pages"),
            F.countDistinct("event_date").alias("dates"),
            F.min("event_date").alias("first_date"),
            F.max("event_date").alias("last_date"),
            F.min("views").alias("minimum_views"),
            F.max("views").alias("maximum_views"),
            F.avg("views").alias("mean_views"),
            F.percentile_approx("views", QUANTILES, 10000).alias("views_quantiles"),
            count_when(F.col("views") == 0).alias("zero_views"),
            count_when(F.col("views") < 0).alias("negative_views"),
            *(count_when(F.col(c).isNull()).alias(f"null_{c}") for c in frame.columns),
        )
        .first()
        .asDict()
    )
    rows["zero_rate"] = rows["zero_views"] / rows["rows"] if rows["rows"] else None
    rows["views_quantile_probabilities"] = QUANTILES
    latency = frame.select(
        F.datediff(F.to_date("kafka_timestamp"), "event_date").alias("lag")
    )
    rows["event_to_kafka_days"] = (
        latency.agg(
            F.min("lag").alias("minimum"),
            F.max("lag").alias("maximum"),
            F.percentile_approx("lag", QUANTILES, 10000).alias("quantiles"),
            count_when(F.col("lag") < 0).alias("negative"),
            count_when(F.col("lag") == 1).alias("one_day"),
            count_when(F.col("lag") > 365).alias("over_one_year"),
        )
        .first()
        .asDict()
    )
    return rows


def profile_keys(frame):
    stats = key_statistics(frame).cache()
    try:
        duplicates = (
            stats.agg(
                count_when(
                    (F.col("observations") > 1)
                    & (F.col("min_views") == F.col("max_views"))
                ).alias("identical_keys"),
                count_when(F.col("min_views") != F.col("max_views")).alias(
                    "conflicting_keys"
                ),
                F.sum(F.col("observations") - 1).alias("surplus_rows"),
            )
            .first()
            .asDict()
        )
        clean = canonical_observations(stats)
        ordered = Window.partitionBy(*SERIES).orderBy("event_date")
        gaps = clean.withColumn(
            "interval_days", F.datediff("event_date", F.lag("event_date").over(ordered))
        )
        series = gaps.groupBy(*SERIES).agg(
            F.count("*").alias("observed_days"),
            F.min("event_date").alias("first_date"),
            F.max("event_date").alias("last_date"),
            count_when(F.col("interval_days") == 1).alias("next_day_pairs"),
            count_when(F.col("interval_days") > 1).alias("gap_intervals"),
            F.sum(F.greatest(F.lit(0), F.col("interval_days") - 1)).alias(
                "missing_days"
            ),
        )
        coverage = (
            series.agg(
                F.min("observed_days").alias("min_days"),
                F.percentile_approx("observed_days", QUANTILES, 10000).alias(
                    "days_quantiles"
                ),
                F.max("observed_days").alias("max_days"),
                count_when(F.col("observed_days") < 28).alias(
                    "series_under_28_observations"
                ),
                count_when(F.col("gap_intervals") > 0).alias("discontinuous_series"),
                F.sum("next_day_pairs").alias("next_day_pairs"),
                F.sum("gap_intervals").alias("gap_intervals"),
                F.sum("missing_days").alias("missing_days"),
            )
            .first()
            .asDict()
        )
        return {"duplicates": duplicates, "coverage": coverage}, series
    finally:
        stats.unpersist()


def to_rows(frame):
    return [row.asDict() for row in frame.collect()]


def profile(spark, args):
    exports = {}
    for name, expected_count in EXPECTED_EXPORT_ROWS.items():
        frame = spark.read.parquet(f"{args.gold_root}/{name}")
        count = frame.count()
        exports[name] = {
            "rows": count,
            "files": len(frame.inputFiles()),
            "schema": frame.schema.simpleString(),
        }
        if args.verify_export and count != expected_count:
            raise ValueError(f"{name} : {count} lignes, {expected_count} attendues")
        if name in {"hackernews_metrics", "google_trends_metrics"}:
            exports[name]["daily"] = to_rows(frame.orderBy(*frame.columns))

    observations = spark.read.parquet(f"{args.gold_root}/ml_dataset").cache()
    require_schema(
        observations, {**HISTORY_SCHEMA, "kafka_timestamp": T.TimestampType()}
    )
    try:
        all_counts = profile_observations(observations)
        all_keys, _ = profile_keys(observations)
        panel = select_panel(observations, args.project, args.pages)
        panel_counts = profile_observations(panel)
        panel_keys, panel_series = profile_keys(panel)
        panel_daily = panel.groupBy("event_date").agg(
            F.countDistinct("page_title").alias("pages_count"),
            F.sum("views").alias("total_views"),
        )
        daily = spark.read.parquet(f"{args.gold_root}/daily_metrics")
        populations = to_rows(
            daily.groupBy("pages_count").count().orderBy("pages_count")
        )
        report = {
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "exports": exports,
            "all": {**all_counts, **all_keys},
            "panel": {
                **panel_counts,
                **panel_keys,
                "configured_pages": args.pages,
                "series": to_rows(panel_series.orderBy(*SERIES)),
            },
            "daily_page_counts": populations,
        }
        if args.output:
            panel_daily.orderBy("event_date").coalesce(1).write.option(
                "header", True
            ).csv(f"{args.output}/panel_daily")
            daily.orderBy("event_date", "project").coalesce(1).write.option(
                "header", True
            ).csv(f"{args.output}/gold_daily")
            write_manifest(spark, f"{args.output}/manifest", report)
        print_manifest("WIKIMEDIA_PROFILE", report)
    finally:
        observations.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", default=GOLD_ROOT)
    parser.add_argument("--project", default="en.wikipedia")
    parser.add_argument("--pages", required=True, type=lambda value: value.split(","))
    parser.add_argument("--verify-export", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    spark = start_spark("TrendCast Wikimedia Gold - profilage")
    try:
        if args.output:
            ensure_new_hdfs_path(spark, args.output)
        profile(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
