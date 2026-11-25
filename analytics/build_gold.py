"""Build all TrendCast Gold datasets."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics.wikimedia_analytics import (
    build_daily_metrics,
    build_ml_dataset,
)
from analytics.hackernews_analytics import build_metrics as build_hn_metrics
from analytics.google_trends_analytics import build_metrics as build_gt_metrics


SILVER = "hdfs://namenode:9000/trendcast/processed"
GOLD = "hdfs://namenode:9000/trendcast/analytics"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write Gold datasets to HDFS.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("TrendCast-Build-Gold")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    try:
        wm = spark.read.parquet(f"{SILVER}/wikimedia_pageviews")
        hn = spark.read.parquet(f"{SILVER}/hackernews")
        gt = spark.read.parquet(f"{SILVER}/google_trends")

        datasets = {
            "daily_metrics": build_daily_metrics(wm),
            "ml_dataset": build_ml_dataset(wm),
            "hackernews_metrics": build_hn_metrics(hn),
            "google_trends_metrics": build_gt_metrics(gt),
        }

        for name, dataset in datasets.items():
            print(f"\nGOLD DATASET: {name}")
            print(f"ROWS: {dataset.count()}")
            dataset.printSchema()
            dataset.show(5, truncate=False)

            if args.write:
                destination = f"{GOLD}/{name}"
                (
                    dataset.write
                    .mode("overwrite")
                    .parquet(destination)
                )
                print(f"GOLD_WRITTEN: {destination}")

        if args.write:
            print("\nGOLD_BUILD_COMPLETED")
        else:
            print("\nSIMULATION_ONLY: no Gold data was written.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
