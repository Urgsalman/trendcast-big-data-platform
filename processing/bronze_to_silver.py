"""Orchestrate Bronze to Silver processing."""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from processing.silver import wikimedia, hackernews, google_trends


DEFAULT_INPUT = "hdfs://namenode:9000/trendcast/raw/events"
DEFAULT_OUTPUT = "hdfs://namenode:9000/trendcast/processed"
DEFAULT_QUARANTINE = "hdfs://namenode:9000/trendcast/processed/quarantine"


TRANSFORMS = {
    "wikimedia_pageviews": wikimedia.transform,
    "hackernews": hackernews.transform,
    "google_trends": google_trends.transform,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--quarantine", default=DEFAULT_QUARANTINE)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write results to HDFS. Without this option, run in simulation mode.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("TrendCast-Bronze-To-Silver")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze = spark.read.parquet(args.input)

        print("BRONZE_TOTAL =", bronze.count())

        for source, transform in TRANSFORMS.items():
            print(f"\nProcessing source: {source}")

            source_df = transform(bronze)

            valid = source_df.filter(F.col("quality_status") == "valid")
            rejected = source_df.filter(F.col("quality_status") == "rejected")

            valid_count = valid.count()
            rejected_count = rejected.count()

            print(f"SOURCE={source}")
            print(f"VALID={valid_count}")
            print(f"REJECTED={rejected_count}")

            rejected.groupBy("quality_reason").count().show(
                truncate=False
            )

            if args.write:
                output_path = f"{args.output}/{source}"
                quarantine_path = f"{args.quarantine}/{source}"

                (
                    valid.write
                    .mode("overwrite")
                    .parquet(output_path)
                )

                if rejected_count > 0:
                    (
                        rejected.write
                        .mode("overwrite")
                        .parquet(quarantine_path)
                    )

                print(f"SILVER_WRITTEN={output_path}")

        if not args.write:
            print("\nSIMULATION_ONLY: no HDFS data was modified.")
        else:
            print("\nSILVER_PROCESSING_COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
