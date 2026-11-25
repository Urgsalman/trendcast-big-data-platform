
"""Read-only audit of TrendCast Bronze Parquet data on HDFS."""

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DEFAULT_PATH = "hdfs://namenode:9000/trendcast/raw/events"


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--path",
        default=DEFAULT_PATH,
        help="HDFS path containing Bronze Parquet events.",
    )

    parser.add_argument(
        "--require-cluster",
        action="store_true",
        help="Require a Spark standalone cluster.",
    )

    parser.add_argument(
        "--export-wikimedia-ids",
        help=(
            "Optional HDFS path to export distinct Wikimedia "
            "event IDs as text files."
        ),
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("TrendCast-Audit-Bronze")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # --------------------------------------------------
        # 1. Verify Spark execution environment
        # --------------------------------------------------

        master = spark.sparkContext.master

        if args.require_cluster and not master.startswith("spark://"):
            raise RuntimeError(
                f"Expected Spark cluster, got {master}"
            )

        # --------------------------------------------------
        # 2. Read Bronze data from HDFS
        # --------------------------------------------------

        bronze = spark.read.parquet(args.path)

        total_rows = bronze.count()

        # --------------------------------------------------
        # 3. Audit by source
        # --------------------------------------------------

        sources = [
            row.asDict(recursive=True)
            for row in bronze.groupBy("source").agg(
                F.count("*").alias("rows"),
                F.countDistinct("event_id").alias(
                    "distinct_event_ids"
                ),
                F.min("event_timestamp").alias(
                    "first_timestamp"
                ),
                F.max("event_timestamp").alias(
                    "last_timestamp"
                ),
            ).orderBy("source").collect()
        ]

        distinct_ids = (
            bronze
            .where(F.col("event_id").isNotNull())
            .select("event_id")
            .distinct()
            .count()
        )

        # --------------------------------------------------
        # 4. Prepare Wikimedia data
        # --------------------------------------------------

        payload_schema = T.StructType([
            T.StructField(
                "page_title",
                T.StringType(),
                True,
            ),
            T.StructField(
                "views",
                T.LongType(),
                True,
            ),
        ])

        wiki = (
            bronze
            .where(
                F.col("source") == "wikimedia_pageviews"
            )
            .withColumn(
                "payload",
                F.from_json(
                    "payload_json",
                    payload_schema,
                ),
            )
            .withColumn(
                "event_date",
                F.to_date("event_timestamp"),
            )
            .select(
                "event_id",
                "event_date",
                F.col("payload.page_title").alias(
                    "page_title"
                ),
                F.col("payload.views").alias("views"),
            )
        )

        # --------------------------------------------------
        # 5. Export distinct Wikimedia event IDs
        # --------------------------------------------------

        if args.export_wikimedia_ids:
            export_path = args.export_wikimedia_ids

            wikimedia_ids = (
                wiki
                .select(
                    F.col("event_id")
                    .cast("string")
                    .alias("value")
                )
                .where(F.col("value").isNotNull())
                .distinct()
            )

            exported_count = wikimedia_ids.count()

            (
                wikimedia_ids
                .coalesce(4)
                .write
                .mode("overwrite")
                .text(export_path)
            )

            print(
                "WIKIMEDIA_IDS_EXPORTED="
                + export_path,
                flush=True,
            )

            print(
                "WIKIMEDIA_IDS_COUNT="
                + str(exported_count),
                flush=True,
            )

        # --------------------------------------------------
        # 6. Calculate Wikimedia date boundaries
        # --------------------------------------------------

        bounds = wiki.agg(
            F.min("event_date").alias("first_date"),
            F.max("event_date").alias("last_date"),
        ).first()

        # --------------------------------------------------
        # 7. Detect duplicate page/date combinations
        # --------------------------------------------------

        duplicate_keys = (
            wiki
            .where(
                F.col("page_title").isNotNull()
                & F.col("event_date").isNotNull()
            )
            .groupBy("page_title", "event_date")
            .agg(F.count("*").alias("rows"))
            .where(F.col("rows") > 1)
        )

        duplicate_key_count = duplicate_keys.count()

        duplicate_excess = (
            duplicate_keys
            .agg(
                F.sum(F.col("rows") - 1).alias("n")
            )
            .first()["n"]
            or 0
        )

        # --------------------------------------------------
        # 8. Wikimedia statistics
        # --------------------------------------------------

        wikimedia_rows = wiki.count()

        wikimedia_distinct_ids = (
            wiki
            .select("event_id")
            .distinct()
            .count()
        )

        wikimedia_distinct_pages = (
            wiki
            .select("page_title")
            .where(F.col("page_title").isNotNull())
            .distinct()
            .count()
        )

        wikimedia_distinct_dates = (
            wiki
            .select("event_date")
            .where(F.col("event_date").isNotNull())
            .distinct()
            .count()
        )

        # --------------------------------------------------
        # 9. Page summary
        # --------------------------------------------------

        page_summary = [
            row.asDict(recursive=True)
            for row in (
                wiki
                .groupBy("page_title")
                .agg(
                    F.count("*").alias("rows"),
                    F.countDistinct("event_date").alias(
                        "distinct_days"
                    ),
                    F.min("event_date").alias("first_date"),
                    F.max("event_date").alias("last_date"),
                )
                .orderBy(F.desc("rows"))
                .collect()
            )
        ]

        duplicate_examples = [
            row.asDict(recursive=True)
            for row in (
                duplicate_keys
                .orderBy(F.desc("rows"))
                .limit(20)
                .collect()
            )
        ]

        # --------------------------------------------------
        # 10. Build audit report
        # --------------------------------------------------

        result = {
            "path": args.path,
            "spark_master": master,
            "total_rows": total_rows,
            "distinct_event_ids": distinct_ids,
            "duplicate_event_id_rows": (
                total_rows - distinct_ids
            ),
            "sources": sources,
            "wikimedia": {
                "rows": wikimedia_rows,
                "distinct_event_ids": (
                    wikimedia_distinct_ids
                ),
                "distinct_pages": (
                    wikimedia_distinct_pages
                ),
                "distinct_dates": (
                    wikimedia_distinct_dates
                ),
                "first_date": (
                    str(bounds["first_date"])
                    if bounds["first_date"]
                    else None
                ),
                "last_date": (
                    str(bounds["last_date"])
                    if bounds["last_date"]
                    else None
                ),
                "duplicate_page_date_keys": (
                    duplicate_key_count
                ),
                "duplicate_excess_rows": int(
                    duplicate_excess
                ),
                "duplicate_examples": duplicate_examples,
                "page_summary_top_50": page_summary,
            },
        }

        print(
            "BRONZE_AUDIT="
            + json.dumps(result, default=str),
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
