"""Automated quality checks for TrendCast Silver datasets."""

from pyspark.sql import SparkSession, functions as F


BASE = "hdfs://namenode:9000/trendcast/processed"

DATASETS = {
    "wikimedia_pageviews": {
        "required": [
            "event_id", "source", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "project", "page_title", "views", "event_date",
        ],
        "non_null": [
            "event_id", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "project", "page_title", "views", "event_date",
        ],
    },
    "hackernews": {
        "required": [
            "event_id", "source", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "story_id", "title", "score", "comments_count",
        ],
        "non_null": [
            "event_id", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "story_id", "title", "score", "comments_count",
        ],
    },
    "google_trends": {
        "required": [
            "event_id", "source", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "geo", "title",
        ],
        "non_null": [
            "event_id", "event_timestamp", "kafka_timestamp",
            "kafka_topic", "kafka_partition", "kafka_offset",
            "geo", "title",
        ],
    },
}


def check_dataset(spark, source, config):
    path = f"{BASE}/{source}"
    df = spark.read.parquet(path)

    failures = []
    total = df.count()

    print(f"\n========== {source} ==========")
    print(f"TOTAL_ROWS: {total}")

    missing_columns = [
        column for column in config["required"]
        if column not in df.columns
    ]

    if missing_columns:
        failures.append(f"Missing columns: {missing_columns}")
    else:
        print("SCHEMA: PASS")

    for column in config["non_null"]:
        if column not in df.columns:
            continue

        null_count = df.filter(F.col(column).isNull()).count()
        print(f"NULL_{column}: {null_count}")

        if null_count > 0:
            failures.append(f"{column}: {null_count} null values")

    kafka_columns = [
        "kafka_topic", "kafka_partition", "kafka_offset"
    ]

    if all(column in df.columns for column in kafka_columns):
        duplicate_count = (
            df.groupBy(*kafka_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"DUPLICATE_KAFKA_POSITIONS: {duplicate_count}")

        if duplicate_count > 0:
            failures.append("Duplicate Kafka positions")

    if source == "wikimedia_pageviews" and "views" in df.columns:
        negative = df.filter(F.col("views") < 0).count()
        print(f"NEGATIVE_VIEWS: {negative}")
        if negative:
            failures.append("Negative pageviews")

    if source == "hackernews":
        for column in ["score", "comments_count"]:
            if column in df.columns:
                negative = df.filter(F.col(column) < 0).count()
                print(f"NEGATIVE_{column}: {negative}")
                if negative:
                    failures.append(f"Negative {column}")

    if source == "wikimedia_pageviews" and "event_date" in df.columns:
        invalid_dates = df.filter(F.col("event_date").isNull()).count()
        print(f"INVALID_EVENT_DATES: {invalid_dates}")
        if invalid_dates:
            failures.append("Invalid event dates")

    if failures:
        print("RESULT: FAIL")
        for failure in failures:
            print(" -", failure)
    else:
        print("RESULT: PASS")

    return failures


def main():
    spark = (
        SparkSession.builder
        .appName("TrendCast-Silver-Quality-Checks")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    all_failures = []

    try:
        for source, config in DATASETS.items():
            try:
                all_failures.extend(
                    check_dataset(spark, source, config)
                )
            except Exception as error:
                all_failures.append(f"{source}: {error}")
                print(f"RESULT: ERROR - {error}")

        print("\n========== FINAL QUALITY REPORT ==========")

        if all_failures:
            print(f"QUALITY_CHECKS_FAILED: {len(all_failures)}")
            for failure in all_failures:
                print(" -", failure)
        else:
            print("ALL_QUALITY_CHECKS_PASSED")

    finally:
        spark.stop()

    if all_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
