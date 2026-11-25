"""Wikimedia Gold transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_metrics(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("event_date", "project")
        .agg(
            F.sum("views").alias("total_views"),
            F.countDistinct("page_title").alias("pages_count"),
            F.avg("views").alias("average_views"),
        )
    )


def build_ml_dataset(df: DataFrame) -> DataFrame:
    return df.select(
        "event_date",
        "project",
        "page_title",
        "views",
        "kafka_timestamp",
    )
