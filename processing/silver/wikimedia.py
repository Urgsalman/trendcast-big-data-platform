
"""Wikimedia Silver transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


WIKIMEDIA_SCHEMA = T.StructType([
    T.StructField("project", T.StringType(), True),
    T.StructField("page_title", T.StringType(), True),
    T.StructField("granularity", T.StringType(), True),
    T.StructField("views", T.LongType(), True),
])


def transform(df: DataFrame) -> DataFrame:
    """Parse and validate Wikimedia Bronze records."""

    parsed = (
        df.filter(F.col("source") == "wikimedia_pageviews")
        .withColumn(
            "payload",
            F.from_json("payload_json", WIKIMEDIA_SCHEMA)
        )
        .select(
            "*",
            F.col("payload.project").alias("project"),
            F.trim(F.col("payload.page_title")).alias("page_title"),
            F.col("payload.granularity").alias("granularity"),
            F.col("payload.views").alias("views"),
            F.to_date("event_timestamp").alias("event_date"),
        )
        .drop("payload")
    )

    reason = (
        F.when(F.col("payload_json").isNull(), "missing_payload")
        .when(F.col("project").isNull(), "missing_project")
        .when(
            F.col("page_title").isNull() | (F.col("page_title") == ""),
            "missing_page_title"
        )
        .when(
            F.col("granularity") != "daily",
            "unsupported_granularity"
        )
        .when(F.col("views").isNull(), "invalid_views")
        .when(F.col("views") < 0, "negative_views")
        .when(F.col("event_date").isNull(), "invalid_event_date")
    )

    return (
        parsed
        .withColumn("quality_reason", reason)
        .withColumn(
            "quality_status",
            F.when(F.col("quality_reason").isNull(), "valid")
            .otherwise("rejected")
        )
    )
