"""Google Trends Silver transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = T.StructType([
    T.StructField("geo", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("approx_traffic", T.StringType()),
    T.StructField("trend_url", T.StringType()),
])


def transform(df: DataFrame) -> DataFrame:
    parsed = (
        df.filter(F.col("source") == "google_trends")
        .withColumn("payload", F.from_json("payload_json", SCHEMA))
        .select(
            "*",
            F.upper(F.trim(F.col("payload.geo"))).alias("geo"),
            F.trim(F.col("payload.title")).alias("title"),
            F.trim(F.col("payload.approx_traffic")).alias("approx_traffic"),
            F.col("payload.trend_url").alias("trend_url"),
        )
        .drop("payload")
    )

    reason = (
        F.when(F.col("payload_json").isNull(), "missing_payload")
        .when(F.col("geo").isNull() | (F.col("geo") == ""), "missing_geo")
        .when(F.col("title").isNull() | (F.col("title") == ""), "missing_title")
        .when(F.col("event_timestamp").isNull(), "invalid_event_timestamp")
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
