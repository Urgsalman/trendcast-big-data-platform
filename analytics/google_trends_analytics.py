"""Google Trends Gold transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "observation_date",
            F.to_date("kafka_timestamp")
        )
        .groupBy("observation_date", "geo")
        .agg(
            F.count("*").alias("observations_count"),
            F.countDistinct("title").alias("distinct_trends"),
        )
    )
