"""Hacker News Gold transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "observation_date",
            F.to_date("kafka_timestamp")
        )
        .groupBy("observation_date")
        .agg(
            F.countDistinct("story_id").alias("stories_count"),
            F.avg("score").alias("average_score"),
            F.max("score").alias("maximum_score"),
            F.sum("comments_count").alias("total_comments"),
        )
    )
