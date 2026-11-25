"""Hacker News Silver transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = T.StructType([
    T.StructField("story_id", T.LongType()),
    T.StructField("story_type", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("discussion_url", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("score", T.LongType()),
    T.StructField("comments_count", T.LongType()),
])


def transform(df: DataFrame) -> DataFrame:
    parsed = (
        df.filter(F.col("source") == "hackernews")
        .withColumn("payload", F.from_json("payload_json", SCHEMA))
        .select(
            "*",
            F.col("payload.story_id").alias("story_id"),
            F.col("payload.story_type").alias("story_type"),
            F.trim(F.col("payload.title")).alias("title"),
            F.col("payload.url").alias("url"),
            F.col("payload.discussion_url").alias("discussion_url"),
            F.col("payload.author").alias("author"),
            F.col("payload.score").alias("score"),
            F.col("payload.comments_count").alias("comments_count"),
        )
        .drop("payload")
    )

    reason = (
        F.when(F.col("payload_json").isNull(), "missing_payload")
        .when(F.col("story_id").isNull(), "invalid_story_id")
        .when(F.col("title").isNull() | (F.col("title") == ""), "missing_title")
        .when(F.col("score").isNull(), "invalid_score")
        .when(F.col("score") < 0, "negative_score")
        .when(F.col("comments_count").isNull(), "invalid_comments_count")
        .when(F.col("comments_count") < 0, "negative_comments_count")
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
