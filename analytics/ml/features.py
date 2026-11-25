"""Contrat journalier partagé entre préparation et inférence."""

import math

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from analytics.ml.runtime import KEY, SERIES, TARGET

HISTORY_DAYS = 28
DEFAULT_AVAILABILITY_DAYS = 2
HISTORY_SCHEMA = {
    "project": T.StringType(),
    "page_title": T.StringType(),
    "event_date": T.DateType(),
    "views": T.LongType(),
}
CORE_FEATURES = ("latest_views", "latest_age", "weekday_sin", "weekday_cos")
EXTRA_FEATURES = (
    "target_day_of_month",
    "target_month",
    "target_weekend",
    "views_lag_1",
    "views_lag_7",
    "views_lag_14",
    "views_lag_28",
    "mean_7",
    "mean_14",
    "std_7",
    "std_14",
    "observed_days_7",
    "observed_days_14",
    "change_since_lag_7",
)


def require_schema(frame, expected):
    observed = {field.name: field.dataType for field in frame.schema}
    mismatches = [
        f"{name}: {observed.get(name)} (attendu {field_type})"
        for name, field_type in expected.items()
        if observed.get(name) != field_type
    ]
    if mismatches:
        raise ValueError("Schéma incompatible : " + ", ".join(mismatches))


def valid_observation():
    return (
        (F.length(F.trim("project")) > 0)
        & (F.length(F.trim("page_title")) > 0)
        & F.col("event_date").isNotNull()
        & (F.col("views") >= 0)
    )


def key_statistics(frame):
    require_schema(frame, HISTORY_SCHEMA)
    return (
        frame.where(valid_observation())
        .groupBy(*KEY)
        .agg(
            F.count("*").alias("observations"),
            F.min("views").alias("min_views"),
            F.max("views").alias("max_views"),
        )
    )


def canonical_observations(key_stats):
    """Collapse repeated counts and exclude all versions of a conflicting key."""
    return key_stats.where(F.col("min_views") == F.col("max_views")).select(
        *KEY, F.col("min_views").alias("views")
    )


def select_panel(frame, project, pages):
    return frame.where((F.col("project") == project) & F.col("page_title").isin(*pages))


def calendar_references(spark, project, pages, start, end):
    panel = spark.createDataFrame([(project, page) for page in pages], list(SERIES))
    return panel.withColumn(
        "event_date",
        F.explode(F.sequence(F.lit(start).cast("date"), F.lit(end).cast("date"))),
    )


def temporal_features(
    references, clean_history, availability_days=DEFAULT_AVAILABILITY_DAYS
):
    """Use a unique timeline per series and day offsets back from each reference."""
    if not 1 <= availability_days <= 15:
        raise ValueError("Le délai d'observation doit être compris entre 1 et 15 jours")
    history = clean_history.select(*KEY, "views")
    markers = references.select(*KEY).distinct().withColumn("is_reference", F.lit(True))
    timeline = history.join(markers, list(KEY), "full").withColumn(
        "day", F.datediff("event_date", F.lit("1970-01-01"))
    )
    by_day = Window.partitionBy(*SERIES).orderBy("day")
    recent = by_day.rangeBetween(-HISTORY_DAYS, -availability_days)
    recent_7 = by_day.rangeBetween(-availability_days - 6, -availability_days)
    recent_14 = by_day.rangeBetween(-availability_days - 13, -availability_days)

    def exact_lag(days):
        if days < availability_days:
            return F.lit(None).cast("long")
        return F.max("views").over(by_day.rangeBetween(-days, -days))

    featured = (
        timeline.select(
            *KEY,
            "is_reference",
            F.date_add("event_date", 1).alias("prediction_date"),
            F.last("views", ignorenulls=True).over(recent).alias("latest_views"),
            (
                F.col("day")
                - F.max(F.when(F.col("views").isNotNull(), F.col("day"))).over(recent)
            ).alias("latest_age"),
            *(exact_lag(days).alias(f"views_lag_{days}") for days in (1, 7, 14, 28)),
            exact_lag(6).alias("weekly_views"),
            F.avg("views").over(recent_7).alias("mean_7"),
            F.avg("views").over(recent_14).alias("mean_14"),
            F.stddev_pop("views").over(recent_7).alias("std_7"),
            F.stddev_pop("views").over(recent_14).alias("std_14"),
            F.count("views").over(recent_7).alias("observed_days_7"),
            F.count("views").over(recent_14).alias("observed_days_14"),
        )
        .where("is_reference")
        .drop("is_reference")
    )
    weekday = F.dayofweek("prediction_date")
    return (
        featured.withColumn("weekday_sin", F.sin((weekday - 1) * (2 * math.pi / 7)))
        .withColumn("weekday_cos", F.cos((weekday - 1) * (2 * math.pi / 7)))
        .withColumn("target_day_of_month", F.dayofmonth("prediction_date"))
        .withColumn("target_month", F.month("prediction_date"))
        .withColumn("target_weekend", weekday.isin(1, 7).cast("double"))
        .withColumn("change_since_lag_7", F.col("latest_views") - F.col("views_lag_7"))
    )


def attach_labels(features, clean_history):
    labels = clean_history.select(
        *SERIES,
        F.col("event_date").alias("prediction_date"),
        F.col("views").alias(TARGET),
    )
    return features.join(labels, [*SERIES, "prediction_date"], "left")
