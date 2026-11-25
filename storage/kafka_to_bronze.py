"""Archive Kafka records in HDFS Parquet with a native streaming file sink."""

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parent))
from settings import validate_destinations

SOURCES = ("wikimedia_pageviews", "hackernews", "google_trends")
ENVELOPE = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("schema_version", T.StringType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)


def bronze_records(records):
    """Preserve every record, including tombstones and malformed envelopes."""
    parsed = records.select(
        F.col("key").alias("raw_key"),
        F.col("value").alias("raw_value"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
    ).withColumn("envelope", F.from_json("raw_json", ENVELOPE))
    parsed = parsed.withColumn(
        "payload_json", F.get_json_object("raw_json", "$.payload")
    )
    parsed = parsed.withColumn(
        "event_timestamp", F.try_to_timestamp("envelope.timestamp")
    )
    valid = (
        F.col("envelope._corrupt_record").isNull()
        & (F.length(F.trim(F.col("envelope.event_id"))) > 0)
        & F.col("envelope.source").isin(*SOURCES)
        & (F.col("envelope.schema_version") == "1.0")
        & F.col("event_timestamp").isNotNull()
        & F.col("payload_json").rlike(r"^\s*\{")
    )
    return parsed.select(
        "raw_key",
        "raw_value",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_timestamp_type",
        F.col("envelope.event_id").alias("event_id"),
        F.col("envelope.timestamp").alias("source_timestamp"),
        "event_timestamp",
        F.col("envelope.schema_version").alias("schema_version"),
        "payload_json",
        F.when(F.coalesce(valid, F.lit(False)), "valid_envelope")
        .otherwise("invalid_envelope")
        .alias("record_status"),
        F.when(F.coalesce(valid, F.lit(False)), F.col("envelope.source"))
        .otherwise(F.lit("__invalid__"))
        .alias("source"),
        F.coalesce(
            F.date_format("kafka_timestamp", "yyyy-MM-dd"), F.lit("unknown")
        ).alias("ingest_date"),
    )


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "trendcast.events.validated")
    )
    parser.add_argument("--output", default="hdfs://namenode:9000/trendcast/raw/events")
    parser.add_argument(
        "--checkpoint",
        default="hdfs://namenode:9000/trendcast/checkpoints/streaming/bronze-events",
    )
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--trigger", default="30 seconds")
    args = parser.parse_args()
    validate_destinations(args.output, args.checkpoint)
    return args


def main():
    args = arguments()
    spark = SparkSession.builder.appName("TrendCast-Kafka-HDFS-Bronze").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        if not spark.sparkContext.master.startswith("spark://"):
            raise ValueError("This job must run on the Spark standalone cluster")
        incoming = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", "10000")
            .load()
        )
        writer = (
            bronze_records(incoming)
            .writeStream.format("parquet")
            .outputMode("append")
            .partitionBy("source", "ingest_date")
            .option("path", args.output)
            .option("checkpointLocation", args.checkpoint)
        )
        writer = (
            writer.trigger(availableNow=True)
            if args.available_now
            else writer.trigger(processingTime=args.trigger)
        )
        query = writer.start()
        last_batch = None
        while not query.awaitTermination(10):
            progress = query.lastProgress
            if progress and progress.get("batchId") != last_batch:
                print("BRONZE_PROGRESS=" + json.dumps(progress), flush=True)
                last_batch = progress.get("batchId")
        if query.lastProgress:
            print("BRONZE_PROGRESS=" + json.dumps(query.lastProgress), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
