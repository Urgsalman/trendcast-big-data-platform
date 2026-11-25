"""Read archived Parquet on the cluster and assert the acceptance criteria."""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kafka_to_bronze import bronze_records


def verify_transform(spark):
    base = {
        "event_id": "snapshot:1",
        "source": "hackernews",
        "timestamp": "2026-11-19T12:00:00Z",
        "schema_version": "1.0",
        "payload": {"score": 10, "nested": {"keep": [1, 2]}},
    }
    rows = []
    samples = [
        json.dumps(base).encode(),
        json.dumps(base).encode(),
        b"not-json",
        None,
        json.dumps({**base, "source": "unknown"}).encode(),
        json.dumps({**base, "timestamp": "bad-date"}).encode(),
    ]
    for offset, raw in enumerate(samples):
        rows.append(
            (
                b"same-key",
                raw,
                "fixture",
                0,
                offset,
                datetime(2026, 11, 19, tzinfo=timezone.utc),
                0,
            )
        )
    schema = T.StructType(
        [
            T.StructField("key", T.BinaryType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("timestampType", T.IntegerType()),
        ]
    )
    # Even ANSI mode must not cause malformed timestamps to drop the whole batch.
    spark.conf.set("spark.sql.ansi.enabled", "true")
    result = bronze_records(spark.createDataFrame(rows, schema)).collect()
    assert len(result) == 6
    indexed = {row.kafka_offset: row for row in result}
    assert indexed[0].record_status == "valid_envelope"
    assert indexed[1].record_status == "valid_envelope"
    assert indexed[0].event_id == indexed[1].event_id
    for offset in range(2, 6):
        assert indexed[offset].record_status == "invalid_envelope"
        assert indexed[offset].source == "__invalid__"
    for offset, raw in enumerate(samples):
        value = indexed[offset].raw_value
        assert (bytes(value) if value is not None else None) == raw
    assert json.loads(indexed[0].payload_json)["nested"]["keep"] == [1, 2]
    assert indexed[0].ingest_date == "2026-11-19"
    print("TRANSFORM_CHECK=passed (6 records, malformed and repeated IDs preserved)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--check-transform", action="store_true")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("TrendCast-Verify-Bronze").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        assert spark.sparkContext.master.startswith("spark://")
        if args.check_transform:
            verify_transform(spark)
        archived = spark.read.parquet(args.path)
        count = archived.count()
        unique_positions = (
            archived.select("kafka_topic", "kafka_partition", "kafka_offset")
            .distinct()
            .count()
        )
        assert count == args.expected_count, (count, args.expected_count)
        assert unique_positions == count, "A Kafka position was archived more than once"
        by_source = {
            row.source: row["count"]
            for row in archived.groupBy("source").count().collect()
        }
        statuses = {
            row.record_status: row["count"]
            for row in archived.groupBy("record_status").count().collect()
        }
        print(
            "BRONZE_VERIFICATION="
            + json.dumps(
                {
                    "count": count,
                    "unique_kafka_positions": unique_positions,
                    "sources": by_source,
                    "statuses": statuses,
                    "master": spark.sparkContext.master,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
