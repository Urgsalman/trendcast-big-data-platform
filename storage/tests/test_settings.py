import pytest

from storage.settings import validate_destinations


@pytest.mark.parametrize(
    "output,checkpoint",
    [
        ("file:///tmp/events", "hdfs://namenode:9000/trendcast/checkpoints/test"),
        (
            "hdfs://namenode:9000/trendcast/raw",
            "hdfs://namenode:9000/trendcast/raw/checkpoint",
        ),
        ("hdfs://namenode:9000/trendcast/raw", "hdfs://namenode:9000/trendcast/raw"),
        (
            "hdfs://namenode:9000/trendcast/raw/../events",
            "hdfs://namenode:9000/trendcast/checkpoints/test",
        ),
        (
            "hdfs://namenode:9000/other/events",
            "hdfs://namenode:9000/trendcast/checkpoints/test",
        ),
        (
            "hdfs://namenode:9000/trendcast/raw",
            "hdfs://other:9000/trendcast/checkpoints/test",
        ),
    ],
)
def test_refuses_unsafe_or_overlapping_storage_paths(output, checkpoint):
    with pytest.raises(ValueError):
        validate_destinations(output, checkpoint)


def test_accepts_distinct_hdfs_archive_and_checkpoint():
    validate_destinations(
        "hdfs://namenode:9000/trendcast/raw/events",
        "hdfs://namenode:9000/trendcast/checkpoints/streaming/bronze-events",
    )
