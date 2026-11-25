"""Conventions communes aux jobs Spark Wikimedia."""

import json

from pyspark.sql import SparkSession

GOLD_ROOT = "hdfs://namenode:9000/trendcast/analytics"
DATASET_ROOT = f"{GOLD_ROOT}/wikimedia_supervised"
MODEL_ROOT = "hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views"
TARGET = "target_next_day_views"
SERIES = ("project", "page_title")
KEY = (*SERIES, "event_date")


def start_spark(job_name):
    spark = SparkSession.builder.appName(job_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    master = spark.sparkContext.master
    if not master.startswith("spark://"):
        spark.stop()
        raise RuntimeError(f"Master Spark distribué requis, reçu : {master}")
    print(
        f"SPARK_MASTER={master} APP_ID={spark.sparkContext.applicationId}", flush=True
    )
    return spark


def ensure_new_hdfs_path(spark, uri):
    if not uri.startswith("hdfs://"):
        raise ValueError(f"Chemin HDFS absolu requis : {uri}")
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        raise FileExistsError(f"Version déjà présente : {uri}")


def write_manifest(spark, path, payload):
    serialized = json.dumps(payload, ensure_ascii=False, default=str, allow_nan=False)
    spark.createDataFrame([(serialized,)], ["value"]).coalesce(1).write.text(path)


def read_manifest(spark, path):
    return json.loads(spark.read.text(path).first().value)


def print_manifest(label, payload):
    print(f"{label}={json.dumps(payload, default=str, allow_nan=False)}", flush=True)
