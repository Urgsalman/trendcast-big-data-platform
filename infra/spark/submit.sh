#!/usr/bin/env bash
set -euo pipefail
# A one-off Compose job has its own IP; workers must reach that actual driver.
driver_ip="$(hostname -i)"
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 768m --executor-memory 512m \
  --conf "spark.driver.host=${driver_ip}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.cores=1 --conf spark.cores.max=2 \
  --conf spark.sql.shuffle.partitions=3 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.jars.ivySettings=/opt/trendcast/ivysettings.xml \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  "/opt/trendcast/storage/${TRENDCAST_JOB:-kafka_to_bronze.py}" "$@"
