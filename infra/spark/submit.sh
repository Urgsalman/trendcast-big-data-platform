#!/usr/bin/env bash
set -euo pipefail

driver_ip="$(hostname -i)"
job="${TRENDCAST_JOB:-kafka_to_bronze.py}"

case "$job" in
  kafka_to_bronze.py|verify_bronze.py|audit_bronze.py)
    job_path="/opt/trendcast/storage/$job"
    ;;
  bronze_to_silver.py|silver_quality_checks.py)
    job_path="/opt/trendcast/processing/$job"
    ;;
  wikimedia.py|hackernews.py|google_trends.py)
    job_path="/opt/trendcast/processing/silver/$job"
    ;;
  build_gold.py|wikimedia_analytics.py|hackernews_analytics.py|google_trends_analytics.py)
    job_path="/opt/trendcast/analytics/$job"
    ;;
  *)
    echo "Job non autorisé : $job" >&2
    exit 1
    ;;
esac

if [[ ! -f "$job_path" ]]; then
  echo "Fichier introuvable : $job_path" >&2
  exit 1
fi

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
  "$job_path" "$@"