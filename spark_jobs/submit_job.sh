#!/usr/bin/env bash
# =================================================================
#  spark_jobs/submit_job.sh
#  Submits the PySpark Structured Streaming job to YARN.
#
#  Run this INSIDE the CDH 5.13 VM:
#    bash /home/cloudera/spark_jobs/submit_job.sh
#
#  Tuned for 8 GB VM RAM — uses 1 executor to leave room for
#  HDFS, Kafka, YARN overhead and other CDH services.
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOB_FILE="$SCRIPT_DIR/streaming_job.py"

# Use spark2-submit if available, otherwise fallback to spark-submit
if command -v spark2-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="spark2-submit"
else
  SPARK_SUBMIT="spark-submit"
fi

# Kafka + Spark SQL streaming package for Spark 2.2 / Scala 2.11
# The VM needs internet access or this JAR pre-downloaded.
# If no internet: download kafka JAR separately — see README.
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0"

echo "=== Predictive Maintenance — Spark Streaming Job ==="
echo "Job:    $JOB_FILE"
echo "Master: yarn (client mode)"
echo ""

$SPARK_SUBMIT \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 1500m \
  --driver-memory 512m \
  --executor-cores 2 \
  --packages "$KAFKA_PACKAGE" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.streaming.backpressure.enabled=true" \
  "$JOB_FILE"
