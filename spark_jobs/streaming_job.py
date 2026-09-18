"""
spark_jobs/streaming_job.py
PySpark Structured Streaming — CDH 5.13 / Spark 2.x

Architecture:
  Kafka(sensor-readings) --> Spark Streaming
    --> [1] Parquet raw store --> HDFS
    --> [2] Windowed metrics  --> Kafka(machine-metrics)
    --> [3] Rule-based alerts --> Kafka(machine-alerts)

NOTE: Copy this file into the CDH VM, then run via spark_jobs/submit_job.sh
"""
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    window, avg, stddev, max as spark_max, min as spark_min,
    count, expr, current_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType, MapType, ArrayType
)

# ═══════════════ CONFIGURATION ═══════════════════════════════════════
KAFKA_BROKER   = "10.0.2.2:9094"        # Kafka running on Windows host (Docker)
INPUT_TOPIC    = "sensor-readings"      # Raw sensor stream (from host)
METRICS_TOPIC  = "machine-metrics"     # Windowed aggregates out
ALERTS_TOPIC   = "machine-alerts"      # Anomaly alerts out

HDFS_RAW       = "hdfs:///user/cloudera/predmaint/sensor_raw"
HDFS_CHK_RAW   = "hdfs:///user/cloudera/predmaint/chk/raw"
HDFS_CHK_MET   = "hdfs:///user/cloudera/predmaint/chk/metrics"
HDFS_CHK_ALT   = "hdfs:///user/cloudera/predmaint/chk/alerts"

# Rule-based thresholds (tuned from CMAPSS FD001 EDA)
# Adjust these after running the training notebook
TEMP_HIGH      = 645.0      # s2  fan inlet temperature (Rankine)
FAN_SPEED_HIGH = 9400.0     # s9  physical fan speed
FUEL_FLOW_HIGH = 550.0      # s12 fuel flow
# ═════════════════════════════════════════════════════════════════════

# ─── Schema for incoming JSON messages ───────────────────────────────
sensor_schema = StructType([
    StructField("timestamp",      StringType(),  True),
    StructField("unit_id",        IntegerType(), True),
    StructField("time_cycle",     IntegerType(), True),
    StructField("operational",    MapType(StringType(), DoubleType()), True),
    StructField("sensors",        MapType(StringType(), DoubleType()), True),
    StructField("rule_alert",     BooleanType(), True),
    StructField("alert_reasons",  ArrayType(StringType()), True),
])

# ─── Spark Session — tuned for 8 GB VM RAM ───────────────────────────
spark = (SparkSession.builder
         .appName("PredMaint-StructuredStreaming")
         # Memory: 1 executor x 1.5 GB (leaves room for HDFS + Kafka)
         .config("spark.executor.memory",          "1500m")
         .config("spark.driver.memory",            "512m")
         .config("spark.sql.shuffle.partitions",   "4")
         .config("spark.streaming.backpressure.enabled", "true")
         .config("spark.streaming.kafka.maxRatePerPartition", "100")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("Spark session started. Listening to Kafka...")

# ─── 1. Read raw stream from Kafka ───────────────────────────────────
raw_kafka = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BROKER)
             .option("subscribe",               INPUT_TOPIC)
             .option("startingOffsets",         "latest")
             .option("failOnDataLoss",          "false")
             .option("maxOffsetsPerTrigger",    "500")
             .load())

# ─── 2. Parse JSON ───────────────────────────────────────────────────
parsed = (raw_kafka
          .selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), sensor_schema).alias("d"))
          .select("d.*"))

# ─── 3. Extract key sensor columns ───────────────────────────────────
flat = (parsed
        .withColumn("ts",          col("timestamp").cast("timestamp"))
        .withColumn("temp",        col("sensors")["s2"])
        .withColumn("pressure",    col("sensors")["s7"])
        .withColumn("fan_speed",   col("sensors")["s9"])
        .withColumn("fuel_flow",   col("sensors")["s12"])
        .withColumn("exhaust_temp",col("sensors")["s4"])
        .withColumn("op1",         col("operational")["op1"]))

# ─── 4. Rule-based alert flag ────────────────────────────────────────
with_alerts = flat.withColumn(
    "is_alert",
    (col("temp") > lit(TEMP_HIGH)) |
    (col("fan_speed") > lit(FAN_SPEED_HIGH)) |
    (col("fuel_flow") > lit(FUEL_FLOW_HIGH))
)

# ─── 5. Windowed aggregation (5-min windows, 1-min slide) ────────────
windowed = (flat
            .withWatermark("ts", "2 minutes")
            .groupBy(
                window(col("ts"), "5 minutes", "1 minute"),
                col("unit_id")
            )
            .agg(
                avg("temp").alias("avg_temp"),
                avg("pressure").alias("avg_pressure"),
                avg("fan_speed").alias("avg_fan_speed"),
                stddev("fan_speed").alias("std_fan_speed"),
                spark_max("fuel_flow").alias("max_fuel_flow"),
                avg("exhaust_temp").alias("avg_exhaust_temp"),
                count("*").alias("n_readings")
            ))

# ─── OUTPUT STREAM 1: Raw sensor data → HDFS (Parquet) ───────────────
hdfs_query = (flat.writeStream
              .outputMode("append")
              .format("parquet")
              .option("path",              HDFS_RAW)
              .option("checkpointLocation", HDFS_CHK_RAW)
              .trigger(processingTime="30 seconds")
              .start())

# ─── OUTPUT STREAM 2: Windowed metrics → Kafka machine-metrics ───────
metrics_payload = windowed.select(
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("unit_id"),
        col("avg_temp"),
        col("avg_pressure"),
        col("avg_fan_speed"),
        col("std_fan_speed"),
        col("max_fuel_flow"),
        col("avg_exhaust_temp"),
        col("n_readings")
    )).alias("value")
)

metrics_query = (metrics_payload.writeStream
                 .outputMode("update")
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BROKER)
                 .option("topic",                   METRICS_TOPIC)
                 .option("checkpointLocation",      HDFS_CHK_MET)
                 .trigger(processingTime="30 seconds")
                 .start())

# ─── OUTPUT STREAM 3: Alert events → Kafka machine-alerts ────────────
alerts_df = with_alerts.filter(col("is_alert") == True)

alert_payload = alerts_df.select(
    to_json(struct(
        col("ts").cast("string").alias("timestamp"),
        col("unit_id"),
        col("time_cycle"),
        col("temp"),
        col("fan_speed"),
        col("fuel_flow"),
        col("rule_alert"),
        col("alert_reasons")
    )).alias("value")
)

alerts_query = (alert_payload.writeStream
                .outputMode("append")
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("topic",                   ALERTS_TOPIC)
                .option("checkpointLocation",      HDFS_CHK_ALT)
                .trigger(processingTime="10 seconds")
                .start())

print("All 3 output streams started:")
print("  [1] Raw → HDFS:              " + HDFS_RAW)
print("  [2] Metrics → Kafka topic:   " + METRICS_TOPIC)
print("  [3] Alerts  → Kafka topic:   " + ALERTS_TOPIC)

spark.streams.awaitAnyTermination()
