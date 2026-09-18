"""
alerting/metrics_consumer.py
Reads windowed sensor metrics from Kafka topic 'machine-metrics'
and writes them to InfluxDB for Grafana to visualise.

Runs on the Windows HOST (not inside the VM).
Connect to VM Kafka via port-forwarding (localhost:9092).
"""
import json
import sys
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ─── Config ──────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "machine-metrics"
KAFKA_GROUP    = "metrics-to-influx"

INFLUX_URL     = "http://localhost:8086"
INFLUX_TOKEN   = "predmaint-super-token"
INFLUX_ORG     = "predmaint"
INFLUX_BUCKET  = "sensor-data"

def connect_kafka() -> KafkaConsumer:
    print(f"Connecting to Kafka '{KAFKA_TOPIC}' on {KAFKA_BROKER}...", end=" ")
    try:
        c = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id=KAFKA_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=-1,
        )
        print("OK")
        return c
    except NoBrokersAvailable:
        print("\nFAILED — check VM is running and port 9092 is forwarded.")
        sys.exit(1)

def connect_influx():
    print(f"Connecting to InfluxDB at {INFLUX_URL}...", end=" ")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    # Quick health check
    try:
        client.ping()
        print("OK")
    except Exception:
        print("FAILED — is Docker running?  docker compose up -d")
        sys.exit(1)
    return write_api

def process_metric(msg: dict, write_api) -> None:
    unit_id = str(msg.get("unit_id", "?"))
    ts = datetime.now(timezone.utc)

    fields = {
        "avg_temp":        msg.get("avg_temp"),
        "avg_pressure":    msg.get("avg_pressure"),
        "avg_fan_speed":   msg.get("avg_fan_speed"),
        "std_fan_speed":   msg.get("std_fan_speed"),
        "max_fuel_flow":   msg.get("max_fuel_flow"),
        "avg_exhaust_temp":msg.get("avg_exhaust_temp"),
        "n_readings":      float(msg.get("n_readings", 0)),
    }
    # Drop None values
    fields = {k: v for k, v in fields.items() if v is not None}

    point = (Point("sensor_metrics")
             .tag("unit_id", unit_id)
             .time(ts, WritePrecision.NS))
    for field, val in fields.items():
        point = point.field(field, float(val))

    write_api.write(bucket=INFLUX_BUCKET, record=point)
    print(f"  Metrics unit={unit_id:>3}  "
          f"avg_temp={fields.get('avg_temp', 0):.2f}  "
          f"avg_fan_speed={fields.get('avg_fan_speed', 0):.2f}")

if __name__ == "__main__":
    consumer  = connect_kafka()
    write_api = connect_influx()
    print(f"\nListening for metrics on '{KAFKA_TOPIC}'...  (Ctrl+C to stop)\n")
    try:
        for message in consumer:
            process_metric(message.value, write_api)
    except KeyboardInterrupt:
        print("\nStopped.")
