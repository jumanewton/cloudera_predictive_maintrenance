"""
alerting/alert_consumer.py
Reads machine alert events from Kafka topic 'machine-alerts'
and prints them to the console + writes to InfluxDB.

Runs on the Windows HOST.  Ctrl+C to stop.
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
KAFKA_TOPIC    = "machine-alerts"
KAFKA_GROUP    = "alerts-handler"

INFLUX_URL     = "http://localhost:8086"
INFLUX_TOKEN   = "predmaint-super-token"
INFLUX_ORG     = "predmaint"
INFLUX_BUCKET  = "sensor-data"

DIVIDER = "=" * 70

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
        )
        print("OK")
        return c
    except NoBrokersAvailable:
        print("\nFAILED — check VM is running and port 9092 is forwarded.")
        sys.exit(1)

def connect_influx():
    print(f"Connecting to InfluxDB at {INFLUX_URL}...", end=" ")
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        client.ping()
        print("OK")
        return client.write_api(write_options=SYNCHRONOUS)
    except Exception:
        print("WARNING — InfluxDB not reachable; alerts will be console-only.")
        return None

def print_alert(alert: dict) -> None:
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    unit = alert.get("unit_id", "?")
    cycle= alert.get("time_cycle", "?")
    temp = alert.get("temp", 0)
    fan  = alert.get("fan_speed", 0)
    fuel = alert.get("fuel_flow", 0)
    reasons = alert.get("alert_reasons", [])

    print(f"\n{DIVIDER}")
    print(f"  🚨  MACHINE ALERT  —  {now}")
    print(DIVIDER)
    print(f"  Unit ID      : {unit}")
    print(f"  Cycle        : {cycle}")
    print(f"  Temperature  : {temp:.2f}  (threshold 645.0 R)")
    print(f"  Fan Speed    : {fan:.2f}   (threshold 9400 RPM)")
    print(f"  Fuel Flow    : {fuel:.2f}  (threshold 550.0)")
    if reasons:
        print(f"  Reasons      :")
        for r in reasons:
            print(f"    • {r}")
    print(DIVIDER)

def write_influx(alert: dict, write_api) -> None:
    if write_api is None:
        return
    try:
        point = (Point("machine_alert")
                 .tag("unit_id", str(alert.get("unit_id", "?")))
                 .field("temp",       float(alert.get("temp",       0)))
                 .field("fan_speed",  float(alert.get("fan_speed",  0)))
                 .field("fuel_flow",  float(alert.get("fuel_flow",  0)))
                 .field("alert_flag", 1)
                 .time(datetime.now(timezone.utc), WritePrecision.NS))
        write_api.write(bucket=INFLUX_BUCKET, record=point)
    except Exception as e:
        print(f"  [InfluxDB write error: {e}]")

if __name__ == "__main__":
    consumer  = connect_kafka()
    write_api = connect_influx()

    print(f"\n{DIVIDER}")
    print(f"  Alert Consumer running — listening on '{KAFKA_TOPIC}'")
    print(f"  Ctrl+C to stop")
    print(DIVIDER)

    alert_count = 0
    try:
        for message in consumer:
            alert = message.value
            alert_count += 1
            print_alert(alert)
            write_influx(alert, write_api)
    except KeyboardInterrupt:
        print(f"\nStopped. Total alerts received: {alert_count}")
