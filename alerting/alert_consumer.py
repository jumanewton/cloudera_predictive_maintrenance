"""
alerting/alert_consumer.py
Reads machine alert events from Kafka topic 'machine-alerts'
and prints them to the console + writes to InfluxDB.

Runs on the Windows HOST.  Ctrl+C to stop.
"""
import json
import sys
import selectors
from datetime import datetime, timezone

# Windows Python 3.13 patch for closed sockets with fileno == -1
def _patch_selectors():
    orig_unregister = selectors.SelectSelector.unregister
    def safe_unregister(self, fileobj):
        try:
            return orig_unregister(self, fileobj)
        except (ValueError, KeyError):
            for fd, key in list(self._fd_to_key.items()):
                if key.fileobj is fileobj:
                    return self._fd_to_key.pop(fd, None)
            return None
    selectors.SelectSelector.unregister = safe_unregister

_patch_selectors()

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
    rul = alert.get("predicted_rul", -1)
    anom = alert.get("anomaly_score", 0.0)

    print(f"\n{DIVIDER}")
    print(f"  🚨  PREDICTIVE MACHINE ALERT  —  {now}")
    print(DIVIDER)
    print(f"  Machine Unit   : {unit}")
    print(f"  Current Cycle  : {cycle}")
    if rul != -1:
        print(f"  PREDICTED RUL  : {rul:.1f} cycles remaining before failure! ⏳")
    print(f"  Anomaly Score  : {anom:.4f}")
    print(f"  Temperature    : {temp:.2f} R")
    print(f"  Fan Speed      : {fan:.2f} RPM")
    reasons = alert.get("alert_reasons", [])
    if reasons:
        print(f"  Triggered Signals :")
        for r in reasons:
            print(f"    ⚠️  {r}")
    print(DIVIDER)

def write_influx(alert: dict, write_api) -> None:
    if write_api is None:
        return
    try:
        rul = float(alert.get("predicted_rul", -1))
        point = (Point("machine_alert")
                 .tag("unit_id", str(alert.get("unit_id", "?")))
                 .field("temp",          float(alert.get("temp",       0)))
                 .field("fan_speed",     float(alert.get("fan_speed",  0)))
                 .field("fuel_flow",     float(alert.get("fuel_flow",  0)))
                 .field("predicted_rul", rul)
                 .field("alert_flag",    1)
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
