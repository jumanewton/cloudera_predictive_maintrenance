"""
simulator/kafka_producer.py
Replays NASA CMAPSS sensor data as a live Kafka stream.

Connects to the CDH 5.13 VM Kafka via port-forwarding (localhost:9092).
Each row becomes a JSON message on the 'sensor-readings' topic.

Usage:
  python simulator/kafka_producer.py                # FD001, 20x speed
  python simulator/kafka_producer.py --file FD002 --speed 50
  python simulator/kafka_producer.py --help
"""
import argparse
import json
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── Column layout for CMAPSS ────────────────────────────────────────
SENSOR_COLS = [f"s{i}" for i in range(1, 22)]
OP_COLS     = ["op1", "op2", "op3"]
ALL_COLS    = ["unit_id", "time_cycle"] + OP_COLS + SENSOR_COLS

# ─── Thresholds for rule-based anomaly flagging (from CMAPSS EDA)
# s2  = Total temperature at fan inlet  (high = bad)
# s9  = Physical fan speed Nf           (irregular = bad)
# s14 = Corrected fan speed NRf         (low = degradation)
ALERT_RULES = {
    "s2":  {"min": None, "max": 650.0},     # degrees Rankine
    "s9":  {"min": None, "max": 9400.0},    # RPM
    "s14": {"min": 8100.0, "max": None},    # RPM (too low = bad)
}


def load_cmapss(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep=r"\s+", header=None, engine="python")
    df = df.dropna(axis=1, how="all")
    df.columns = ALL_COLS[: len(df.columns)]
    return df


def make_producer(broker: str) -> KafkaProducer:
    print(f"Connecting to Kafka at {broker} ...", end=" ")
    try:
        p = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            request_timeout_ms=10_000,
        )
        print("OK")
        return p
    except NoBrokersAvailable:
        print("\nFAILED — no brokers available.")
        print("  Check that:")
        print("  1. The CDH VM is running")
        print("  2. Port forwarding 9092 -> 9092 is set in VirtualBox")
        print("  3. You have run setup/fix_kafka_advertised_host.sh")
        sys.exit(1)


def check_alerts(sensors: dict) -> list[str]:
    triggered = []
    for sensor, rules in ALERT_RULES.items():
        val = sensors.get(sensor)
        if val is None:
            continue
        if rules["max"] is not None and val > rules["max"]:
            triggered.append(f"{sensor}={val:.2f} > max={rules['max']}")
        if rules["min"] is not None and val < rules["min"]:
            triggered.append(f"{sensor}={val:.2f} < min={rules['min']}")
    return triggered


def simulate(filepath: Path, topic: str, speed: float, broker: str,
             max_rows: int = None):
    df = load_cmapss(filepath)
    if max_rows:
        df = df.head(max_rows)

    producer = make_producer(broker)
    delay = 1.0 / speed          # seconds between messages
    total = len(df)
    sent  = 0

    print(f"\nStreaming {total} readings from {filepath.name} to '{topic}'")
    print(f"Speed: {speed}x  (1 msg every {delay*1000:.0f} ms)")
    print(f"Broker: {broker}\n")
    print(f"{'Unit':>5} {'Cycle':>6} {'T_fan(s2)':>10} {'FanSpd(s9)':>11} "
          f"{'FuelFlow(s12)':>13}  Flags")
    print("-" * 80)

    for _, row in df.iterrows():
        sensors = {c: round(float(row[c]), 4) for c in SENSOR_COLS if c in row.index}
        ops     = {c: round(float(row[c]), 4) for c in OP_COLS     if c in row.index}
        alerts  = check_alerts(sensors)

        msg = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "unit_id":   int(row["unit_id"]),
            "time_cycle": int(row["time_cycle"]),
            "operational": ops,
            "sensors":    sensors,
            "rule_alert": len(alerts) > 0,
            "alert_reasons": alerts,
        }

        producer.send(topic, value=msg)
        sent += 1

        flag_str = "  ALERT: " + " | ".join(alerts) if alerts else ""
        print(f"{int(row['unit_id']):>5} {int(row['time_cycle']):>6} "
              f"{sensors.get('s2', 0):>10.2f} {sensors.get('s9', 0):>11.2f} "
              f"{sensors.get('s12', 0):>13.2f}{flag_str}")

        time.sleep(delay)

    producer.flush()
    print(f"\nDone. {sent} messages sent to '{topic}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NASA CMAPSS → Kafka simulator")
    parser.add_argument("--file",   default="FD001",
                        help="Sub-dataset to stream: FD001/FD002/FD003/FD004 (default: FD001)")
    parser.add_argument("--speed",  type=float, default=20.0,
                        help="Replay speed multiplier (default: 20 = 50ms per reading)")
    parser.add_argument("--topic",  default="sensor-readings",
                        help="Kafka topic to publish to (default: sensor-readings)")
    parser.add_argument("--broker", default="localhost:9092",
                        help="Kafka broker address (default: localhost:9092)")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Limit rows for quick testing (default: all)")
    args = parser.parse_args()

    data_file = Path(f"data/cmapss/train_{args.file}.txt")
    if not data_file.exists():
        print(f"Dataset file not found: {data_file}")
        print("Run:  python simulator/download_dataset.py")
        sys.exit(1)

    simulate(data_file, args.topic, args.speed, args.broker, args.max_rows)
