"""
spark_jobs/stream_processor.py
Real-time Stream Processor for Predictive Maintenance.

Consumes: 'sensor-readings' from Kafka (localhost:9092)
Processes:
  - 5-minute sliding window aggregations
  - Anomaly & threshold detection
Produces:
  - Windowed metrics -> 'machine-metrics' (for InfluxDB & Grafana)
  - Failure alerts   -> 'machine-alerts'  (for Alert Consumer)
  - Parquet records  -> 'data/sensor_raw/' (local data lake)
"""
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER  = "localhost:9092"
INPUT_TOPIC   = "sensor-readings"
METRICS_TOPIC = "machine-metrics"
ALERTS_TOPIC  = "machine-alerts"

PARQUET_DIR   = Path("data/sensor_raw")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# Failure thresholds
TEMP_HIGH      = 645.0
FAN_SPEED_HIGH = 9400.0
FUEL_FLOW_HIGH = 550.0

WINDOW_SECONDS = 300  # 5-minute sliding window
EMIT_INTERVAL  = 5    # Emit metrics every 5 seconds

def run_processor():
    print(f"=== Predictive Maintenance Stream Processor ===")
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="stream-processor-group",
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Listening for sensor readings...")
    
    # Store sliding window per unit: deque of (timestamp, data_dict)
    windows = defaultdict(lambda: deque())
    raw_buffer = []
    last_emit = time.time()

    for message in consumer:
        reading = message.value
        unit_id = reading["unit_id"]
        cycle = reading["time_cycle"]
        sensors = reading["sensors"]
        now = time.time()

        temp = sensors.get("s2", 0)
        pressure = sensors.get("s7", 0)
        fan_speed = sensors.get("s9", 0)
        fuel_flow = sensors.get("s12", 0)

        # 1. Anomaly & Alert Checking
        reasons = []
        if temp > TEMP_HIGH:
            reasons.append(f"High Temperature: {temp:.2f} > {TEMP_HIGH}")
        if fan_speed > FAN_SPEED_HIGH:
            reasons.append(f"High Fan Speed: {fan_speed:.2f} > {FAN_SPEED_HIGH}")
        if fuel_flow > FUEL_FLOW_HIGH:
            reasons.append(f"Excess Fuel Flow: {fuel_flow:.2f} > {FUEL_FLOW_HIGH}")

        if reasons:
            alert = {
                "timestamp": reading.get("timestamp", datetime.now(timezone.utc).isoformat()),
                "unit_id": unit_id,
                "time_cycle": cycle,
                "temp": temp,
                "fan_speed": fan_speed,
                "fuel_flow": fuel_flow,
                "alert_reasons": reasons
            }
            producer.send(ALERTS_TOPIC, value=alert)

        # 2. Add to sliding window
        w = windows[unit_id]
        w.append((now, {"temp": temp, "pressure": pressure, "fan_speed": fan_speed, "fuel_flow": fuel_flow}))

        # Evict old data outside the window
        while w and w[0][0] < (now - WINDOW_SECONDS):
            w.popleft()

        # Buffer for Parquet lake
        raw_buffer.append({
            "timestamp": reading.get("timestamp"),
            "unit_id": unit_id,
            "cycle": cycle,
            "temp": temp,
            "pressure": pressure,
            "fan_speed": fan_speed,
            "fuel_flow": fuel_flow
        })

        # 3. Periodically emit windowed metrics
        if now - last_emit >= EMIT_INTERVAL:
            for uid, items in list(windows.items()):
                if not items:
                    continue
                temps = [x[1]["temp"] for x in items]
                pressures = [x[1]["pressure"] for x in items]
                fans = [x[1]["fan_speed"] for x in items]
                fuels = [x[1]["fuel_flow"] for x in items]

                n = len(temps)
                avg_temp = sum(temps) / n
                avg_pressure = sum(pressures) / n
                avg_fan = sum(fans) / n
                max_fuel = max(fuels)

                metrics_msg = {
                    "unit_id": uid,
                    "avg_temp": avg_temp,
                    "avg_pressure": avg_pressure,
                    "avg_fan_speed": avg_fan,
                    "std_fan_speed": (sum((x - avg_fan)**2 for x in fans) / n)**0.5 if n > 1 else 0.0,
                    "max_fuel_flow": max_fuel,
                    "n_readings": n
                }
                producer.send(METRICS_TOPIC, value=metrics_msg)

            # Flush Parquet buffer
            if len(raw_buffer) >= 50:
                df = pd.DataFrame(raw_buffer)
                part_file = PARQUET_DIR / f"sensors_{int(now)}.parquet"
                df.to_parquet(part_file, index=False)
                raw_buffer.clear()

            producer.flush()
            last_emit = now

if __name__ == "__main__":
    run_processor()
