"""
spark_jobs/stream_processor.py
Real-time Stream Processor with Integrated Machine Learning (RUL + Anomaly Detection).

Consumes: 'sensor-readings' from Kafka (localhost:9092)
ML Models:
  - models/rf_rul.pkl (Random Forest RUL Regressor - MAE 8.8 cycles)
  - models/scaler.pkl (MinMaxScaler for 45 features)
  - models/isolation_forest.pkl (Anomaly detector)

Produces:
  - Live AI-Predicted RUL & Failure Alerts -> 'machine-alerts'
  - Sliding window aggregations & AI metrics -> 'machine-metrics' (for InfluxDB/Grafana)
  - Parquet data lake -> 'data/sensor_raw/'
"""
import json
import time
import selectors
import joblib
import numpy as np
import pandas as pd
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

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

from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER  = "localhost:9092"
INPUT_TOPIC   = "sensor-readings"
METRICS_TOPIC = "machine-metrics"
ALERTS_TOPIC  = "machine-alerts"

PARQUET_DIR   = Path("data/sensor_raw")
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR    = Path("models")

# Feature schema matching 01_eda_and_training.py
ACTIVE_SENSORS = ['s2', 's3', 's4', 's7', 's8', 's9', 's11', 's12', 's13', 's14', 's15', 's17', 's20', 's21']
OP_SETTINGS    = ['op1', 'op2', 'op3']

# Thresholds
RUL_CRITICAL_THRESHOLD = 30.0  # Alert when machine has <= 30 cycles left!
EMIT_INTERVAL          = 3     # Emit metrics every 3 seconds
WINDOW_SIZE            = 10    # 10 cycles for rolling mean/std

def load_models():
    try:
        rf = joblib.load(MODELS_DIR / "rf_rul.pkl")
        scaler = joblib.load(MODELS_DIR / "scaler.pkl")
        iso = joblib.load(MODELS_DIR / "isolation_forest.pkl")
        print("✅ ML Models Loaded: RF RUL Regressor, Scaler, and Isolation Forest ready!")
        return rf, scaler, iso
    except Exception as e:
        print(f"⚠️ Warning: Could not load ML models: {e}. Falling back to rule-based.")
        return None, None, None

def build_feature_vector(unit_history, current_ops, current_sensors):
    """Builds the exact 45-feature vector required by the trained model."""
    feats = []
    # 1. Operational settings (3)
    for op in OP_SETTINGS:
        feats.append(current_ops.get(op, 0.0))
    # 2. Raw current sensors (14)
    for s in ACTIVE_SENSORS:
        feats.append(current_sensors.get(s, 0.0))
    # 3. Rolling means (14)
    for s in ACTIVE_SENSORS:
        vals = [h[s] for h in unit_history if s in h]
        feats.append(float(np.mean(vals)) if vals else current_sensors.get(s, 0.0))
    # 4. Rolling stds (14)
    for s in ACTIVE_SENSORS:
        vals = [h[s] for h in unit_history if s in h]
        feats.append(float(np.std(vals)) if len(vals) > 1 else 0.0)
    return np.array([feats], dtype=np.float64)

def run_processor():
    print(f"=== Real-Time AI Stream Processor ===")
    rf_model, scaler, iso_model = load_models()

    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="ai-stream-processor-group",
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("🚀 Ingestion & Live AI Scoring active. Listening for sensor events...\n")
    
    # Store rolling history per machine: unit_id -> deque of sensor dicts (max 10)
    history = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
    latest_predictions = {}
    raw_buffer = []
    last_emit = time.time()

    for message in consumer:
        reading = message.value
        unit_id = reading["unit_id"]
        cycle = reading["time_cycle"]
        sensors = reading.get("sensors", {})
        ops = reading.get("operational", {})
        now = time.time()

        # Update history
        history[unit_id].append(sensors)

        # ─── Live ML Inference ──────────────────────────────────────────
        pred_rul = None
        anomaly_score = 0.0
        is_anomaly = False

        if rf_model is not None and scaler is not None:
            try:
                X = build_feature_vector(history[unit_id], ops, sensors)
                X_scaled = scaler.transform(X)
                pred_rul = round(float(rf_model.predict(X_scaled)[0]), 1)
                anomaly_score = round(float(iso_model.decision_function(X_scaled)[0]), 4)
                is_anomaly = bool(iso_model.predict(X_scaled)[0] == -1)
                latest_predictions[unit_id] = (pred_rul, anomaly_score)
            except Exception as e:
                pass

        # ─── Failure & Anomaly Detection ────────────────────────────────
        alerts = []
        # AI-predicted failure
        if pred_rul is not None and pred_rul <= RUL_CRITICAL_THRESHOLD:
            alerts.append(f"AI PREDICTION: Machine breakdown imminent in {pred_rul:.1f} cycles!")
        # Statistical anomaly
        if is_anomaly:
            alerts.append(f"AI ANOMALY: Abnormal multivariate degradation detected (score: {anomaly_score:.3f})")
        # Rule threshold check
        temp = sensors.get("s2", 0)
        fan_speed = sensors.get("s9", 0)
        if temp > 645.0:
            alerts.append(f"Critical Temperature: {temp:.2f} R > 645.0 R")
        if fan_speed > 9400.0:
            alerts.append(f"Excess Fan Speed: {fan_speed:.2f} RPM > 9400.0 RPM")

        if alerts:
            alert_msg = {
                "timestamp": reading.get("timestamp", datetime.now(timezone.utc).isoformat()),
                "unit_id": unit_id,
                "time_cycle": cycle,
                "predicted_rul": pred_rul if pred_rul is not None else -1,
                "anomaly_score": anomaly_score,
                "temp": temp,
                "fan_speed": fan_speed,
                "fuel_flow": sensors.get("s12", 0),
                "alert_reasons": alerts
            }
            producer.send(ALERTS_TOPIC, value=alert_msg)

        # Buffer for Parquet lake
        raw_buffer.append({
            "timestamp": reading.get("timestamp"),
            "unit_id": unit_id,
            "cycle": cycle,
            "temp": temp,
            "fan_speed": fan_speed,
            "predicted_rul": pred_rul if pred_rul is not None else -1
        })

        # ─── Periodic Metrics Push (to InfluxDB / Grafana) ──────────────
        if now - last_emit >= EMIT_INTERVAL:
            for uid, (last_rul, last_anom) in list(latest_predictions.items()):
                recent = list(history[uid])
                temps = [x.get("s2", 0) for x in recent if "s2" in x]
                fans = [x.get("s9", 0) for x in recent if "s9" in x]

                metrics_msg = {
                    "unit_id": uid,
                    "avg_temp": float(np.mean(temps)) if temps else 0.0,
                    "avg_fan_speed": float(np.mean(fans)) if fans else 0.0,
                    "predicted_rul": float(last_rul),
                    "anomaly_score": float(last_anom),
                    "n_readings": len(recent)
                }
                producer.send(METRICS_TOPIC, value=metrics_msg)

            # Flush to Parquet file
            if len(raw_buffer) >= 50:
                df = pd.DataFrame(raw_buffer)
                part_file = PARQUET_DIR / f"sensors_{int(now)}.parquet"
                df.to_parquet(part_file, index=False)
                raw_buffer.clear()

            producer.flush()
            last_emit = now

if __name__ == "__main__":
    run_processor()
