# ================================================================
#  notebooks/01_eda_and_training.py
#  Run this in JupyterLab (http://localhost:8889, token: predmaint)
#  as a notebook, or directly:  python notebooks/01_eda_and_training.py
#
#  Steps:
#    1. Load and explore NASA CMAPSS FD001
#    2. Engineer features (rolling stats)
#    3. Label RUL (Remaining Useful Life)
#    4. Train: Isolation Forest (anomaly) + Random Forest (RUL regression)
#    5. Log everything to MLflow
#    6. Export best model as pickle for use in Spark job
# ================================================================

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import mlflow
import mlflow.sklearn
from pathlib import Path
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ─── Paths ──────────────────────────────────────────────────────────
DATA_DIR   = Path("/home/jovyan/data/cmapss")   # inside JupyterLab container
MODELS_DIR = Path("/home/jovyan/models")
MODELS_DIR.mkdir(exist_ok=True)

MLFLOW_URI = "http://host.docker.internal:5000"  # MLflow on host from inside Docker

# ─── Column names ───────────────────────────────────────────────────
SENSOR_COLS = [f"s{i}" for i in range(1, 22)]
OP_COLS     = ["op1", "op2", "op3"]
ALL_COLS    = ["unit_id", "time_cycle"] + OP_COLS + SENSOR_COLS

# ─── 1. Load data ───────────────────────────────────────────────────
print("Loading CMAPSS FD001...")
train = pd.read_csv(DATA_DIR / "train_FD001.txt", sep=r"\s+",
                    header=None, engine="python")
train = train.dropna(axis=1, how="all")
train.columns = ALL_COLS[:len(train.columns)]

test  = pd.read_csv(DATA_DIR / "test_FD001.txt",  sep=r"\s+",
                    header=None, engine="python")
test  = test.dropna(axis=1, how="all")
test.columns = ALL_COLS[:len(test.columns)]

rul_test = pd.read_csv(DATA_DIR / "RUL_FD001.txt", header=None, names=["RUL"])

print(f"Train shape: {train.shape}  |  Test shape: {test.shape}")
print(train.head(3))

# ─── 2. RUL labelling ───────────────────────────────────────────────
print("\nComputing RUL labels...")
max_cycle = train.groupby("unit_id")["time_cycle"].max().rename("max_cycle")
train = train.merge(max_cycle, on="unit_id")
train["RUL"] = train["max_cycle"] - train["time_cycle"]
train.drop(columns=["max_cycle"], inplace=True)

# Clip RUL at 125 (early cycles have perfect health — cap RUL)
train["RUL"] = train["RUL"].clip(upper=125)
print(f"RUL distribution:\n{train['RUL'].describe()}")

# ─── 3. Drop constant sensors (no variance in FD001) ────────────────
DROP_SENSORS = ["s1", "s5", "s6", "s10", "s16", "s18", "s19"]
train.drop(columns=DROP_SENSORS, errors="ignore", inplace=True)
test.drop( columns=DROP_SENSORS, errors="ignore", inplace=True)
SENSOR_COLS = [c for c in SENSOR_COLS if c not in DROP_SENSORS]
print(f"\nUsing {len(SENSOR_COLS)} sensors: {SENSOR_COLS}")

# ─── 4. Rolling window features (10-cycle window) ───────────────────
print("\nEngineering rolling features...")
ROLL_WINDOW = 10
for col in SENSOR_COLS:
    train[f"{col}_rmean"] = (train.groupby("unit_id")[col]
                              .transform(lambda x: x.rolling(ROLL_WINDOW, min_periods=1).mean()))
    train[f"{col}_rstd"]  = (train.groupby("unit_id")[col]
                              .transform(lambda x: x.rolling(ROLL_WINDOW, min_periods=1)
                                                    .std().fillna(0)))
FEATURE_COLS = (OP_COLS +
                SENSOR_COLS +
                [f"{c}_rmean" for c in SENSOR_COLS] +
                [f"{c}_rstd"  for c in SENSOR_COLS])
FEATURE_COLS = [c for c in FEATURE_COLS if c in train.columns]
print(f"Total feature count: {len(FEATURE_COLS)}")

# ─── 5. Scale features ──────────────────────────────────────────────
X = train[FEATURE_COLS].fillna(0).values
y = train["RUL"].values

scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)

X_train, X_val, y_train, y_val = train_test_split(
    X_scaled, y, test_size=0.2, random_state=42)

# ─── 6. MLflow setup ────────────────────────────────────────────────
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("predmaint-cmapss-fd001")
print(f"\nMLflow tracking URI: {MLFLOW_URI}")

# ─── 7. Train Isolation Forest (anomaly detection) ──────────────────
print("\nTraining Isolation Forest...")
with mlflow.start_run(run_name="IsolationForest-v1"):
    iso = IsolationForest(n_estimators=200, contamination=0.05,
                          max_samples="auto", random_state=42)
    iso.fit(X_scaled)

    scores      = iso.decision_function(X_scaled)
    predictions = iso.predict(X_scaled)
    anomaly_pct = (predictions == -1).mean() * 100

    mlflow.log_params({
        "n_estimators":  200,
        "contamination": 0.05,
        "window":        ROLL_WINDOW,
        "features":      len(FEATURE_COLS),
    })
    mlflow.log_metric("anomaly_pct",    anomaly_pct)
    mlflow.log_metric("mean_score",     float(scores.mean()))
    mlflow.log_metric("min_score",      float(scores.min()))

    mlflow.sklearn.log_model(iso, "isolation_forest",
                              registered_model_name="PredMaint-IsolationForest")
    mlflow.sklearn.log_model(scaler, "scaler",
                              registered_model_name="PredMaint-Scaler")

    print(f"  Anomalies flagged: {anomaly_pct:.1f}%  |  mean score: {scores.mean():.4f}")

# Save locally for Spark job to load
joblib.dump(iso,    MODELS_DIR / "isolation_forest.pkl")
joblib.dump(scaler, MODELS_DIR / "scaler.pkl")
print(f"  Models saved to {MODELS_DIR}")

# ─── 8. Train Random Forest for RUL prediction ──────────────────────
print("\nTraining Random Forest RUL predictor...")
with mlflow.start_run(run_name="RandomForest-RUL-v1"):
    rf = RandomForestRegressor(n_estimators=300, max_depth=15,
                                min_samples_leaf=5, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)

    val_preds = rf.predict(X_val)
    mae  = mean_absolute_error(y_val, val_preds)
    rmse = np.sqrt(mean_squared_error(y_val, val_preds))

    mlflow.log_params({
        "n_estimators":    300,
        "max_depth":       15,
        "min_samples_leaf":5,
        "window":          ROLL_WINDOW,
    })
    mlflow.log_metric("val_MAE_cycles",  mae)
    mlflow.log_metric("val_RMSE_cycles", rmse)

    mlflow.sklearn.log_model(rf, "random_forest_rul",
                              registered_model_name="PredMaint-RUL-RF")

    print(f"  Validation MAE:  {mae:.1f} cycles")
    print(f"  Validation RMSE: {rmse:.1f} cycles")
    print(f"  (Cycles = remaining engine life before failure)")

joblib.dump(rf, MODELS_DIR / "rf_rul.pkl")
print(f"  RF model saved to {MODELS_DIR}")

# ─── 9. Quick feature importance plot ───────────────────────────────
importances = pd.Series(rf.feature_importances_, index=FEATURE_COLS)
top20 = importances.nlargest(20)

fig, ax = plt.subplots(figsize=(10, 6))
top20.sort_values().plot(kind="barh", ax=ax, color="steelblue")
ax.set_title("Top 20 Feature Importances (Random Forest RUL)")
ax.set_xlabel("Importance")
plt.tight_layout()
plt.savefig(MODELS_DIR / "feature_importance.png", dpi=100)
print(f"\nFeature importance plot saved to {MODELS_DIR / 'feature_importance.png'}")

print("\n=== Training Complete ===")
print(f"  Isolation Forest: {MODELS_DIR / 'isolation_forest.pkl'}")
print(f"  Scaler:           {MODELS_DIR / 'scaler.pkl'}")
print(f"  RF RUL predictor: {MODELS_DIR / 'rf_rul.pkl'}")
print(f"\nOpen MLflow at http://localhost:5000 to review runs.")
