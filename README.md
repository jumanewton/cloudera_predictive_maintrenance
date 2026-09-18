# Predictive Maintenance — Big Data Project
## CDH 5.13 QuickStart VM + Docker Host Stack

Real-time machine failure prediction using NASA CMAPSS sensor data,
Apache Kafka, PySpark Structured Streaming, and Grafana dashboards.

---

## Architecture

```
[NASA CMAPSS] → [Python Simulator] → [Kafka: sensor-readings]
                                              |
                                    [PySpark Streaming @ CDH VM]
                                    /           |             \
                          [HDFS Parquet]  [Kafka:metrics]  [Kafka:alerts]
                                                |                |
                                       [metrics_consumer]  [alert_consumer]
                                                |                |
                                           [InfluxDB]       [Console]
                                                |
                                           [Grafana]
```

---

## Service URLs

| Service          | URL                      | Login              |
|------------------|--------------------------|--------------------|
| Cloudera Manager | http://localhost:7180     | admin / admin      |
| HDFS UI          | http://localhost:50070    | —                  |
| YARN UI          | http://localhost:8088     | —                  |
| Hue              | http://localhost:8888     | cloudera / cloudera|
| JupyterLab       | http://localhost:8889     | token: predmaint   |
| MLflow           | http://localhost:5000     | —                  |
| InfluxDB         | http://localhost:8086     | admin / adminpassword |
| Grafana          | http://localhost:3000     | admin / admin      |

---

## Quick Start

### Step 1 — VirtualBox Port Forwarding
Add these rules: VM Settings > Network > Adapter 1 (NAT) > Port Forwarding
| Host Port | Guest Port | Service       |
|-----------|------------|---------------|
| 7180      | 7180       | Cloudera Mgr  |
| 50070     | 50070      | HDFS UI       |
| 8088      | 8088       | YARN UI       |
| 8888      | 8888       | Hue           |
| 9092      | 9092       | Kafka         |
| 2181      | 2181       | Zookeeper     |

### Step 2 — Start Docker host services
```powershell
cd d:\cloudera_test1
docker compose up -d
```

### Step 3 — Prepare CDH VM (run INSIDE the VM)
```bash
# Install Python 3.6
sudo yum install -y epel-release python36 python36-pip
sudo pip3.6 install kafka-python pandas numpy scikit-learn joblib

# Create Kafka topics
bash /home/cloudera/spark_jobs/create_kafka_topics.sh

# Fix Kafka advertised host (for Windows connectivity)
bash /home/cloudera/spark_jobs/fix_kafka_advertised_host.sh
```

### Step 4 — Download dataset (Windows host)
```powershell
pip install -r requirements.txt
python simulator/download_dataset.py
```

### Step 5 — Start Spark streaming job (INSIDE VM)
```bash
bash /home/cloudera/spark_jobs/submit_job.sh
```

### Step 6 — Start consumers (Windows host, separate terminals)
```powershell
# Terminal 1: metrics → InfluxDB
python alerting/metrics_consumer.py

# Terminal 2: alerts → console
python alerting/alert_consumer.py
```

### Step 7 — Start data simulation (Windows host)
```powershell
python simulator/kafka_producer.py --speed 20
```

### Step 8 — Train ML models (JupyterLab)
Open http://localhost:8889 (token: predmaint)
Upload and run: notebooks/01_eda_and_training.py

### Step 9 — Build Grafana dashboard
Open http://localhost:3000
Add InfluxDB datasource, then create sensor metric panels.

---

## Dataset: NASA CMAPSS

- **FD001**: Single fault (HPC degradation), single operating condition (easiest)
- **FD002**: Single fault, 6 operating conditions
- **FD003**: Two fault modes, single condition
- **FD004**: Two fault modes, 6 conditions (hardest)

Start with FD001 for initial development.

---

## VM Memory Allocation (8 GB VM)

| Service         | Approx RAM |
|-----------------|-----------|
| HDFS NameNode   | 500 MB    |
| YARN RM         | 500 MB    |
| Spark Driver    | 512 MB    |
| Spark Executor  | 1500 MB   |
| Kafka           | 512 MB    |
| Zookeeper       | 256 MB    |
| Other CDH svcs  | 1000 MB   |
| OS overhead     | 1000 MB   |
| **Buffer**      | **2220 MB**|

Tip: Stop unused CDH services (Solr, Sqoop, Oozie) in Cloudera Manager
to free ~500MB more RAM for Spark.
