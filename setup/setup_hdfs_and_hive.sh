#!/usr/bin/env bash
# =================================================================
#  setup/setup_hdfs_and_hive.sh
#  Populates HDFS with sensor telemetry and creates Hive analytical
#  tables for querying in Hue (http://localhost:8888).
# =================================================================

set -e

echo "======================================================"
echo "  Step 1: Creating HDFS Directories"
echo "======================================================"
hdfs dfs -mkdir -p /user/cloudera/predmaint/raw_data
hdfs dfs -mkdir -p /user/cloudera/predmaint/parquet_data

echo "======================================================"
echo "  Step 2: Transferring Sensor Dataset into HDFS"
echo "======================================================"
echo "Downloading train_FD001.txt from host..."
curl -s -o /tmp/train_FD001.txt http://10.0.2.2:8000/data/cmapss/train_FD001.txt
hdfs dfs -put -f /tmp/train_FD001.txt /user/cloudera/predmaint/raw_data/
echo "Dataset verified in HDFS:"
hdfs dfs -ls /user/cloudera/predmaint/raw_data/

echo "======================================================"
echo "  Step 3: Creating Hive Database & External Tables"
echo "======================================================"
hive -e "
CREATE DATABASE IF NOT EXISTS predmaint_db;
USE predmaint_db;

-- 1. External table over the raw sensor data in HDFS
DROP TABLE IF EXISTS turbofan_telemetry;
CREATE EXTERNAL TABLE turbofan_telemetry (
    unit_id INT,
    time_cycle INT,
    op1 DOUBLE,
    op2 DOUBLE,
    op3 DOUBLE,
    s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, s4 DOUBLE, s5 DOUBLE,
    s6 DOUBLE, s7 DOUBLE, s8 DOUBLE, s9 DOUBLE, s10 DOUBLE,
    s11 DOUBLE, s12 DOUBLE, s13 DOUBLE, s14 DOUBLE, s15 DOUBLE,
    s16 DOUBLE, s17 DOUBLE, s18 DOUBLE, s19 DOUBLE, s20 DOUBLE,
    s21 DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/user/cloudera/predmaint/raw_data';

-- 2. Aggregated analytical summary table with Risk Classification
DROP TABLE IF EXISTS engine_risk_summary;
CREATE TABLE engine_risk_summary AS
SELECT 
    unit_id,
    MAX(time_cycle) as total_cycles_run,
    ROUND(AVG(s2), 2) as avg_fan_temp,
    ROUND(MAX(s2), 2) as peak_fan_temp,
    ROUND(AVG(s9), 2) as avg_fan_speed,
    ROUND(AVG(s12), 2) as avg_fuel_flow,
    CASE 
        WHEN MAX(s2) > 644.0 THEN 'CRITICAL_RISK'
        WHEN MAX(s2) > 642.5 THEN 'ELEVATED_WEAR'
        ELSE 'NORMAL'
    END as machine_health_status
FROM turbofan_telemetry
GROUP BY unit_id;
"

echo "======================================================"
echo "  Step 4: Querying Risk Summary from Hive"
echo "======================================================"
hive -e "USE predmaint_db; SELECT * FROM engine_risk_summary LIMIT 10;"

echo ""
echo "======================================================"
echo "  SUCCESS! HDFS & Hive Pipeline Ready."
echo "  Open Hue in your Windows browser: http://localhost:8888"
echo "  Login: cloudera / cloudera"
echo "  Database: predmaint_db"
echo "  Table: turbofan_telemetry & engine_risk_summary"
echo "======================================================"
