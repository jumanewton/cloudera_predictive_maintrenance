#!/usr/bin/env bash
# =================================================================
#  setup/create_kafka_topics.sh
#  Run this INSIDE the CDH 5.13 VM to create Kafka topics.
#  Usage: bash /home/cloudera/setup/create_kafka_topics.sh
# =================================================================

ZOOKEEPER="localhost:2181"
REPLICATION=1
PARTITIONS=3

echo "=== Creating Kafka topics ==="

kafka-topics --create \
  --zookeeper $ZOOKEEPER \
  --replication-factor $REPLICATION \
  --partitions $PARTITIONS \
  --topic sensor-readings \
  2>/dev/null || echo "  [INFO] sensor-readings already exists"

kafka-topics --create \
  --zookeeper $ZOOKEEPER \
  --replication-factor $REPLICATION \
  --partitions 1 \
  --topic machine-metrics \
  2>/dev/null || echo "  [INFO] machine-metrics already exists"

kafka-topics --create \
  --zookeeper $ZOOKEEPER \
  --replication-factor $REPLICATION \
  --partitions 1 \
  --topic machine-alerts \
  2>/dev/null || echo "  [INFO] machine-alerts already exists"

echo ""
echo "=== Existing topics ==="
kafka-topics --list --zookeeper $ZOOKEEPER

echo ""
echo "Done."
