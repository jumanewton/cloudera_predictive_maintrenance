#!/usr/bin/env bash
# =================================================================
#  setup/fix_kafka_advertised_host.sh
#
#  CDH 5.13 Kafka advertises its internal hostname by default.
#  This means connections from outside the VM (your Windows host)
#  will fail even with port forwarding, because Kafka sends back
#  its own hostname in the bootstrap response.
#
#  Fix: set advertised.host.name = localhost in Cloudera Manager
#  OR add the VM hostname to Windows hosts file.
#
#  Run from INSIDE the VM to print its hostname:
#    bash /home/cloudera/setup/fix_kafka_advertised_host.sh
# =================================================================

echo "=== VM Hostname ==="
hostname
echo ""
echo "=== VM IP addresses ==="
ip addr show | grep "inet " | awk '{print $2}'
echo ""
echo "=== Kafka broker config ==="
# Show current advertised.host.name if set
grep -i "advertised" /etc/kafka/conf/server.properties 2>/dev/null || \
  echo "  Not set in server.properties (check Cloudera Manager)"

echo ""
echo "==================================================================="
echo "ACTION REQUIRED — choose ONE of these options:"
echo ""
echo "OPTION A (Easiest): Add VM hostname to Windows hosts file"
echo "  1. Run this script to get the VM hostname (shown above)"
echo "  2. On Windows, edit C:\Windows\System32\drivers\etc\hosts as Admin"
echo "  3. Add this line:  127.0.0.1   <VM-HOSTNAME>"
echo ""
echo "OPTION B: Set advertised.host.name via Cloudera Manager"
echo "  1. Open http://localhost:7180 → Kafka → Configuration"
echo "  2. Search: 'advertised.host.name'"
echo "  3. Set value to: localhost"
echo "  4. Restart Kafka"
echo "==================================================================="
