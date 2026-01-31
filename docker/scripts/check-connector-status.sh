#!/bin/bash

# Check the status of the Kafka Replicator connector

set -e

echo "=== Kafka Replicator Connector Status ==="
echo ""

# Check if jq is available
if command -v jq &> /dev/null; then
    curl -s http://localhost:8083/connectors/kafka-replicator/status | jq
else
    curl -s http://localhost:8083/connectors/kafka-replicator/status
fi

echo ""
echo "=== Connector Tasks ==="
curl -s http://localhost:8083/connectors/kafka-replicator/tasks | jq 2>/dev/null || curl -s http://localhost:8083/connectors/kafka-replicator/tasks

echo ""
echo "=== Consumer Group Lag ==="
docker exec kafka-source /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kafka-replicator-connector \
  --describe 2>/dev/null || echo "No consumer group found or no lag"
