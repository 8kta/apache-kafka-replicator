#!/bin/bash

# Deploy the Kafka Replicator connector via REST API

set -e

echo "=== Deploying Kafka Replicator Connector ==="

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -s http://localhost:8083/ > /dev/null; do
    echo "Waiting for Kafka Connect REST API..."
    sleep 5
done

echo "Kafka Connect is ready!"
echo ""

# Check if connector already exists
if curl -s http://localhost:8083/connectors | grep -q "kafka-replicator"; then
    echo "Connector already exists. Deleting..."
    curl -X DELETE http://localhost:8083/connectors/kafka-replicator
    sleep 2
fi

# Deploy connector
echo "Deploying connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-replicator",
    "config": {
      "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "source.bootstrap.servers": "kafka-source:9092",
      "source.topic": "test-source-topic",
      "target.topic": "test-target-topic",
      "source.group.id": "kafka-replicator-connector",
      "poll.timeout.ms": "1000",
      "max.poll.records": "500",
      "preserve.partitions": "true",
      "preserve.timestamps": "true"
    }
  }'

echo ""
echo ""
echo "✓ Connector deployed successfully"
echo ""
echo "Check connector status:"
echo "  curl http://localhost:8083/connectors/kafka-replicator/status | jq"
echo ""
echo "View connector logs:"
echo "  docker logs -f kafka-connect"
