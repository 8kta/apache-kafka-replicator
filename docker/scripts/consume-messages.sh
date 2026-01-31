#!/bin/bash

# Consume messages from the target cluster

set -e

echo "=== Consuming Messages from Target Cluster ==="
echo "Reading from test-target-topic..."
echo "Press Ctrl+C to stop"
echo ""

docker exec -it kafka-target /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-target-topic \
  --from-beginning \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.partition=true
