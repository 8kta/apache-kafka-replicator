#!/bin/bash

# Produce test messages to the source cluster

set -e

echo "=== Producing Test Messages to Source Cluster ==="

# Number of messages to produce
NUM_MESSAGES=${1:-10}

echo "Producing $NUM_MESSAGES messages to test-source-topic..."

for i in $(seq 1 $NUM_MESSAGES); do
    MESSAGE="Test message $i - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "$MESSAGE" | docker exec -i kafka-source /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic test-source-topic
    echo "Sent: $MESSAGE"
    sleep 0.5
done

echo ""
echo "✓ Produced $NUM_MESSAGES messages to source cluster"
echo ""
echo "To consume from target cluster:"
echo "  ./scripts/consume-messages.sh"
