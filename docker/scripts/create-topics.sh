#!/bin/bash

# Create test topics in source and target clusters

set -e

echo "=== Creating Kafka Topics ==="

# Wait for Kafka to be ready
echo "Waiting for Kafka clusters to be ready..."
sleep 10

# Create topic in source cluster
echo "Creating topic 'test-source-topic' in source cluster..."
docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic test-source-topic \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Create topic in target cluster
echo "Creating topic 'test-target-topic' in target cluster..."
docker exec kafka-target /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic test-target-topic \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "✓ Topics created successfully"
echo ""
echo "Source cluster topics:"
docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

echo ""
echo "Target cluster topics:"
docker exec kafka-target /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
