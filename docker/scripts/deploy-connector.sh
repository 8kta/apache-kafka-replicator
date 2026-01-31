#!/bin/bash

# Deploy Connector JAR to Kafka Connect plugins directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DOCKER_DIR")"

echo "=== Deploying Kafka Replicator Connector ==="

# Check if JAR exists
JAR_FILE="$PROJECT_ROOT/target/kafka-connector-1.0.0-jar-with-dependencies.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: Connector JAR not found at $JAR_FILE"
    echo "Please build the project first: mvn clean package"
    exit 1
fi

# Create plugins directory
PLUGINS_DIR="$DOCKER_DIR/plugins/kafka-connector"
mkdir -p "$PLUGINS_DIR"

# Copy JAR
echo "Copying JAR to $PLUGINS_DIR"
cp "$JAR_FILE" "$PLUGINS_DIR/"

echo "✓ Connector JAR deployed successfully"
echo ""
echo "Next steps:"
echo "  1. Start the environment: docker-compose up -d"
echo "  2. Wait for services to be ready (~30 seconds)"
echo "  3. Deploy the connector: ./scripts/deploy-replicator.sh"
