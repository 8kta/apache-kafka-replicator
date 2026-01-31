#!/bin/bash

# Clean up the Docker environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

cd "$DOCKER_DIR"

echo "=== Cleaning Up Docker Environment ==="

echo "Stopping containers..."
docker-compose down

echo "Removing volumes..."
docker-compose down -v

echo "Removing plugins directory..."
rm -rf plugins/kafka-connector

echo "Removing logs directory..."
rm -rf connect-logs

echo ""
echo "✓ Cleanup complete"
echo ""
echo "To start fresh:"
echo "  1. Build the connector: cd .. && mvn clean package && cd docker"
echo "  2. Deploy JAR: ./scripts/deploy-connector.sh"
echo "  3. Start environment: docker-compose up -d"
echo "  4. Create topics: ./scripts/create-topics.sh"
echo "  5. Deploy connector: ./scripts/deploy-replicator.sh"
