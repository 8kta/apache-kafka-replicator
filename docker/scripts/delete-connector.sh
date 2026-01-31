#!/bin/bash

# Delete the Kafka Replicator connector

set -e

echo "=== Deleting Kafka Replicator Connector ==="

curl -X DELETE http://localhost:8083/connectors/kafka-replicator

echo ""
echo "✓ Connector deleted successfully"
echo ""
echo "To redeploy:"
echo "  ./scripts/deploy-replicator.sh"
