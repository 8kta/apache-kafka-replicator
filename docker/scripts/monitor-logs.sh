#!/bin/bash

# Monitor Kafka Replicator connector logs in real-time

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="$DOCKER_DIR/logs/connect"

echo "=== Kafka Replicator Log Monitor ==="
echo ""
echo "Available log files:"
echo "  1. Connector logs (kafka-replicator.log)"
echo "  2. Kafka Connect logs (connect.log)"
echo "  3. Docker container logs"
echo "  4. All logs (combined)"
echo ""

# Default to connector logs
LOG_TYPE=${1:-1}

case $LOG_TYPE in
    1)
        echo "Monitoring connector logs: $LOGS_DIR/kafka-replicator.log"
        echo "Press Ctrl+C to stop"
        echo ""
        if [ -f "$LOGS_DIR/kafka-replicator.log" ]; then
            tail -f "$LOGS_DIR/kafka-replicator.log"
        else
            echo "Log file not found. Waiting for logs to be created..."
            while [ ! -f "$LOGS_DIR/kafka-replicator.log" ]; do
                sleep 1
            done
            tail -f "$LOGS_DIR/kafka-replicator.log"
        fi
        ;;
    2)
        echo "Monitoring Kafka Connect logs: $LOGS_DIR/connect.log"
        echo "Press Ctrl+C to stop"
        echo ""
        if [ -f "$LOGS_DIR/connect.log" ]; then
            tail -f "$LOGS_DIR/connect.log"
        else
            echo "Log file not found. Waiting for logs to be created..."
            while [ ! -f "$LOGS_DIR/connect.log" ]; do
                sleep 1
            done
            tail -f "$LOGS_DIR/connect.log"
        fi
        ;;
    3)
        echo "Monitoring Docker container logs"
        echo "Press Ctrl+C to stop"
        echo ""
        docker logs -f kafka-connect
        ;;
    4)
        echo "Monitoring all logs"
        echo "Press Ctrl+C to stop"
        echo ""
        if [ -f "$LOGS_DIR/kafka-replicator.log" ] && [ -f "$LOGS_DIR/connect.log" ]; then
            tail -f "$LOGS_DIR/kafka-replicator.log" "$LOGS_DIR/connect.log"
        else
            echo "Log files not found yet. Using Docker logs..."
            docker logs -f kafka-connect
        fi
        ;;
    *)
        echo "Invalid option. Usage:"
        echo "  ./monitor-logs.sh [1|2|3|4]"
        echo "    1 - Connector logs (default)"
        echo "    2 - Kafka Connect logs"
        echo "    3 - Docker container logs"
        echo "    4 - All logs"
        exit 1
        ;;
esac
