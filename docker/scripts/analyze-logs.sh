#!/bin/bash

# Analyze connector logs for errors, warnings, and statistics

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="$DOCKER_DIR/logs/connect"
CONNECTOR_LOG="$LOGS_DIR/kafka-replicator.log"

echo "=== Kafka Replicator Log Analysis ==="
echo ""

if [ ! -f "$CONNECTOR_LOG" ]; then
    echo "ERROR: Connector log file not found at $CONNECTOR_LOG"
    echo "Make sure the connector is running and has generated logs."
    exit 1
fi

echo "Analyzing: $CONNECTOR_LOG"
echo "Log file size: $(du -h "$CONNECTOR_LOG" | cut -f1)"
echo "Last modified: $(stat -f "%Sm" "$CONNECTOR_LOG")"
echo ""

# Error analysis
echo "=== ERRORS ==="
ERROR_COUNT=$(grep -c "ERROR" "$CONNECTOR_LOG" 2>/dev/null || echo "0")
echo "Total errors: $ERROR_COUNT"
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo ""
    echo "Recent errors:"
    grep "ERROR" "$CONNECTOR_LOG" | tail -5
fi
echo ""

# Warning analysis
echo "=== WARNINGS ==="
WARN_COUNT=$(grep -c "WARN" "$CONNECTOR_LOG" 2>/dev/null || echo "0")
echo "Total warnings: $WARN_COUNT"
if [ "$WARN_COUNT" -gt 0 ]; then
    echo ""
    echo "Recent warnings:"
    grep "WARN" "$CONNECTOR_LOG" | tail -5
fi
echo ""

# Replication statistics
echo "=== REPLICATION STATISTICS ==="
STARTED=$(grep -c "CustomSourceTask started successfully" "$CONNECTOR_LOG" 2>/dev/null || echo "0")
STOPPED=$(grep -c "CustomSourceTask stopped" "$CONNECTOR_LOG" 2>/dev/null || echo "0")
echo "Task starts: $STARTED"
echo "Task stops: $STOPPED"
echo ""

# Recent activity
echo "=== RECENT ACTIVITY (Last 10 lines) ==="
tail -10 "$CONNECTOR_LOG"
echo ""

# Log level distribution
echo "=== LOG LEVEL DISTRIBUTION ==="
echo "DEBUG: $(grep -c "DEBUG" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo "INFO:  $(grep -c "INFO" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo "WARN:  $WARN_COUNT"
echo "ERROR: $ERROR_COUNT"
echo ""

# Search for specific patterns
echo "=== KEY EVENTS ==="
echo "Configuration validations: $(grep -c "Configuration validated" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo "Consumer creations: $(grep -c "Creating Kafka consumer" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo "Offset resumptions: $(grep -c "Resuming from stored offsets" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo "Records polled: $(grep -c "Polled.*records" "$CONNECTOR_LOG" 2>/dev/null || echo "0")"
echo ""

echo "=== ANALYSIS COMPLETE ==="
echo ""
echo "To view full logs:"
echo "  ./scripts/monitor-logs.sh 1"
echo ""
echo "To search for specific patterns:"
echo "  grep 'pattern' $CONNECTOR_LOG"
