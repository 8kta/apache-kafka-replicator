# Quick Start Guide

Get the Kafka Replicator up and running in 5 minutes!

## Prerequisites

- Docker and Docker Compose installed
- Maven installed
- 8GB RAM available for Docker

## Steps

### 1. Build the Connector (from project root)

```bash
cd /Users/octavalo/Documents/projects/apache-kafka-replicator
mvn clean package
cd docker
```

### 2. Deploy the Connector JAR

```bash
./scripts/deploy-connector.sh
```

### 3. Start the Environment

```bash
docker-compose up -d
```

Wait for services to start (~30 seconds):
```bash
docker-compose ps
```

All services should show "Up" status.

### 4. Create Topics

```bash
./scripts/create-topics.sh
```

### 5. Deploy the Replicator

```bash
./scripts/deploy-replicator.sh
```

### 6. Test Replication

**Terminal 1 - Start Consumer (Target Cluster):**
```bash
./scripts/consume-messages.sh
```

**Terminal 2 - Produce Messages (Source Cluster):**
```bash
./scripts/produce-messages.sh 20
```

You should see messages appearing in Terminal 1 as they're replicated from source to target!

## Verify Replication

Check connector status:
```bash
./scripts/check-connector-status.sh
```

View connector logs:
```bash
docker logs -f kafka-connect
```

## Cleanup

Stop everything:
```bash
docker-compose down
```

Complete cleanup (removes all data):
```bash
./scripts/cleanup.sh
```

## Troubleshooting

**Services won't start:**
```bash
docker-compose logs
```

**Connector fails to deploy:**
```bash
# Check if JAR is in plugins directory
ls -la plugins/kafka-connector/

# Restart Kafka Connect
docker-compose restart kafka-connect
```

**No messages replicating:**
```bash
# Check connector status
curl http://localhost:8083/connectors/kafka-replicator/status | jq

# Check connector logs
docker logs kafka-connect | grep -i error
```

## Next Steps

- Modify `connector-config.json` to test different configurations
- Test with authentication (see main README.md)
- Try multi-partition topics
- Test high-volume replication

For detailed documentation, see [README.md](README.md)
