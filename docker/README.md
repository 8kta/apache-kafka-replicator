# Docker Test Environment for Kafka Replicator

This directory contains a complete Docker Compose setup to test the Kafka Replicator connector with two separate Kafka clusters.

## Architecture

```
┌─────────────────────┐         ┌─────────────────────┐
│  Source Cluster     │         │  Target Cluster     │
│                     │         │                     │
│  ┌──────────────┐   │         │  ┌──────────────┐   │
│  │ Zookeeper    │   │         │  │ Zookeeper    │   │
│  │ (KRaft)      │   │         │  │ (KRaft)      │   │
│  │ :2181        │   │         │  │ :2182        │   │
│  └──────────────┘   │         │  └──────────────┘   │
│         │           │         │         │           │
│  ┌──────────────┐   │         │  ┌──────────────┐   │
│  │ Kafka Broker │   │         │  │ Kafka Broker │   │
│  │ :9092/:19092 │   │         │  │ :9093/:19093 │   │
│  └──────────────┘   │         │  └──────────────┘   │
│         │           │         │         │           │
└─────────┼───────────┘         └─────────┼───────────┘
          │                               │
          │        ┌──────────────┐       │
          └────────│ Kafka Connect│───────┘
                   │ Replicator   │
                   │ :8083        │
                   └──────────────┘
```

## Components

### Source Cluster
- **Zookeeper**: Port 2181
- **Kafka Broker**: 
  - Internal: `kafka-source:9092`
  - External: `localhost:19092`

### Target Cluster
- **Zookeeper**: Port 2182
- **Kafka Broker**: 
  - Internal: `kafka-target:9092`
  - External: `localhost:19093`

### Kafka Connect
- **REST API**: Port 8083
- **Plugin Directory**: `./plugins`
- **Logs Directory**: `./connect-logs`

## Prerequisites

- Docker and Docker Compose installed
- Maven (to build the connector JAR)
- curl or httpie (for REST API calls)

## Quick Start

### 1. Build the Connector

From the project root directory:
```bash
cd ..
mvn clean package
cd docker
```

### 2. Deploy the Connector JAR

```bash
./scripts/deploy-connector.sh
```

Or manually:
```bash
mkdir -p plugins/kafka-connector
cp ../target/kafka-connector-1.0.0-jar-with-dependencies.jar plugins/kafka-connector/
```

### 3. Start the Environment

```bash
docker-compose up -d
```

Wait for all services to be healthy (about 30 seconds):
```bash
docker-compose ps
```

### 4. Create Topics

```bash
./scripts/create-topics.sh
```

### 5. Deploy the Replicator Connector

```bash
./scripts/deploy-replicator.sh
```

### 6. Test the Replication

Produce messages to the source cluster:
```bash
./scripts/produce-messages.sh
```

Consume messages from the target cluster:
```bash
./scripts/consume-messages.sh
```

## Log Files

Logs are stored in `./logs/connect/` directory:

- **kafka-replicator.log** - Connector-specific logs (DEBUG level)
- **connect.log** - Kafka Connect framework logs (INFO level)

Log rotation:
- Max file size: 50MB (connector), 100MB (connect)
- Max backup files: 5 (connector), 10 (connect)

## Available Scripts

### `scripts/deploy-connector.sh`
Copies the connector JAR to the plugins directory.

### `scripts/create-topics.sh`
Creates test topics in both source and target clusters.

### `scripts/deploy-replicator.sh`
Deploys the replicator connector via Kafka Connect REST API.

### `scripts/produce-messages.sh`
Produces test messages to the source cluster.

### `scripts/consume-messages.sh`
Consumes messages from the target cluster to verify replication.

### `scripts/check-connector-status.sh`
Checks the status of the replicator connector.

### `scripts/delete-connector.sh`
Deletes the replicator connector.

### `scripts/cleanup.sh`
Stops and removes all containers and volumes.

## Manual Testing

### Produce Messages to Source
```bash
docker exec -it kafka-source /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-source-topic
```

### Consume from Target
```bash
docker exec -it kafka-target /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-target-topic \
  --from-beginning
```

### Check Connector Status
```bash
curl http://localhost:8083/connectors/kafka-replicator/status | jq
```

### View Connector Logs

**Option 1: Monitor connector-specific logs (recommended)**
```bash
./scripts/monitor-logs.sh 1
```

**Option 2: Monitor all Kafka Connect logs**
```bash
./scripts/monitor-logs.sh 2
```

**Option 3: Monitor Docker container logs**
```bash
docker logs -f kafka-connect
```

**Option 4: Analyze logs for errors and statistics**
```bash
./scripts/analyze-logs.sh
```

## Kafka Connect REST API

### List Connectors
```bash
curl http://localhost:8083/connectors
```

### Get Connector Config
```bash
curl http://localhost:8083/connectors/kafka-replicator | jq
```

### Update Connector Config
```bash
curl -X PUT http://localhost:8083/connectors/kafka-replicator/config \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

### Restart Connector
```bash
curl -X POST http://localhost:8083/connectors/kafka-replicator/restart
```

### Delete Connector
```bash
curl -X DELETE http://localhost:8083/connectors/kafka-replicator
```

## Troubleshooting

### Check if services are running
```bash
docker-compose ps
```

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka-connect
docker-compose logs -f kafka-source
docker-compose logs -f kafka-target
```

### Monitor connector logs in real-time
```bash
# Connector-specific logs (filtered)
./scripts/monitor-logs.sh 1

# All Kafka Connect logs
./scripts/monitor-logs.sh 2

# Combined view
./scripts/monitor-logs.sh 4
```

### Analyze logs for issues
```bash
./scripts/analyze-logs.sh
```

This will show:
- Error count and recent errors
- Warning count and recent warnings
- Replication statistics
- Log level distribution
- Key events (consumer creation, offset management, etc.)

### Check Kafka Connect plugins
```bash
curl http://localhost:8083/connector-plugins | jq
```

### Verify topics exist
```bash
# Source cluster
docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Target cluster
docker exec kafka-target /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Check consumer groups
```bash
docker exec kafka-source /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
```

### Reset the environment
```bash
./scripts/cleanup.sh
docker-compose up -d
```

## Configuration Files

### connector-config.json
Example connector configuration for deployment:
```json
{
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
}
```

## Performance Testing

### High Volume Test
```bash
# Produce 10000 messages
docker exec kafka-source /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic test-source-topic \
  --num-records 10000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092
```

### Monitor Lag
```bash
docker exec kafka-source /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kafka-replicator-connector \
  --describe
```

## Cleanup

Stop and remove all containers:
```bash
docker-compose down
```

Remove volumes (deletes all data):
```bash
docker-compose down -v
```

Clean up everything:
```bash
./scripts/cleanup.sh
```

## Network Ports

| Service | Internal Port | External Port | Purpose |
|---------|--------------|---------------|---------|
| zookeeper-source | 9093 | 2181 | Source ZooKeeper |
| kafka-source | 9092 | 19092 | Source Kafka Broker |
| zookeeper-target | 9093 | 2182 | Target ZooKeeper |
| kafka-target | 9092 | 19093 | Target Kafka Broker |
| kafka-connect | 8083 | 8083 | Kafka Connect REST API |

## Notes

- The setup uses Apache Kafka 3.6.0 with KRaft mode (no ZooKeeper dependency)
- Both clusters are configured with replication factor 1 for simplicity
- Kafka Connect is configured to connect to the target cluster
- The connector consumes from the source cluster and produces to the target cluster
- All data is persisted in Docker volumes
