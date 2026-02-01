# Apache Kafka Replicator Connector

A Kafka Connect source connector for replicating data from one Kafka cluster to another. This connector consumes messages from a source Kafka topic and produces them to a target Kafka cluster, preserving message keys, values, headers, partitions, and timestamps.

## Project Structure

```
apache-kafka-replicator/
├── src/
│   ├── main/
│   │   ├── java/com/octavalo/kafka/connector/
│   │   │   ├── CustomSourceConnector.java       # Main connector class
│   │   │   ├── CustomSourceTask.java            # Task implementation with consumer logic
│   │   │   └── CustomSourceConnectorConfig.java # Configuration with JAAS/SSL support
│   │   └── resources/
│   │       └── custom-source-connector.properties
│   └── test/
│       └── java/com/octavalo/kafka/connector/
│           ├── CustomSourceConnectorTest.java        # Connector unit tests
│           ├── CustomSourceTaskTest.java             # Task unit tests
│           └── CustomSourceConnectorConfigTest.java  # Configuration unit tests
├── pom.xml                                      # Maven build configuration
├── README.md                                    # This file
└── .gitignore                                   # Git ignore rules
```

## Components

### Replication Connector
- **CustomSourceConnector**: Main connector class that manages replication tasks
- **CustomSourceTask**: Task that consumes from source Kafka cluster and produces to target cluster
- **CustomSourceConnectorConfig**: Configuration management with source cluster connection settings

## Features

- **Cross-cluster replication**: Replicate data from one Kafka cluster to another
- **Preserve message integrity**: Maintains original keys, values, and headers
- **Partition preservation**: Option to maintain source partition assignment
- **Timestamp preservation**: Option to keep original message timestamps
- **Offset management**: Automatic offset tracking and resumption
- **Security support**: SASL/SSL authentication for source cluster
- **Configurable performance**: Tunable batch sizes and poll intervals
- **Consumer Group Offset Sync**: Synchronize consumer group offsets from source to target cluster

## Prerequisites

- Java 11 or higher
- Apache Maven 3.6+
- Apache Kafka 3.6.0 or compatible version
- Kafka Connect runtime environment

## Building the Project

1. Clone the repository:
```bash
cd /Users/octavalo/Documents/projects/apache-kafka-replicator
```

2. Build the project with Maven:
```bash
mvn clean package
```

3. The compiled JAR will be available at:
```
target/kafka-connector-1.0.0-jar-with-dependencies.jar
```

## Configuration

### Connector Configuration

Edit `src/main/resources/custom-source-connector.properties`:

```properties
name=kafka-replicator-connector
connector.class=com.octavalo.kafka.connector.CustomSourceConnector
tasks.max=1

# Source Kafka cluster configuration
source.bootstrap.servers=source-kafka-broker:9092
source.topic=source-topic-name
source.group.id=kafka-replicator-connector
source.security.protocol=PLAINTEXT

# Target topic configuration
target.topic=target-topic-name

# Performance tuning
poll.timeout.ms=1000
max.poll.records=500

# Data preservation options
preserve.partitions=true
preserve.timestamps=true
```

**Configuration Parameters:**

**Required:**
- `source.bootstrap.servers`: Bootstrap servers for the source Kafka cluster
- `source.topic`: Source topic to replicate from

**Optional:**
- `target.topic`: Target topic name (defaults to source topic name if not specified)
- `source.group.id`: Consumer group ID for source cluster (default: `kafka-replicator-connector`)
- `poll.timeout.ms`: Timeout for polling source cluster (default: `1000`)
- `max.poll.records`: Maximum records per poll (default: `500`)
- `preserve.partitions`: Preserve source partition assignment (default: `true`)
- `preserve.timestamps`: Preserve original message timestamps (default: `true`)

**Security Configuration:**
- `source.security.protocol`: Security protocol - PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL (default: `PLAINTEXT`)
- `source.sasl.mechanism`: SASL mechanism - PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
- `source.sasl.jaas.config`: Direct JAAS configuration string (most flexible)
- `source.sasl.username`: Username for SASL authentication (auto-builds JAAS config)
- `source.sasl.password`: Password for SASL authentication (auto-builds JAAS config)
- `source.ssl.truststore.location`: Path to SSL truststore file
- `source.ssl.truststore.password`: Password for SSL truststore
- `source.ssl.keystore.location`: Path to SSL keystore file
- `source.ssl.keystore.password`: Password for SSL keystore
- `source.ssl.key.password`: Password for the key in the keystore

**Consumer Group Offset Sync:**
- `sync.consumer.offsets`: Enable offset synchronization (default: `false`)
- `sync.consumer.groups`: Comma-separated list of groups to sync (empty = all groups)
- `offset.sync.interval.ms`: Sync interval in milliseconds (default: `60000`)
- `offset.sync.topic`: Internal topic for sync state (default: `__consumer_offsets_sync`)

## Deployment

### Standalone Mode

1. Copy the JAR to Kafka Connect's plugin directory:
```bash
mkdir -p /path/to/kafka/plugins/kafka-connector
cp target/kafka-connector-1.0.0-jar-with-dependencies.jar /path/to/kafka/plugins/kafka-connector/
```

2. Update Kafka Connect worker configuration (`connect-standalone.properties`):
```properties
plugin.path=/path/to/kafka/plugins
```

3. Start the connector in standalone mode:
```bash
connect-standalone.sh config/connect-standalone.properties \
    src/main/resources/custom-source-connector.properties
```

### Distributed Mode

1. Copy the JAR to Kafka Connect's plugin directory on all worker nodes.

2. Start Kafka Connect in distributed mode:
```bash
connect-distributed.sh config/connect-distributed.properties
```

3. Deploy the connector via REST API:

**Basic Configuration:**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-replicator-connector",
    "config": {
      "connector.class": "com._8kta.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "source.bootstrap.servers": "source-kafka:9092",
      "source.topic": "my-source-topic",
      "target.topic": "my-target-topic",
      "source.group.id": "kafka-replicator",
      "poll.timeout.ms": "1000",
      "max.poll.records": "500",
      "preserve.partitions": "true",
      "preserve.timestamps": "true"
    }
  }'
```

**With SASL PLAIN Authentication (Direct JAAS):**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-replicator-connector",
    "config": {
      "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "source.bootstrap.servers": "source-kafka:9092",
      "source.topic": "my-source-topic",
      "target.topic": "my-target-topic",
      "source.security.protocol": "SASL_SSL",
      "source.sasl.mechanism": "PLAIN",
      "source.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"password\";"
    }
  }'
```

**With SASL Authentication (Simplified Username/Password):**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-replicator-connector",
    "config": {
      "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "source.bootstrap.servers": "source-kafka:9092",
      "source.topic": "my-source-topic",
      "target.topic": "my-target-topic",
      "source.security.protocol": "SASL_SSL",
      "source.sasl.mechanism": "PLAIN",
      "source.sasl.username": "myuser",
      "source.sasl.password": "mypassword"
    }
  }'
```

**With SCRAM-SHA-256 Authentication:**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-replicator-connector",
    "config": {
      "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "source.bootstrap.servers": "source-kafka:9092",
      "source.topic": "my-source-topic",
      "target.topic": "my-target-topic",
      "source.security.protocol": "SASL_SSL",
      "source.sasl.mechanism": "SCRAM-SHA-256",
      "source.sasl.username": "myuser",
      "source.sasl.password": "mypassword",
      "source.ssl.truststore.location": "/path/to/truststore.jks",
      "source.ssl.truststore.password": "truststore-pass"
    }
  }'
```

## Managing Connectors

### Check connector status:
```bash
curl http://localhost:8083/connectors/kafka-replicator-connector/status
```

### List all connectors:
```bash
curl http://localhost:8083/connectors
```

### Delete a connector:
```bash
curl -X DELETE http://localhost:8083/connectors/kafka-replicator-connector
```

### Pause a connector:
```bash
curl -X PUT http://localhost:8083/connectors/kafka-replicator-connector/pause
```

### Resume a connector:
```bash
curl -X PUT http://localhost:8083/connectors/kafka-replicator-connector/resume
```

## How It Works

1. **Consumer Setup**: The connector creates a Kafka consumer that connects to the source cluster
2. **Offset Management**: Uses Kafka Connect's offset storage to track progress and enable resumption
3. **Message Polling**: Continuously polls messages from the source topic
4. **Data Preservation**: Maintains message keys, values, headers, partitions (optional), and timestamps (optional)
5. **Production**: Produces messages to the target cluster via Kafka Connect framework

## Authentication Methods

### SASL/PLAIN
Simplest authentication method, suitable for development and testing.

**Option 1: Direct JAAS Configuration**
```properties
source.security.protocol=SASL_PLAINTEXT
source.sasl.mechanism=PLAIN
source.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";
```

**Option 2: Simplified (Auto-builds JAAS)**
```properties
source.security.protocol=SASL_PLAINTEXT
source.sasl.mechanism=PLAIN
source.sasl.username=user
source.sasl.password=password
```

### SASL/SCRAM
More secure than PLAIN, recommended for production.

```properties
source.security.protocol=SASL_SSL
source.sasl.mechanism=SCRAM-SHA-256
source.sasl.username=user
source.sasl.password=password
source.ssl.truststore.location=/path/to/truststore.jks
source.ssl.truststore.password=truststore-password
```

### SSL/TLS (Mutual Authentication)
Certificate-based authentication.

```properties
source.security.protocol=SSL
source.ssl.truststore.location=/path/to/truststore.jks
source.ssl.truststore.password=truststore-password
source.ssl.keystore.location=/path/to/keystore.jks
source.ssl.keystore.password=keystore-password
source.ssl.key.password=key-password
```

### SASL/GSSAPI (Kerberos)
For enterprise environments with Kerberos.

```properties
source.security.protocol=SASL_PLAINTEXT
source.sasl.mechanism=GSSAPI
source.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="/path/to/kafka.keytab" principal="kafka/hostname@REALM";
```

## Security Best Practices

1. **Use SSL/TLS in Production**: Always use `SASL_SSL` or `SSL` protocols for production environments
2. **Secure Credentials**: Store passwords in external secret management systems (e.g., HashiCorp Vault, AWS Secrets Manager)
3. **Use SCRAM over PLAIN**: SCRAM-SHA-256 or SCRAM-SHA-512 are more secure than PLAIN
4. **Rotate Credentials**: Regularly rotate passwords and certificates
5. **Limit Permissions**: Use Kafka ACLs to restrict connector permissions to only necessary topics
6. **Monitor Authentication**: Enable audit logging for authentication attempts

## Consumer Group Offset Synchronization

The replicator can synchronize consumer group offsets from the source cluster to the target cluster, enabling seamless failover and migration scenarios.

**Enable offset sync:**
```json
{
  "sync.consumer.offsets": "true",
  "sync.consumer.groups": "group1,group2",
  "offset.sync.interval.ms": "60000"
}
```

**Benefits:**
- Consumer groups maintain their position during failover
- No message reprocessing or data loss
- Smooth cluster migration for active consumers

For detailed documentation, see [OFFSET_SYNC.md](OFFSET_SYNC.md)

## Use Cases

- **Disaster Recovery**: Replicate critical topics to a backup cluster with consumer position sync
- **Data Migration**: Move data from one Kafka cluster to another while maintaining consumer state
- **Multi-Region Replication**: Sync data across geographically distributed clusters
- **Development/Testing**: Copy production data to non-production environments
- **Cloud Migration**: Migrate from on-premise to cloud Kafka clusters with zero downtime

## Customization

To adapt this connector for specific requirements:

1. **Add Message Filtering**:
   - Modify `CustomSourceTask.poll()` to filter messages based on criteria
   - Add configuration for filter rules

2. **Add Message Transformation**:
   - Implement transformation logic in the task
   - Consider using Kafka Connect SMTs (Single Message Transforms) instead

3. **Multi-Topic Replication**:
   - Extend configuration to support multiple source topics
   - Update task to handle topic routing

4. **Add Dependencies**:
   - Update `pom.xml` with any additional libraries needed

## Testing

Run unit tests:
```bash
mvn test
```

## Logging

The connectors use SLF4J for logging. Configure logging levels in your Kafka Connect worker configuration:

```properties
log4j.logger.com.octavalo.kafka.connector=DEBUG
```

Available log levels:
- **ERROR**: Critical errors and exceptions
- **WARN**: Warning messages (invalid configurations, processing errors)
- **INFO**: General operational information (startup, shutdown, replication status)
- **DEBUG**: Detailed debugging information (configuration details, offset management)
- **TRACE**: Very detailed trace information (individual record processing)

## Troubleshooting

### Dependencies not resolved
Run `mvn clean install` to download all dependencies.

### Connector fails to start
- Check the Kafka Connect logs for detailed error messages
- Verify all required configuration parameters are provided
- Ensure the JAR is in the correct plugin directory

### No data flowing through connector
- Verify the connector status via REST API
- Check connector and task logs
- Ensure topics exist and are accessible

## License

This project is open source. See LICENSE file for details.

## Contributing

Contributions are welcome! Please submit pull requests or open issues for bugs and feature requests
