# Apache Kafka Custom Source Connector

A custom Apache Kafka Connect source connector implementation. This project provides a template for building custom Kafka source connectors to integrate external data sources with Kafka.

## Project Structure

```
apache-kafka-replicator/
├── src/
│   ├── main/
│   │   ├── java/com/example/kafka/connector/
│   │   │   ├── CustomSourceConnector.java
│   │   │   ├── CustomSourceTask.java
│   │   │   └── CustomSourceConnectorConfig.java
│   │   └── resources/
│   │       └── custom-source-connector.properties
│   └── test/
└── pom.xml
```

## Components

### Source Connector
- **CustomSourceConnector**: Main connector class that manages source tasks
- **CustomSourceTask**: Task that polls data from a source and produces to Kafka topics
- **CustomSourceConnectorConfig**: Configuration management for source connector

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
target/kafka-custom-connector-1.0.0-jar-with-dependencies.jar
```

## Configuration

### Source Connector Configuration

Edit `src/main/resources/custom-source-connector.properties`:

```properties
name=custom-source-connector
connector.class=com.example.kafka.connector.CustomSourceConnector
tasks.max=1

# Custom configurations
topic=custom-source-topic
poll.interval.ms=1000
data.source=example-data-source
batch.size=100
```

**Configuration Parameters:**
- `topic`: Target Kafka topic to write data to
- `poll.interval.ms`: Interval in milliseconds between polling for new data
- `data.source`: Identifier or path to the data source
- `batch.size`: Maximum number of records to return in a single poll

## Deployment

### Standalone Mode

1. Copy the JAR to Kafka Connect's plugin directory:
```bash
mkdir -p /path/to/kafka/plugins/custom-connector
cp target/kafka-custom-connector-1.0.0-jar-with-dependencies.jar /path/to/kafka/plugins/custom-connector/
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

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "custom-source-connector",
    "config": {
      "connector.class": "com.example.kafka.connector.CustomSourceConnector",
      "tasks.max": "1",
      "topic": "custom-source-topic",
      "poll.interval.ms": "1000",
      "data.source": "example-data-source",
      "batch.size": "100"
    }
  }'
```

## Managing Connectors

### Check connector status:
```bash
curl http://localhost:8083/connectors/custom-source-connector/status
```

### List all connectors:
```bash
curl http://localhost:8083/connectors
```

### Delete a connector:
```bash
curl -X DELETE http://localhost:8083/connectors/custom-source-connector
```

### Pause a connector:
```bash
curl -X PUT http://localhost:8083/connectors/custom-source-connector/pause
```

### Resume a connector:
```bash
curl -X PUT http://localhost:8083/connectors/custom-source-connector/resume
```

## Customization

To adapt this connector for your specific use case:

1. **Modify the Source Task** (`CustomSourceTask.java`):
   - Implement your data source polling logic in the `poll()` method
   - Update offset management for your data source
   - Customize record creation based on your data format

2. **Update Configuration Class**:
   - Add new configuration parameters in `CustomSourceConnectorConfig`
   - Update validation logic as needed

3. **Add Dependencies**:
   - Update `pom.xml` with any additional libraries needed for your integration

## Testing

Run unit tests:
```bash
mvn test
```

## Logging

The connectors use SLF4J for logging. Configure logging levels in your Kafka Connect worker configuration:

```properties
log4j.logger.com.example.kafka.connector=DEBUG
```

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
