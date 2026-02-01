# Consumer Group Offset Synchronization

The Kafka Replicator now supports synchronizing consumer group offsets from the source cluster to the target cluster. This feature ensures that when you replicate data between clusters, consumer applications can maintain their position and resume from where they left off.

## Overview

When enabled, the replicator will:
1. Monitor consumer groups on the source cluster
2. Track their committed offsets for the replicated topic
3. Periodically sync those offsets to the target cluster
4. Translate partition offsets from source topic to target topic

This is particularly useful for:
- **Disaster Recovery**: Consumer groups can failover to the target cluster without losing position
- **Cluster Migration**: Move consumers gradually while maintaining offset consistency
- **Testing**: Replicate production data and consumer positions to test environments

## Configuration

### Enable Offset Synchronization

Add these parameters to your connector configuration:

```json
{
  "sync.consumer.offsets": "true",
  "sync.consumer.groups": "group1,group2,group3",
  "offset.sync.interval.ms": "60000"
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sync.consumer.offsets` | boolean | `false` | Enable consumer group offset synchronization |
| `sync.consumer.groups` | string | `null` | Comma-separated list of consumer groups to sync (empty = all groups) |
| `offset.sync.interval.ms` | int | `60000` | Interval in milliseconds between offset sync operations |
| `offset.sync.topic` | string | `__consumer_offsets_sync` | Internal topic for tracking sync state |

## Usage Examples

### Sync All Consumer Groups

Synchronize offsets for all consumer groups that consume from the source topic:

```json
{
  "name": "kafka-replicator",
  "config": {
    "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
    "tasks.max": "1",
    "source.bootstrap.servers": "source-kafka:9092",
    "source.topic": "orders",
    "target.topic": "orders",
    "sync.consumer.offsets": "true",
    "offset.sync.interval.ms": "30000"
  }
}
```

### Sync Specific Consumer Groups

Only synchronize offsets for specific consumer groups:

```json
{
  "name": "kafka-replicator",
  "config": {
    "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
    "tasks.max": "1",
    "source.bootstrap.servers": "source-kafka:9092",
    "source.topic": "orders",
    "target.topic": "orders",
    "sync.consumer.offsets": "true",
    "sync.consumer.groups": "order-processor,analytics-consumer,audit-service",
    "offset.sync.interval.ms": "60000"
  }
}
```

### High-Frequency Sync

For critical applications requiring near real-time offset synchronization:

```json
{
  "sync.consumer.offsets": "true",
  "offset.sync.interval.ms": "10000"
}
```

## How It Works

### 1. Discovery Phase
- The replicator queries the source cluster for all consumer groups
- Filters groups based on `sync.consumer.groups` configuration
- Identifies groups that consume from the replicated topic

### 2. Offset Collection
- For each consumer group, retrieves committed offsets
- Extracts offsets for partitions of the source topic
- Captures offset metadata (if available)

### 3. Offset Translation
- Maps source topic partitions to target topic partitions
- Preserves partition numbers when `preserve.partitions=true`
- Maintains offset values (no translation needed for identical replication)

### 4. Offset Commit
- Commits translated offsets to the target cluster
- Uses Kafka Admin API for atomic offset updates
- Handles errors gracefully without disrupting replication

### 5. Periodic Sync
- Repeats the process based on `offset.sync.interval.ms`
- Logs sync statistics (groups synced, errors, duration)
- Continues replication even if offset sync fails

## Monitoring

### Check Offset Sync Status

View connector logs to monitor offset synchronization:

```bash
# Docker environment
./docker/scripts/monitor-logs.sh 1

# Look for these log messages:
# - "Initializing consumer group offset synchronization"
# - "Offset sync completed: X groups synced, Y errors, duration: Zms"
# - "Synced N partition offsets for consumer group 'group-id'"
```

### Verify Offsets on Target Cluster

Check that offsets are being synced to the target cluster:

```bash
# List consumer groups on target
kafka-consumer-groups.sh --bootstrap-server target-kafka:9092 --list

# Describe specific group
kafka-consumer-groups.sh --bootstrap-server target-kafka:9092 \
  --group my-consumer-group --describe
```

### Log Messages

**Successful sync:**
```
[INFO] Offset synchronization initialized with interval: 60000ms
[INFO] Offset sync completed: 3 groups synced, 0 errors, duration: 245ms
[INFO] Synced 5 partition offsets for consumer group 'order-processor' from topic 'orders' to 'orders'
```

**Warnings:**
```
[WARN] Failed to sync offsets for consumer group 'inactive-group': Group not found
[WARN] Error during offset synchronization: Connection timeout
```

## Best Practices

### 1. Choose Appropriate Sync Interval

- **Low-frequency (60s+)**: Suitable for most use cases, reduces overhead
- **Medium-frequency (30s)**: Good balance for active consumer groups
- **High-frequency (10s)**: For critical applications, increases load

### 2. Filter Consumer Groups

Sync only necessary groups to reduce overhead:

```json
{
  "sync.consumer.groups": "critical-app,important-service"
}
```

### 3. Monitor Sync Performance

Watch for:
- Sync duration increasing over time
- Frequent errors in logs
- Consumer group lag on target cluster

### 4. Test Failover Scenarios

Before relying on offset sync for DR:
1. Enable offset sync
2. Let it run for several sync intervals
3. Stop consumers on source
4. Start consumers on target
5. Verify they resume from correct offsets

### 5. Handle Partition Count Differences

If source and target topics have different partition counts:
- Set `preserve.partitions=false`
- Offset sync will map to available partitions
- Consumer groups may need rebalancing

## Limitations

### Current Limitations

1. **Single Topic**: Syncs offsets only for the configured source/target topic pair
2. **Active Groups**: Best results with actively consuming groups
3. **Partition Mapping**: Assumes 1:1 partition mapping when `preserve.partitions=true`
4. **No Offset Translation**: Doesn't adjust offsets for compacted topics or different retention

### Not Supported

- Cross-topic offset mapping (different topic names)
- Offset adjustment for time-based sync
- Transactional offset commits
- Custom offset storage backends

## Troubleshooting

### Offsets Not Syncing

**Check configuration:**
```bash
curl http://localhost:8083/connectors/kafka-replicator | jq '.config'
```

Verify:
- `sync.consumer.offsets` is `"true"`
- Consumer groups exist on source cluster
- Groups consume from the source topic

**Check permissions:**
- Source cluster: `READ` on consumer group offsets
- Target cluster: `WRITE` on consumer group offsets

### Sync Errors in Logs

**"Group not found":**
- Consumer group may be inactive
- Group may not consume from the source topic
- Check group exists: `kafka-consumer-groups.sh --list`

**"Connection timeout":**
- Network issues between connector and clusters
- Check firewall rules
- Verify bootstrap servers configuration

**"Authorization failed":**
- Insufficient permissions on source or target
- Add required ACLs for offset management

### Performance Issues

**High sync duration:**
- Reduce number of consumer groups to sync
- Increase `offset.sync.interval.ms`
- Check network latency between clusters

**Memory usage:**
- Large number of consumer groups
- Many partitions per topic
- Consider increasing connector memory

## Examples

### Complete Configuration with Offset Sync

```json
{
  "name": "production-replicator",
  "config": {
    "connector.class": "com.octavalo.kafka.connector.CustomSourceConnector",
    "tasks.max": "1",
    
    "source.bootstrap.servers": "prod-kafka-1:9092,prod-kafka-2:9092",
    "source.topic": "transactions",
    "source.group.id": "replicator-prod",
    "source.security.protocol": "SASL_SSL",
    "source.sasl.mechanism": "SCRAM-SHA-256",
    "source.sasl.username": "replicator-user",
    "source.sasl.password": "secret",
    
    "target.topic": "transactions",
    
    "poll.timeout.ms": "1000",
    "max.poll.records": "500",
    "preserve.partitions": "true",
    "preserve.timestamps": "true",
    
    "sync.consumer.offsets": "true",
    "sync.consumer.groups": "payment-processor,fraud-detection,analytics",
    "offset.sync.interval.ms": "30000"
  }
}
```

### Docker Compose Configuration

Update your connector deployment in `docker/connector-config.json`:

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
    "preserve.timestamps": "true",
    "sync.consumer.offsets": "true",
    "offset.sync.interval.ms": "60000"
  }
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Source Kafka Cluster                      │
│                                                              │
│  ┌──────────────┐         ┌─────────────────────┐          │
│  │ Topic:       │         │ Consumer Groups:    │          │
│  │ orders       │◄────────│ - order-processor   │          │
│  │ (partitions) │         │ - analytics         │          │
│  └──────────────┘         │ - audit-service     │          │
│                           └─────────────────────┘          │
└───────────────────────────────┬──────────────────────────────┘
                                │
                                │ Replicator reads:
                                │ 1. Messages
                                │ 2. Consumer offsets
                                ▼
                    ┌───────────────────────┐
                    │  Kafka Replicator     │
                    │  Connector            │
                    │                       │
                    │  - Data replication   │
                    │  - Offset sync        │
                    └───────────┬───────────┘
                                │
                                │ Writes:
                                │ 1. Messages
                                │ 2. Consumer offsets
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    Target Kafka Cluster                      │
│                                                              │
│  ┌──────────────┐         ┌─────────────────────┐          │
│  │ Topic:       │         │ Consumer Groups:    │          │
│  │ orders       │────────►│ - order-processor   │          │
│  │ (partitions) │         │ - analytics         │          │
│  └──────────────┘         │ - audit-service     │          │
│                           └─────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## API Reference

### ConsumerGroupOffsetSync Class

The `ConsumerGroupOffsetSync` class handles offset synchronization:

```java
// Initialize
ConsumerGroupOffsetSync offsetSync = new ConsumerGroupOffsetSync(
    sourceAdminClient,
    targetAdminClient,
    sourceTopic,
    targetTopic,
    consumerGroupsConfig,
    syncIntervalMs
);

// Check if sync is needed
if (offsetSync.shouldSync()) {
    offsetSync.syncOffsets();
}

// Get statistics
Map<String, Object> stats = offsetSync.getSyncStats();

// Cleanup
offsetSync.close();
```

### Key Methods

- `shouldSync()`: Returns true if sync interval has elapsed
- `syncOffsets()`: Performs the offset synchronization
- `getSyncStats()`: Returns sync statistics
- `close()`: Closes admin clients

## See Also

- [Main README](README.md) - General connector documentation
- [Docker Setup](docker/README.md) - Testing environment
- [Authentication Guide](README.md#authentication-methods) - Security configuration
