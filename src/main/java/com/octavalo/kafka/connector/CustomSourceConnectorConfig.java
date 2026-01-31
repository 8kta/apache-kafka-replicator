package com.octavalo.kafka.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class CustomSourceConnectorConfig extends AbstractConfig {

    public static final String SOURCE_BOOTSTRAP_SERVERS_CONFIG = "source.bootstrap.servers";
    private static final String SOURCE_BOOTSTRAP_SERVERS_DOC = "Source Kafka cluster bootstrap servers";

    public static final String SOURCE_TOPIC_CONFIG = "source.topic";
    private static final String SOURCE_TOPIC_DOC = "Source Kafka topic to replicate from";

    public static final String TARGET_TOPIC_CONFIG = "target.topic";
    private static final String TARGET_TOPIC_DOC = "Target Kafka topic to replicate to (defaults to source topic name if not specified)";

    public static final String SOURCE_GROUP_ID_CONFIG = "source.group.id";
    private static final String SOURCE_GROUP_ID_DOC = "Consumer group ID for source cluster";
    private static final String SOURCE_GROUP_ID_DEFAULT = "kafka-replicator-connector";

    public static final String POLL_TIMEOUT_MS_CONFIG = "poll.timeout.ms";
    private static final String POLL_TIMEOUT_MS_DOC = "Timeout in milliseconds for polling source cluster";
    private static final int POLL_TIMEOUT_MS_DEFAULT = 1000;

    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "Maximum number of records to poll in a single batch";
    private static final int MAX_POLL_RECORDS_DEFAULT = 500;

    public static final String SOURCE_SECURITY_PROTOCOL_CONFIG = "source.security.protocol";
    private static final String SOURCE_SECURITY_PROTOCOL_DOC = "Security protocol for source cluster (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)";
    private static final String SOURCE_SECURITY_PROTOCOL_DEFAULT = "PLAINTEXT";

    public static final String SOURCE_SASL_MECHANISM_CONFIG = "source.sasl.mechanism";
    private static final String SOURCE_SASL_MECHANISM_DOC = "SASL mechanism for source cluster authentication";

    public static final String SOURCE_SASL_JAAS_CONFIG = "source.sasl.jaas.config";
    private static final String SOURCE_SASL_JAAS_CONFIG_DOC = "JAAS configuration for source cluster SASL authentication";

    public static final String PRESERVE_PARTITIONS_CONFIG = "preserve.partitions";
    private static final String PRESERVE_PARTITIONS_DOC = "Preserve source partition assignment in target topic";
    private static final boolean PRESERVE_PARTITIONS_DEFAULT = true;

    public static final String PRESERVE_TIMESTAMPS_CONFIG = "preserve.timestamps";
    private static final String PRESERVE_TIMESTAMPS_DOC = "Preserve original message timestamps";
    private static final boolean PRESERVE_TIMESTAMPS_DEFAULT = true;

    public CustomSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(SOURCE_BOOTSTRAP_SERVERS_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        SOURCE_BOOTSTRAP_SERVERS_DOC)
                .define(SOURCE_TOPIC_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        SOURCE_TOPIC_DOC)
                .define(TARGET_TOPIC_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        TARGET_TOPIC_DOC)
                .define(SOURCE_GROUP_ID_CONFIG,
                        Type.STRING,
                        SOURCE_GROUP_ID_DEFAULT,
                        Importance.MEDIUM,
                        SOURCE_GROUP_ID_DOC)
                .define(POLL_TIMEOUT_MS_CONFIG,
                        Type.INT,
                        POLL_TIMEOUT_MS_DEFAULT,
                        Importance.MEDIUM,
                        POLL_TIMEOUT_MS_DOC)
                .define(MAX_POLL_RECORDS_CONFIG,
                        Type.INT,
                        MAX_POLL_RECORDS_DEFAULT,
                        Importance.MEDIUM,
                        MAX_POLL_RECORDS_DOC)
                .define(SOURCE_SECURITY_PROTOCOL_CONFIG,
                        Type.STRING,
                        SOURCE_SECURITY_PROTOCOL_DEFAULT,
                        Importance.MEDIUM,
                        SOURCE_SECURITY_PROTOCOL_DOC)
                .define(SOURCE_SASL_MECHANISM_CONFIG,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        SOURCE_SASL_MECHANISM_DOC)
                .define(SOURCE_SASL_JAAS_CONFIG,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        SOURCE_SASL_JAAS_CONFIG_DOC)
                .define(PRESERVE_PARTITIONS_CONFIG,
                        Type.BOOLEAN,
                        PRESERVE_PARTITIONS_DEFAULT,
                        Importance.LOW,
                        PRESERVE_PARTITIONS_DOC)
                .define(PRESERVE_TIMESTAMPS_CONFIG,
                        Type.BOOLEAN,
                        PRESERVE_TIMESTAMPS_DEFAULT,
                        Importance.LOW,
                        PRESERVE_TIMESTAMPS_DOC);
    }

    public String getSourceBootstrapServers() {
        return getString(SOURCE_BOOTSTRAP_SERVERS_CONFIG);
    }

    public String getSourceTopic() {
        return getString(SOURCE_TOPIC_CONFIG);
    }

    public String getTargetTopic() {
        String target = getString(TARGET_TOPIC_CONFIG);
        return target != null ? target : getSourceTopic();
    }

    public String getSourceGroupId() {
        return getString(SOURCE_GROUP_ID_CONFIG);
    }

    public int getPollTimeoutMs() {
        return getInt(POLL_TIMEOUT_MS_CONFIG);
    }

    public int getMaxPollRecords() {
        return getInt(MAX_POLL_RECORDS_CONFIG);
    }

    public String getSourceSecurityProtocol() {
        return getString(SOURCE_SECURITY_PROTOCOL_CONFIG);
    }

    public String getSourceSaslMechanism() {
        return getString(SOURCE_SASL_MECHANISM_CONFIG);
    }

    public String getSourceSaslJaasConfig() {
        return getPassword(SOURCE_SASL_JAAS_CONFIG) != null ? 
               getPassword(SOURCE_SASL_JAAS_CONFIG).value() : null;
    }

    public boolean getPreservePartitions() {
        return getBoolean(PRESERVE_PARTITIONS_CONFIG);
    }

    public boolean getPreserveTimestamps() {
        return getBoolean(PRESERVE_TIMESTAMPS_CONFIG);
    }
}
