package com.octavalo.kafka.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomSourceConnectorConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(CustomSourceConnectorConfig.class);

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

    public static final String SOURCE_SASL_USERNAME_CONFIG = "source.sasl.username";
    private static final String SOURCE_SASL_USERNAME_DOC = "Username for SASL authentication (used to build JAAS config if source.sasl.jaas.config is not provided)";

    public static final String SOURCE_SASL_PASSWORD_CONFIG = "source.sasl.password";
    private static final String SOURCE_SASL_PASSWORD_DOC = "Password for SASL authentication (used to build JAAS config if source.sasl.jaas.config is not provided)";

    public static final String SOURCE_SSL_TRUSTSTORE_LOCATION_CONFIG = "source.ssl.truststore.location";
    private static final String SOURCE_SSL_TRUSTSTORE_LOCATION_DOC = "Location of the truststore file for SSL";

    public static final String SOURCE_SSL_TRUSTSTORE_PASSWORD_CONFIG = "source.ssl.truststore.password";
    private static final String SOURCE_SSL_TRUSTSTORE_PASSWORD_DOC = "Password for the truststore";

    public static final String SOURCE_SSL_KEYSTORE_LOCATION_CONFIG = "source.ssl.keystore.location";
    private static final String SOURCE_SSL_KEYSTORE_LOCATION_DOC = "Location of the keystore file for SSL";

    public static final String SOURCE_SSL_KEYSTORE_PASSWORD_CONFIG = "source.ssl.keystore.password";
    private static final String SOURCE_SSL_KEYSTORE_PASSWORD_DOC = "Password for the keystore";

    public static final String SOURCE_SSL_KEY_PASSWORD_CONFIG = "source.ssl.key.password";
    private static final String SOURCE_SSL_KEY_PASSWORD_DOC = "Password for the key in the keystore";

    public static final String PRESERVE_PARTITIONS_CONFIG = "preserve.partitions";
    private static final String PRESERVE_PARTITIONS_DOC = "Preserve source partition assignment in target topic";
    private static final boolean PRESERVE_PARTITIONS_DEFAULT = true;

    public static final String PRESERVE_TIMESTAMPS_CONFIG = "preserve.timestamps";
    private static final String PRESERVE_TIMESTAMPS_DOC = "Preserve original message timestamps";
    private static final boolean PRESERVE_TIMESTAMPS_DEFAULT = true;

    public static final String SYNC_CONSUMER_OFFSETS_CONFIG = "sync.consumer.offsets";
    private static final String SYNC_CONSUMER_OFFSETS_DOC = "Enable synchronization of consumer group offsets from source to target cluster";
    private static final boolean SYNC_CONSUMER_OFFSETS_DEFAULT = false;

    public static final String SYNC_CONSUMER_GROUPS_CONFIG = "sync.consumer.groups";
    private static final String SYNC_CONSUMER_GROUPS_DOC = "Comma-separated list of consumer groups to sync offsets for (empty means all groups)";

    public static final String OFFSET_SYNC_INTERVAL_MS_CONFIG = "offset.sync.interval.ms";
    private static final String OFFSET_SYNC_INTERVAL_MS_DOC = "Interval in milliseconds for syncing consumer group offsets";
    private static final int OFFSET_SYNC_INTERVAL_MS_DEFAULT = 60000;

    public static final String OFFSET_SYNC_TOPIC_CONFIG = "offset.sync.topic";
    private static final String OFFSET_SYNC_TOPIC_DOC = "Internal topic for tracking offset sync state (defaults to __consumer_offsets_sync)";
    private static final String OFFSET_SYNC_TOPIC_DEFAULT = "__consumer_offsets_sync";

    public CustomSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        try {
            validateConfiguration();
            log.debug("Configuration validated successfully");
        } catch (ConfigException e) {
            log.error("Configuration validation failed: {}", e.getMessage());
            throw e;
        }
    }
    
    private void validateConfiguration() {
        String bootstrapServers = getSourceBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new ConfigException(SOURCE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, 
                    "Bootstrap servers cannot be null or empty");
        }
        
        String sourceTopic = getSourceTopic();
        if (sourceTopic == null || sourceTopic.trim().isEmpty()) {
            throw new ConfigException(SOURCE_TOPIC_CONFIG, sourceTopic, 
                    "Source topic cannot be null or empty");
        }
        
        if (getPollTimeoutMs() < 0) {
            throw new ConfigException(POLL_TIMEOUT_MS_CONFIG, getPollTimeoutMs(), 
                    "Poll timeout must be non-negative");
        }
        
        if (getMaxPollRecords() <= 0) {
            throw new ConfigException(MAX_POLL_RECORDS_CONFIG, getMaxPollRecords(), 
                    "Max poll records must be positive");
        }
        
        log.debug("Configuration validation: bootstrap.servers={}, source.topic={}, target.topic={}",
                bootstrapServers, sourceTopic, getTargetTopic());
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
                .define(SOURCE_SASL_USERNAME_CONFIG,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        SOURCE_SASL_USERNAME_DOC)
                .define(SOURCE_SASL_PASSWORD_CONFIG,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        SOURCE_SASL_PASSWORD_DOC)
                .define(SOURCE_SSL_TRUSTSTORE_LOCATION_CONFIG,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        SOURCE_SSL_TRUSTSTORE_LOCATION_DOC)
                .define(SOURCE_SSL_TRUSTSTORE_PASSWORD_CONFIG,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        SOURCE_SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(SOURCE_SSL_KEYSTORE_LOCATION_CONFIG,
                        Type.STRING,
                        null,
                        Importance.LOW,
                        SOURCE_SSL_KEYSTORE_LOCATION_DOC)
                .define(SOURCE_SSL_KEYSTORE_PASSWORD_CONFIG,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        SOURCE_SSL_KEYSTORE_PASSWORD_DOC)
                .define(SOURCE_SSL_KEY_PASSWORD_CONFIG,
                        Type.PASSWORD,
                        null,
                        Importance.LOW,
                        SOURCE_SSL_KEY_PASSWORD_DOC)
                .define(PRESERVE_PARTITIONS_CONFIG,
                        Type.BOOLEAN,
                        PRESERVE_PARTITIONS_DEFAULT,
                        Importance.LOW,
                        PRESERVE_PARTITIONS_DOC)
                .define(PRESERVE_TIMESTAMPS_CONFIG,
                        Type.BOOLEAN,
                        PRESERVE_TIMESTAMPS_DEFAULT,
                        Importance.LOW,
                        PRESERVE_TIMESTAMPS_DOC)
                .define(SYNC_CONSUMER_OFFSETS_CONFIG,
                        Type.BOOLEAN,
                        SYNC_CONSUMER_OFFSETS_DEFAULT,
                        Importance.MEDIUM,
                        SYNC_CONSUMER_OFFSETS_DOC)
                .define(SYNC_CONSUMER_GROUPS_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        SYNC_CONSUMER_GROUPS_DOC)
                .define(OFFSET_SYNC_INTERVAL_MS_CONFIG,
                        Type.INT,
                        OFFSET_SYNC_INTERVAL_MS_DEFAULT,
                        Importance.LOW,
                        OFFSET_SYNC_INTERVAL_MS_DOC)
                .define(OFFSET_SYNC_TOPIC_CONFIG,
                        Type.STRING,
                        OFFSET_SYNC_TOPIC_DEFAULT,
                        Importance.LOW,
                        OFFSET_SYNC_TOPIC_DOC);
    }

    public String getSourceBootstrapServers() {
        try {
            return getString(SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        } catch (Exception e) {
            log.error("Error getting source bootstrap servers: {}", e.getMessage());
            throw new ConfigException("Failed to retrieve source bootstrap servers", e);
        }
    }

    public String getSourceTopic() {
        try {
            return getString(SOURCE_TOPIC_CONFIG);
        } catch (Exception e) {
            log.error("Error getting source topic: {}", e.getMessage());
            throw new ConfigException("Failed to retrieve source topic", e);
        }
    }

    public String getTargetTopic() {
        try {
            String target = getString(TARGET_TOPIC_CONFIG);
            String result = target != null && !target.trim().isEmpty() ? target : getSourceTopic();
            log.trace("Target topic resolved to: {}", result);
            return result;
        } catch (Exception e) {
            log.error("Error getting target topic: {}", e.getMessage());
            throw new ConfigException("Failed to retrieve target topic", e);
        }
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
        String jaasConfig = getPassword(SOURCE_SASL_JAAS_CONFIG) != null ? 
               getPassword(SOURCE_SASL_JAAS_CONFIG).value() : null;
        
        if (jaasConfig == null) {
            String username = getString(SOURCE_SASL_USERNAME_CONFIG);
            String password = getPassword(SOURCE_SASL_PASSWORD_CONFIG) != null ?
                    getPassword(SOURCE_SASL_PASSWORD_CONFIG).value() : null;
            
            if (username != null && password != null) {
                String mechanism = getSourceSaslMechanism();
                if ("PLAIN".equalsIgnoreCase(mechanism)) {
                    jaasConfig = String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        username, password);
                    log.debug("Built JAAS config from username/password for PLAIN mechanism");
                } else if ("SCRAM-SHA-256".equalsIgnoreCase(mechanism) || "SCRAM-SHA-512".equalsIgnoreCase(mechanism)) {
                    jaasConfig = String.format(
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                        username, password);
                    log.debug("Built JAAS config from username/password for {} mechanism", mechanism);
                }
            }
        }
        
        return jaasConfig;
    }
    
    public String getSourceSslTruststoreLocation() {
        return getString(SOURCE_SSL_TRUSTSTORE_LOCATION_CONFIG);
    }
    
    public String getSourceSslTruststorePassword() {
        return getPassword(SOURCE_SSL_TRUSTSTORE_PASSWORD_CONFIG) != null ?
                getPassword(SOURCE_SSL_TRUSTSTORE_PASSWORD_CONFIG).value() : null;
    }
    
    public String getSourceSslKeystoreLocation() {
        return getString(SOURCE_SSL_KEYSTORE_LOCATION_CONFIG);
    }
    
    public String getSourceSslKeystorePassword() {
        return getPassword(SOURCE_SSL_KEYSTORE_PASSWORD_CONFIG) != null ?
                getPassword(SOURCE_SSL_KEYSTORE_PASSWORD_CONFIG).value() : null;
    }
    
    public String getSourceSslKeyPassword() {
        return getPassword(SOURCE_SSL_KEY_PASSWORD_CONFIG) != null ?
                getPassword(SOURCE_SSL_KEY_PASSWORD_CONFIG).value() : null;
    }

    public boolean getPreservePartitions() {
        return getBoolean(PRESERVE_PARTITIONS_CONFIG);
    }

    public boolean getPreserveTimestamps() {
        return getBoolean(PRESERVE_TIMESTAMPS_CONFIG);
    }

    public boolean getSyncConsumerOffsets() {
        return getBoolean(SYNC_CONSUMER_OFFSETS_CONFIG);
    }

    public String getSyncConsumerGroups() {
        return getString(SYNC_CONSUMER_GROUPS_CONFIG);
    }

    public int getOffsetSyncIntervalMs() {
        return getInt(OFFSET_SYNC_INTERVAL_MS_CONFIG);
    }

    public String getOffsetSyncTopic() {
        return getString(OFFSET_SYNC_TOPIC_CONFIG);
    }
}
