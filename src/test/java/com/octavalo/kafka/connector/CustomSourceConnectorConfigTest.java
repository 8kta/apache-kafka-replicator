package com.octavalo.kafka.connector;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomSourceConnectorConfigTest {

    @Test
    void testMissingBootstrapServers() {
        Map<String, String> props = new HashMap<>();
        props.put("source.topic", "test-topic");

        assertThrows(ConfigException.class, () -> new CustomSourceConnectorConfig(props));
    }

    @Test
    void testMissingSourceTopic() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");

        assertThrows(ConfigException.class, () -> new CustomSourceConnectorConfig(props));
    }

    @Test
    void testRequiredConfigurations() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("localhost:9092", config.getSourceBootstrapServers());
        assertEquals("test-topic", config.getSourceTopic());
    }

    @Test
    void testDefaultValues() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("kafka-replicator-connector", config.getSourceGroupId());
        assertEquals(1000, config.getPollTimeoutMs());
        assertEquals(500, config.getMaxPollRecords());
        assertEquals("PLAINTEXT", config.getSourceSecurityProtocol());
        assertTrue(config.getPreservePartitions());
        assertTrue(config.getPreserveTimestamps());
    }

    @Test
    void testTargetTopicDefaultsToSourceTopic() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "source-topic");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("source-topic", config.getTargetTopic());
    }

    @Test
    void testTargetTopicOverride() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "source-topic");
        props.put("target.topic", "target-topic");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("target-topic", config.getTargetTopic());
    }

    @Test
    void testCustomValues() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "kafka:9092");
        props.put("source.topic", "my-topic");
        props.put("source.group.id", "my-group");
        props.put("poll.timeout.ms", "2000");
        props.put("max.poll.records", "1000");
        props.put("source.security.protocol", "SASL_SSL");
        props.put("preserve.partitions", "false");
        props.put("preserve.timestamps", "false");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("kafka:9092", config.getSourceBootstrapServers());
        assertEquals("my-topic", config.getSourceTopic());
        assertEquals("my-group", config.getSourceGroupId());
        assertEquals(2000, config.getPollTimeoutMs());
        assertEquals(1000, config.getMaxPollRecords());
        assertEquals("SASL_SSL", config.getSourceSecurityProtocol());
        assertFalse(config.getPreservePartitions());
        assertFalse(config.getPreserveTimestamps());
    }

    @Test
    void testSaslConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.sasl.mechanism", "PLAIN");
        props.put("source.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("PLAIN", config.getSourceSaslMechanism());
        assertNotNull(config.getSourceSaslJaasConfig());
        assertTrue(config.getSourceSaslJaasConfig().contains("username"));
    }

    @Test
    void testSaslUsernamePasswordPlain() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.sasl.mechanism", "PLAIN");
        props.put("source.sasl.username", "testuser");
        props.put("source.sasl.password", "testpass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        String jaasConfig = config.getSourceSaslJaasConfig();
        assertNotNull(jaasConfig);
        assertTrue(jaasConfig.contains("PlainLoginModule"));
        assertTrue(jaasConfig.contains("testuser"));
        assertTrue(jaasConfig.contains("testpass"));
    }

    @Test
    void testSaslUsernamePasswordScram() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.sasl.mechanism", "SCRAM-SHA-256");
        props.put("source.sasl.username", "testuser");
        props.put("source.sasl.password", "testpass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        String jaasConfig = config.getSourceSaslJaasConfig();
        assertNotNull(jaasConfig);
        assertTrue(jaasConfig.contains("ScramLoginModule"));
        assertTrue(jaasConfig.contains("testuser"));
        assertTrue(jaasConfig.contains("testpass"));
    }

    @Test
    void testSaslJaasConfigTakesPrecedence() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.sasl.mechanism", "PLAIN");
        props.put("source.sasl.jaas.config", "custom jaas config");
        props.put("source.sasl.username", "testuser");
        props.put("source.sasl.password", "testpass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        String jaasConfig = config.getSourceSaslJaasConfig();
        assertEquals("custom jaas config", jaasConfig);
    }

    @Test
    void testSslTruststoreConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.security.protocol", "SSL");
        props.put("source.ssl.truststore.location", "/path/to/truststore.jks");
        props.put("source.ssl.truststore.password", "truststore-pass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("/path/to/truststore.jks", config.getSourceSslTruststoreLocation());
        assertEquals("truststore-pass", config.getSourceSslTruststorePassword());
    }

    @Test
    void testSslKeystoreConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.security.protocol", "SSL");
        props.put("source.ssl.keystore.location", "/path/to/keystore.jks");
        props.put("source.ssl.keystore.password", "keystore-pass");
        props.put("source.ssl.key.password", "key-pass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("/path/to/keystore.jks", config.getSourceSslKeystoreLocation());
        assertEquals("keystore-pass", config.getSourceSslKeystorePassword());
        assertEquals("key-pass", config.getSourceSslKeyPassword());
    }

    @Test
    void testSaslSslCombinedConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("source.security.protocol", "SASL_SSL");
        props.put("source.sasl.mechanism", "SCRAM-SHA-256");
        props.put("source.sasl.username", "testuser");
        props.put("source.sasl.password", "testpass");
        props.put("source.ssl.truststore.location", "/path/to/truststore.jks");
        props.put("source.ssl.truststore.password", "truststore-pass");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertEquals("SASL_SSL", config.getSourceSecurityProtocol());
        assertEquals("SCRAM-SHA-256", config.getSourceSaslMechanism());
        assertNotNull(config.getSourceSaslJaasConfig());
        assertEquals("/path/to/truststore.jks", config.getSourceSslTruststoreLocation());
        assertEquals("truststore-pass", config.getSourceSslTruststorePassword());
    }

    @Test
    void testNullSslConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");

        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(props);

        assertNull(config.getSourceSslTruststoreLocation());
        assertNull(config.getSourceSslTruststorePassword());
        assertNull(config.getSourceSslKeystoreLocation());
        assertNull(config.getSourceSslKeystorePassword());
        assertNull(config.getSourceSslKeyPassword());
    }
}
