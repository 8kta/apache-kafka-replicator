package com.octavalo.kafka.connector;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomSourceConnectorConfigTest {

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
}
