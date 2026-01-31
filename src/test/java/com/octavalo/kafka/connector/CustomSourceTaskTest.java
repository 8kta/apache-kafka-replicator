package com.octavalo.kafka.connector;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class CustomSourceTaskTest {

    private CustomSourceTask task;
    private Map<String, String> props;

    @Mock
    private SourceTaskContext context;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        task = new CustomSourceTask();
        
        props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-source-topic");
        props.put("target.topic", "test-target-topic");
        props.put("source.group.id", "test-group");
        props.put("poll.timeout.ms", "100");
        props.put("max.poll.records", "10");
    }

    @Test
    void testVersion() {
        assertEquals("1.0.0", task.version());
    }

    @Test
    void testStartWithMockedContext() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        
        task.initialize(context);
        
        assertDoesNotThrow(() -> task.start(props));
    }

    @Test
    void testStop() {
        assertDoesNotThrow(() -> task.stop());
    }

    @Test
    void testConfigurationParsing() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        
        task.initialize(context);
        
        assertDoesNotThrow(() -> task.start(props));
    }

    @Test
    void testStartWithNullProps() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        task.initialize(context);
        
        assertThrows(ConnectException.class, () -> task.start(null));
    }

    @Test
    void testStartWithEmptyProps() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        task.initialize(context);
        
        assertThrows(ConnectException.class, () -> task.start(new HashMap<>()));
    }

    @Test
    void testAuthenticationConfigurationValidation() {
        Map<String, String> authProps = new HashMap<>(props);
        authProps.put("source.security.protocol", "SASL_PLAINTEXT");
        authProps.put("source.sasl.mechanism", "PLAIN");
        authProps.put("source.sasl.username", "testuser");
        authProps.put("source.sasl.password", "testpass");
        
        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(authProps);
        
        assertEquals("SASL_PLAINTEXT", config.getSourceSecurityProtocol());
        assertEquals("PLAIN", config.getSourceSaslMechanism());
        assertNotNull(config.getSourceSaslJaasConfig());
        assertTrue(config.getSourceSaslJaasConfig().contains("testuser"));
    }

    @Test
    void testSslConfigurationValidation() {
        Map<String, String> sslProps = new HashMap<>(props);
        sslProps.put("source.ssl.truststore.location", "/path/to/truststore.jks");
        sslProps.put("source.ssl.truststore.password", "truststore-pass");
        
        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(sslProps);
        
        assertEquals("/path/to/truststore.jks", config.getSourceSslTruststoreLocation());
        assertEquals("truststore-pass", config.getSourceSslTruststorePassword());
    }

    @Test
    void testScramConfigurationValidation() {
        Map<String, String> scramProps = new HashMap<>(props);
        scramProps.put("source.sasl.mechanism", "SCRAM-SHA-256");
        scramProps.put("source.sasl.username", "testuser");
        scramProps.put("source.sasl.password", "testpass");
        
        CustomSourceConnectorConfig config = new CustomSourceConnectorConfig(scramProps);
        
        assertEquals("SCRAM-SHA-256", config.getSourceSaslMechanism());
        assertNotNull(config.getSourceSaslJaasConfig());
        assertTrue(config.getSourceSaslJaasConfig().contains("ScramLoginModule"));
    }

    @Test
    void testMultipleStopCalls() {
        assertDoesNotThrow(() -> {
            task.stop();
            task.stop();
        });
    }

    @Test
    void testStopWithoutStart() {
        assertDoesNotThrow(() -> task.stop());
    }
}
