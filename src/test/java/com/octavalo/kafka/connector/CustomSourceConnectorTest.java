package com.octavalo.kafka.connector;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomSourceConnectorTest {

    private CustomSourceConnector connector;
    private Map<String, String> props;

    @BeforeEach
    void setUp() {
        connector = new CustomSourceConnector();
        props = new HashMap<>();
        props.put("source.bootstrap.servers", "localhost:9092");
        props.put("source.topic", "test-topic");
        props.put("target.topic", "target-topic");
    }

    @Test
    void testVersion() {
        assertEquals("1.0.0", connector.version());
    }

    @Test
    void testTaskClass() {
        assertEquals(CustomSourceTask.class, connector.taskClass());
    }

    @Test
    void testStartAndStop() {
        assertDoesNotThrow(() -> {
            connector.start(props);
            connector.stop();
        });
    }

    @Test
    void testTaskConfigs() {
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);
        
        assertEquals(3, taskConfigs.size());
        
        for (int i = 0; i < taskConfigs.size(); i++) {
            Map<String, String> taskConfig = taskConfigs.get(i);
            assertEquals(String.valueOf(i), taskConfig.get("task.id"));
            assertEquals("localhost:9092", taskConfig.get("source.bootstrap.servers"));
            assertEquals("test-topic", taskConfig.get("source.topic"));
            assertEquals("target-topic", taskConfig.get("target.topic"));
        }
    }

    @Test
    void testTaskConfigsSingleTask() {
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        
        assertEquals(1, taskConfigs.size());
        assertEquals("0", taskConfigs.get(0).get("task.id"));
    }

    @Test
    void testConfigDef() {
        assertNotNull(connector.config());
        assertTrue(connector.config().configKeys().containsKey("source.bootstrap.servers"));
        assertTrue(connector.config().configKeys().containsKey("source.topic"));
        assertTrue(connector.config().configKeys().containsKey("target.topic"));
        assertTrue(connector.config().configKeys().containsKey("source.sasl.username"));
        assertTrue(connector.config().configKeys().containsKey("source.ssl.truststore.location"));
    }

    @Test
    void testStartWithNullProps() {
        assertThrows(ConnectException.class, () -> connector.start(null));
    }

    @Test
    void testStartWithEmptyProps() {
        assertThrows(ConnectException.class, () -> connector.start(new HashMap<>()));
    }

    @Test
    void testTaskConfigsWithInvalidMaxTasks() {
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(0);
        
        assertEquals(1, taskConfigs.size());
    }

    @Test
    void testTaskConfigsWithNegativeMaxTasks() {
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(-1);
        
        assertEquals(1, taskConfigs.size());
    }

    @Test
    void testStartWithAuthenticationConfig() {
        Map<String, String> authProps = new HashMap<>(props);
        authProps.put("source.security.protocol", "SASL_SSL");
        authProps.put("source.sasl.mechanism", "PLAIN");
        authProps.put("source.sasl.username", "testuser");
        authProps.put("source.sasl.password", "testpass");
        
        assertDoesNotThrow(() -> {
            connector.start(authProps);
            connector.stop();
        });
    }

    @Test
    void testMultipleStartStopCycles() {
        assertDoesNotThrow(() -> {
            connector.start(props);
            connector.stop();
            connector.start(props);
            connector.stop();
        });
    }
}
