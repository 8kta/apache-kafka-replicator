package com.octavalo.kafka.connector;

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
}
