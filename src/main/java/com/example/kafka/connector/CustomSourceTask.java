package com.example.kafka.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CustomSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(CustomSourceTask.class);
    private CustomSourceConnectorConfig config;
    private String topic;
    private int pollIntervalMs;
    private int batchSize;
    private AtomicLong offset = new AtomicLong(0);
    private String taskId;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting CustomSourceTask");
        config = new CustomSourceConnectorConfig(props);
        topic = config.getTopic();
        pollIntervalMs = config.getPollIntervalMs();
        batchSize = config.getBatchSize();
        taskId = props.getOrDefault("task.id", "0");
        
        Map<String, Object> lastOffset = context.offsetStorageReader()
                .offset(Collections.singletonMap("task", taskId));
        
        if (lastOffset != null && lastOffset.containsKey("offset")) {
            offset.set((Long) lastOffset.get("offset"));
            log.info("Resuming from offset: {}", offset.get());
        } else {
            log.info("Starting from offset: 0");
        }
        
        log.info("CustomSourceTask started for topic: {}", topic);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(pollIntervalMs);
        
        List<SourceRecord> records = new ArrayList<>();
        
        for (int i = 0; i < batchSize; i++) {
            long currentOffset = offset.incrementAndGet();
            
            Map<String, String> sourcePartition = Collections.singletonMap("task", taskId);
            Map<String, Long> sourceOffset = Collections.singletonMap("offset", currentOffset);
            
            String key = "key-" + currentOffset;
            String value = "value-" + currentOffset + "-from-task-" + taskId;
            
            SourceRecord record = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topic,
                    Schema.STRING_SCHEMA,
                    key,
                    Schema.STRING_SCHEMA,
                    value
            );
            
            records.add(record);
        }
        
        log.debug("Polled {} records, current offset: {}", records.size(), offset.get());
        return records;
    }

    @Override
    public void stop() {
        log.info("Stopping CustomSourceTask at offset: {}", offset.get());
    }
}
