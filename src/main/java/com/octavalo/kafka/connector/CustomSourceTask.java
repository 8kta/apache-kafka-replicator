package com.octavalo.kafka.connector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CustomSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(CustomSourceTask.class);
    private CustomSourceConnectorConfig config;
    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceTopic;
    private String targetTopic;
    private String taskId;
    private boolean preservePartitions;
    private boolean preserveTimestamps;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting CustomSourceTask for Kafka replication");
        config = new CustomSourceConnectorConfig(props);
        taskId = props.getOrDefault("task.id", "0");
        
        sourceTopic = config.getSourceTopic();
        targetTopic = config.getTargetTopic();
        preservePartitions = config.getPreservePartitions();
        preserveTimestamps = config.getPreserveTimestamps();
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSourceBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getSourceGroupId() + "-task-" + taskId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollRecords());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("security.protocol", config.getSourceSecurityProtocol());
        
        if (config.getSourceSaslMechanism() != null) {
            consumerProps.put("sasl.mechanism", config.getSourceSaslMechanism());
        }
        if (config.getSourceSaslJaasConfig() != null) {
            consumerProps.put("sasl.jaas.config", config.getSourceSaslJaasConfig());
        }
        
        consumer = new KafkaConsumer<>(consumerProps);
        
        Map<String, Object> offset = context.offsetStorageReader()
                .offset(Collections.singletonMap("source_topic", sourceTopic));
        
        if (offset != null && !offset.isEmpty()) {
            log.info("Resuming from stored offsets");
            Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
            for (Map.Entry<String, Object> entry : offset.entrySet()) {
                if (entry.getKey().startsWith("partition_")) {
                    int partition = Integer.parseInt(entry.getKey().substring("partition_".length()));
                    long offsetValue = ((Number) entry.getValue()).longValue();
                    TopicPartition tp = new TopicPartition(sourceTopic, partition);
                    partitionOffsets.put(tp, offsetValue);
                }
            }
            consumer.assign(partitionOffsets.keySet());
            for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
                log.info("Seeking to offset {} for partition {}", entry.getValue(), entry.getKey().partition());
            }
        } else {
            log.info("Starting from beginning - no stored offsets found");
            consumer.subscribe(Collections.singletonList(sourceTopic));
        }
        
        log.info("CustomSourceTask started - replicating from source topic '{}' to target topic '{}'", 
                sourceTopic, targetTopic);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(config.getPollTimeoutMs()));
        
        if (records.isEmpty()) {
            return null;
        }
        
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Map<String, Object> sourcePartition = Collections.singletonMap("source_topic", sourceTopic);
        
        for (ConsumerRecord<byte[], byte[]> record : records) {
            Map<String, Object> sourceOffset = new HashMap<>();
            sourceOffset.put("partition_" + record.partition(), record.offset() + 1);
            
            Integer targetPartition = preservePartitions ? record.partition() : null;
            Long timestamp = preserveTimestamps ? record.timestamp() : null;
            
            ConnectHeaders connectHeaders = new ConnectHeaders();
            record.headers().forEach(header -> 
                connectHeaders.add(header.key(), header.value(), Schema.OPTIONAL_BYTES_SCHEMA)
            );
            
            SourceRecord sourceRecord = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    targetTopic,
                    targetPartition,
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    record.key(),
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    record.value(),
                    timestamp,
                    connectHeaders
            );
            
            sourceRecords.add(sourceRecord);
        }
        
        log.debug("Polled {} records from source topic '{}'", sourceRecords.size(), sourceTopic);
        return sourceRecords;
    }

    @Override
    public void stop() {
        log.info("Stopping CustomSourceTask");
        if (consumer != null) {
            consumer.close();
            log.info("Source Kafka consumer closed");
        }
    }
}
