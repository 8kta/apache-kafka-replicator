package com.octavalo.kafka.connector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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
        try {
            if (props == null || props.isEmpty()) {
                throw new ConnectException("Task configuration cannot be null or empty");
            }
            
            config = new CustomSourceConnectorConfig(props);
            taskId = props.getOrDefault("task.id", "0");
            log.info("Task ID: {}", taskId);
        
            sourceTopic = config.getSourceTopic();
            targetTopic = config.getTargetTopic();
            preservePartitions = config.getPreservePartitions();
            preserveTimestamps = config.getPreserveTimestamps();
            
            log.debug("Configuring Kafka consumer for source cluster");
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
                log.debug("SASL mechanism configured: {}", config.getSourceSaslMechanism());
            }
            if (config.getSourceSaslJaasConfig() != null) {
                consumerProps.put("sasl.jaas.config", config.getSourceSaslJaasConfig());
                log.debug("SASL JAAS configuration provided");
            }
            
            // SSL configuration
            if (config.getSourceSslTruststoreLocation() != null) {
                consumerProps.put("ssl.truststore.location", config.getSourceSslTruststoreLocation());
                log.debug("SSL truststore location configured: {}", config.getSourceSslTruststoreLocation());
            }
            if (config.getSourceSslTruststorePassword() != null) {
                consumerProps.put("ssl.truststore.password", config.getSourceSslTruststorePassword());
                log.debug("SSL truststore password configured");
            }
            if (config.getSourceSslKeystoreLocation() != null) {
                consumerProps.put("ssl.keystore.location", config.getSourceSslKeystoreLocation());
                log.debug("SSL keystore location configured: {}", config.getSourceSslKeystoreLocation());
            }
            if (config.getSourceSslKeystorePassword() != null) {
                consumerProps.put("ssl.keystore.password", config.getSourceSslKeystorePassword());
                log.debug("SSL keystore password configured");
            }
            if (config.getSourceSslKeyPassword() != null) {
                consumerProps.put("ssl.key.password", config.getSourceSslKeyPassword());
                log.debug("SSL key password configured");
            }
            
            log.info("Creating Kafka consumer for source cluster: {}", config.getSourceBootstrapServers());
            log.debug("Security protocol: {}, SASL mechanism: {}", 
                    config.getSourceSecurityProtocol(), config.getSourceSaslMechanism());
            consumer = new KafkaConsumer<>(consumerProps);
        
            
            log.debug("Checking for stored offsets");
            Map<String, Object> offset = context.offsetStorageReader()
                    .offset(Collections.singletonMap("source_topic", sourceTopic));
            
            if (offset != null && !offset.isEmpty()) {
                log.info("Resuming from stored offsets");
                Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
                for (Map.Entry<String, Object> entry : offset.entrySet()) {
                    if (entry.getKey().startsWith("partition_")) {
                        try {
                            int partition = Integer.parseInt(entry.getKey().substring("partition_".length()));
                            long offsetValue = ((Number) entry.getValue()).longValue();
                            TopicPartition tp = new TopicPartition(sourceTopic, partition);
                            partitionOffsets.put(tp, offsetValue);
                            log.debug("Found stored offset {} for partition {}", offsetValue, partition);
                        } catch (NumberFormatException e) {
                            log.warn("Invalid offset entry: {}", entry.getKey(), e);
                        }
                    }
                }
                
                if (!partitionOffsets.isEmpty()) {
                    consumer.assign(partitionOffsets.keySet());
                    for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                        consumer.seek(entry.getKey(), entry.getValue());
                        log.info("Seeking to offset {} for partition {}", entry.getValue(), entry.getKey().partition());
                    }
                } else {
                    log.warn("No valid partition offsets found, subscribing from beginning");
                    consumer.subscribe(Collections.singletonList(sourceTopic));
                }
            } else {
                log.info("Starting from beginning - no stored offsets found");
                consumer.subscribe(Collections.singletonList(sourceTopic));
            }
            
            log.info("CustomSourceTask started successfully");
            log.info("Replicating from source topic '{}' to target topic '{}'", sourceTopic, targetTopic);
            log.info("Preserve partitions: {}, Preserve timestamps: {}", preservePartitions, preserveTimestamps);
            
        } catch (KafkaException e) {
            log.error("Kafka error while starting task: {}", e.getMessage(), e);
            closeConsumer();
            throw new ConnectException("Failed to start task due to Kafka error", e);
        } catch (Exception e) {
            log.error("Unexpected error while starting task: {}", e.getMessage(), e);
            closeConsumer();
            throw new ConnectException("Failed to start task", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            if (consumer == null) {
                log.error("Consumer is null, task may not have been started properly");
                throw new ConnectException("Consumer is not initialized");
            }
            
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(config.getPollTimeoutMs()));
            
            if (records.isEmpty()) {
                log.trace("No records available in this poll cycle");
                return null;
            }
            
            log.debug("Polled {} records from source topic '{}'", records.count(), sourceTopic);
            
            List<SourceRecord> sourceRecords = new ArrayList<>();
            Map<String, Object> sourcePartition = Collections.singletonMap("source_topic", sourceTopic);
            int processedCount = 0;
            int errorCount = 0;
            
            for (ConsumerRecord<byte[], byte[]> record : records) {
                try {
                    Map<String, Object> sourceOffset = new HashMap<>();
                    sourceOffset.put("partition_" + record.partition(), record.offset() + 1);
                    
                    Integer targetPartition = preservePartitions ? record.partition() : null;
                    Long timestamp = preserveTimestamps ? record.timestamp() : null;
                    
                    ConnectHeaders connectHeaders = new ConnectHeaders();
                    if (record.headers() != null) {
                        record.headers().forEach(header -> {
                            try {
                                connectHeaders.add(header.key(), header.value(), Schema.OPTIONAL_BYTES_SCHEMA);
                            } catch (Exception e) {
                                log.warn("Failed to add header '{}': {}", header.key(), e.getMessage());
                            }
                        });
                    }
                    
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
                    processedCount++;
                    
                } catch (Exception e) {
                    errorCount++;
                    log.error("Error processing record from partition {} at offset {}: {}", 
                            record.partition(), record.offset(), e.getMessage(), e);
                }
            }
            
            if (errorCount > 0) {
                log.warn("Processed {} records with {} errors", processedCount, errorCount);
            } else {
                log.debug("Successfully processed {} records", processedCount);
            }
            
            return sourceRecords.isEmpty() ? null : sourceRecords;
            
        } catch (WakeupException e) {
            log.info("Consumer wakeup called, likely shutting down");
            throw new InterruptedException("Consumer wakeup");
        } catch (KafkaException e) {
            log.error("Kafka error during poll: {}", e.getMessage(), e);
            throw new RetriableException("Kafka error during poll, will retry", e);
        } catch (Exception e) {
            log.error("Unexpected error during poll: {}", e.getMessage(), e);
            throw new ConnectException("Failed to poll records", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping CustomSourceTask");
        closeConsumer();
        log.info("CustomSourceTask stopped successfully");
    }
    
    private void closeConsumer() {
        if (consumer != null) {
            try {
                log.debug("Closing Kafka consumer");
                consumer.close(Duration.ofSeconds(30));
                log.info("Source Kafka consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing consumer: {}", e.getMessage(), e);
            } finally {
                consumer = null;
            }
        }
    }
}
