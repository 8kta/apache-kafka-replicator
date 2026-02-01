package com.octavalo.kafka.connector;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Manages synchronization of consumer group offsets from source to target cluster.
 * This allows maintaining consumer position consistency across clusters.
 */
public class ConsumerGroupOffsetSync {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsetSync.class);

    private final AdminClient sourceAdminClient;
    private final AdminClient targetAdminClient;
    private final String sourceTopic;
    private final String targetTopic;
    private final Set<String> consumerGroupsToSync;
    private final boolean syncAllGroups;
    private long lastSyncTime;
    private final long syncIntervalMs;

    public ConsumerGroupOffsetSync(
            AdminClient sourceAdminClient,
            AdminClient targetAdminClient,
            String sourceTopic,
            String targetTopic,
            String consumerGroupsConfig,
            long syncIntervalMs) {
        
        this.sourceAdminClient = sourceAdminClient;
        this.targetAdminClient = targetAdminClient;
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
        this.syncIntervalMs = syncIntervalMs;
        this.lastSyncTime = 0;

        if (consumerGroupsConfig == null || consumerGroupsConfig.trim().isEmpty()) {
            this.syncAllGroups = true;
            this.consumerGroupsToSync = new HashSet<>();
            log.info("Offset sync configured for ALL consumer groups");
        } else {
            this.syncAllGroups = false;
            this.consumerGroupsToSync = Arrays.stream(consumerGroupsConfig.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet());
            log.info("Offset sync configured for consumer groups: {}", consumerGroupsToSync);
        }
    }

    /**
     * Check if it's time to sync offsets based on the configured interval
     */
    public boolean shouldSync() {
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastSyncTime) >= syncIntervalMs;
    }

    /**
     * Synchronize consumer group offsets from source to target cluster
     */
    public void syncOffsets() {
        try {
            log.debug("Starting consumer group offset synchronization");
            long startTime = System.currentTimeMillis();
            
            Set<String> groupsToSync = getConsumerGroupsToSync();
            
            if (groupsToSync.isEmpty()) {
                log.debug("No consumer groups found to sync");
                return;
            }

            int totalSynced = 0;
            int totalErrors = 0;

            for (String groupId : groupsToSync) {
                try {
                    boolean synced = syncGroupOffsets(groupId);
                    if (synced) {
                        totalSynced++;
                    }
                } catch (Exception e) {
                    totalErrors++;
                    log.warn("Failed to sync offsets for consumer group '{}': {}", groupId, e.getMessage());
                }
            }

            lastSyncTime = System.currentTimeMillis();
            long duration = lastSyncTime - startTime;
            
            log.info("Offset sync completed: {} groups synced, {} errors, duration: {}ms", 
                    totalSynced, totalErrors, duration);

        } catch (Exception e) {
            log.error("Error during offset synchronization: {}", e.getMessage(), e);
        }
    }

    /**
     * Get the list of consumer groups to synchronize
     */
    private Set<String> getConsumerGroupsToSync() throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupListing> allGroups = sourceAdminClient.listConsumerGroups()
                .all()
                .get();

        Set<String> groups = new HashSet<>();
        
        for (ConsumerGroupListing listing : allGroups) {
            String groupId = listing.groupId();
            
            // Filter based on configuration
            if (syncAllGroups || consumerGroupsToSync.contains(groupId)) {
                // Check if this group consumes from our source topic
                if (groupConsumesFromTopic(groupId)) {
                    groups.add(groupId);
                }
            }
        }

        log.debug("Found {} consumer groups to sync", groups.size());
        return groups;
    }

    /**
     * Check if a consumer group consumes from the source topic
     */
    private boolean groupConsumesFromTopic(String groupId) {
        try {
            Map<String, ConsumerGroupDescription> descriptions = sourceAdminClient
                    .describeConsumerGroups(Collections.singleton(groupId))
                    .all()
                    .get();

            ConsumerGroupDescription description = descriptions.get(groupId);
            if (description == null) {
                return false;
            }

            // Get the offsets for this group
            Map<TopicPartition, OffsetAndMetadata> offsets = sourceAdminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();

            // Check if any partition belongs to our source topic
            return offsets.keySet().stream()
                    .anyMatch(tp -> tp.topic().equals(sourceTopic));

        } catch (Exception e) {
            log.debug("Error checking if group '{}' consumes from topic '{}': {}", 
                    groupId, sourceTopic, e.getMessage());
            return false;
        }
    }

    /**
     * Sync offsets for a specific consumer group
     */
    private boolean syncGroupOffsets(String groupId) throws ExecutionException, InterruptedException {
        log.debug("Syncing offsets for consumer group: {}", groupId);

        // Get current offsets from source cluster
        Map<TopicPartition, OffsetAndMetadata> sourceOffsets = sourceAdminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();

        if (sourceOffsets.isEmpty()) {
            log.debug("No offsets found for consumer group '{}'", groupId);
            return false;
        }

        // Filter and translate offsets for our topic
        Map<TopicPartition, OffsetAndMetadata> targetOffsets = new HashMap<>();
        
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : sourceOffsets.entrySet()) {
            TopicPartition sourcePartition = entry.getKey();
            
            if (sourcePartition.topic().equals(sourceTopic)) {
                // Translate to target topic partition
                TopicPartition targetPartition = new TopicPartition(
                        targetTopic,
                        sourcePartition.partition()
                );
                
                targetOffsets.put(targetPartition, entry.getValue());
                
                log.trace("Mapping offset: {} -> {} (offset: {})", 
                        sourcePartition, targetPartition, entry.getValue().offset());
            }
        }

        if (targetOffsets.isEmpty()) {
            log.debug("No offsets to sync for topic '{}' in consumer group '{}'", sourceTopic, groupId);
            return false;
        }

        // Commit offsets to target cluster
        try {
            targetAdminClient.alterConsumerGroupOffsets(groupId, targetOffsets)
                    .all()
                    .get();
            
            log.info("Synced {} partition offsets for consumer group '{}' from topic '{}' to '{}'",
                    targetOffsets.size(), groupId, sourceTopic, targetTopic);
            
            return true;
            
        } catch (Exception e) {
            log.error("Failed to commit offsets to target cluster for group '{}': {}", 
                    groupId, e.getMessage());
            throw e;
        }
    }

    /**
     * Get statistics about the last sync operation
     */
    public Map<String, Object> getSyncStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("lastSyncTime", lastSyncTime);
        stats.put("syncIntervalMs", syncIntervalMs);
        stats.put("timeSinceLastSync", System.currentTimeMillis() - lastSyncTime);
        stats.put("syncAllGroups", syncAllGroups);
        stats.put("configuredGroups", consumerGroupsToSync);
        return stats;
    }

    /**
     * Close admin clients
     */
    public void close() {
        log.info("Closing offset sync admin clients");
        try {
            if (sourceAdminClient != null) {
                sourceAdminClient.close();
            }
        } catch (Exception e) {
            log.warn("Error closing source admin client: {}", e.getMessage());
        }
        
        try {
            if (targetAdminClient != null) {
                targetAdminClient.close();
            }
        } catch (Exception e) {
            log.warn("Error closing target admin client: {}", e.getMessage());
        }
    }
}
