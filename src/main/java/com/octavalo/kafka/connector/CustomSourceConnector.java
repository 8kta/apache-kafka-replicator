package com.octavalo.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(CustomSourceConnector.class);
    private CustomSourceConnectorConfig config;
    private Map<String, String> configProps;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting CustomSourceConnector for Kafka replication");
        try {
            if (props == null || props.isEmpty()) {
                throw new ConnectException("Configuration properties cannot be null or empty");
            }
            
            log.debug("Validating connector configuration");
            configProps = props;
            config = new CustomSourceConnectorConfig(props);
            
            log.info("CustomSourceConnector started successfully");
            log.info("Source cluster: {}", config.getSourceBootstrapServers());
            log.info("Source topic: {}", config.getSourceTopic());
            log.info("Target topic: {}", config.getTargetTopic());
            log.info("Consumer group: {}", config.getSourceGroupId());
            log.info("Security protocol: {}", config.getSourceSecurityProtocol());
            
        } catch (ConfigException e) {
            log.error("Configuration error while starting connector: {}", e.getMessage(), e);
            throw new ConnectException("Failed to start connector due to configuration error", e);
        } catch (Exception e) {
            log.error("Unexpected error while starting connector: {}", e.getMessage(), e);
            throw new ConnectException("Failed to start connector", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configuration(s)", maxTasks);
        try {
            if (maxTasks <= 0) {
                log.warn("Invalid maxTasks value: {}. Setting to 1", maxTasks);
                maxTasks = 1;
            }
            
            if (configProps == null) {
                throw new ConnectException("Connector has not been started. Configuration is null.");
            }
            
            List<Map<String, String>> configs = new ArrayList<>();
            for (int i = 0; i < maxTasks; i++) {
                Map<String, String> taskConfig = new HashMap<>(configProps);
                taskConfig.put("task.id", String.valueOf(i));
                configs.add(taskConfig);
                log.debug("Created task configuration for task {}", i);
            }
            
            log.info("Successfully created {} task configuration(s)", configs.size());
            return configs;
            
        } catch (Exception e) {
            log.error("Error creating task configurations: {}", e.getMessage(), e);
            throw new ConnectException("Failed to create task configurations", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping CustomSourceConnector");
        try {
            if (config != null) {
                log.debug("Cleaning up connector resources");
                config = null;
                configProps = null;
            }
            log.info("CustomSourceConnector stopped successfully");
        } catch (Exception e) {
            log.error("Error while stopping connector: {}", e.getMessage(), e);
        }
    }

    @Override
    public ConfigDef config() {
        return CustomSourceConnectorConfig.config();
    }
}
