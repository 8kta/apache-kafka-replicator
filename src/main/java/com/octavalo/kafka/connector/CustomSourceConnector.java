package com.octavalo.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
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
        configProps = props;
        config = new CustomSourceConnectorConfig(props);
        log.info("CustomSourceConnector started - replicating from source topic '{}' to target topic '{}'", 
                config.getSourceTopic(), config.getTargetTopic());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping CustomSourceConnector");
    }

    @Override
    public ConfigDef config() {
        return CustomSourceConnectorConfig.config();
    }
}
