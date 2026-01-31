package com.example.kafka.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class CustomSourceConnectorConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Topic to write to";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Interval in milliseconds to poll for new data";
    private static final int POLL_INTERVAL_MS_DEFAULT = 1000;

    public static final String DATA_SOURCE_CONFIG = "data.source";
    private static final String DATA_SOURCE_DOC = "Data source identifier or path";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Maximum number of records to return in a single poll";
    private static final int BATCH_SIZE_DEFAULT = 100;

    public CustomSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(TOPIC_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        TOPIC_DOC)
                .define(POLL_INTERVAL_MS_CONFIG,
                        Type.INT,
                        POLL_INTERVAL_MS_DEFAULT,
                        Importance.MEDIUM,
                        POLL_INTERVAL_MS_DOC)
                .define(DATA_SOURCE_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        DATA_SOURCE_DOC)
                .define(BATCH_SIZE_CONFIG,
                        Type.INT,
                        BATCH_SIZE_DEFAULT,
                        Importance.MEDIUM,
                        BATCH_SIZE_DOC);
    }

    public String getTopic() {
        return getString(TOPIC_CONFIG);
    }

    public int getPollIntervalMs() {
        return getInt(POLL_INTERVAL_MS_CONFIG);
    }

    public String getDataSource() {
        return getString(DATA_SOURCE_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }
}
