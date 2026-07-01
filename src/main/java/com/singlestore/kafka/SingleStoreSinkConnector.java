package com.singlestore.kafka;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.VersionProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleStoreSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(SingleStoreSinkConnector.class);
    public static final String TASK_ID_CONFIG = "task.id";
    public static final String CONNECTOR_NAME_CONFIG = "name";

    Map<String, String> configs;

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting SingleStore Sink Connector");
        this.configs = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleStoreSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        log.info("Setting task configurations for {} workers.", i);
        if (i <= 0) {
            return Collections.emptyList();
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>(i);
        for (int taskId = 0; taskId < i; taskId++) {
            Map<String, String> taskProps = new HashMap<>(configs);
            taskProps.put(TASK_ID_CONFIG, String.valueOf(taskId));
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping SingleStore Sink Connector");
    }

    @Override
    public ConfigDef config() {
        return SingleStoreSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionProvider.getVersion();
    }
}
