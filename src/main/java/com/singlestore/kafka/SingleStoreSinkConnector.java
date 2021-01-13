package com.singlestore.kafka;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.VersionProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SingleStoreSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(SingleStoreSinkConnector.class);

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
        return Collections.nCopies(i, configs);
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
