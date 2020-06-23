package com.memsql.kafka;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.VersionProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MemSQLSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(MemSQLSinkConnector.class);

    Map<String, String> configs;

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting MemSQL Sink Connector");
        this.configs = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MemSQLSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        log.info("Setting task configurations for {} workers.", i);
        return Collections.nCopies(i, configs);
    }

    @Override
    public void stop() {
        log.info("Stopping MemSQL Sink Connector");
    }

    @Override
    public ConfigDef config() {
        return MemSQLSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionProvider.getVersion();
    }
}
