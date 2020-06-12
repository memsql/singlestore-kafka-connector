package com.memsql.kafka;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MemSQLSinkConnector extends SinkConnector {

    Map<String, String> configs;

    @Override
    public void start(Map<String, String> map) {
        this.configs = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MemSQLSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return Collections.nCopies(i, configs);
    }

    @Override
    public void stop() { }

    @Override
    public ConfigDef config() {
        return MemSQLSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "0.0.1-beta";
    }
}
