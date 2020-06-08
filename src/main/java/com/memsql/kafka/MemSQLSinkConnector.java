package com.memsql.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
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
        List<Map<String, String>> conf = new ArrayList<>();
        conf.add(configs);
        return conf;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return MemSQLSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "0.0.1-beta";
    }
}
