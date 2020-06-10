package com.memsql.kafka.sink;

import com.memsql.kafka.utils.JdbcHelper;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.util.Collection;

public class MemSQLDbWriter {

    private MemSQLSinkConfig config;

    public MemSQLDbWriter(MemSQLSinkConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> records) {
        Connection connection = JdbcHelper.getConnection(config.dmlEndpoints, config);
    }

}
