package com.memsql.kafka.sink;

import com.memsql.kafka.utils.JdbcHelper;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MemSQLDbWriter {

    private static final Logger log = LoggerFactory.getLogger(MemSQLDbWriter.class);
    private final MemSQLSinkConfig config;

    public MemSQLDbWriter(MemSQLSinkConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> records) throws SQLException {
        // TODO think about caching connection instead of opening it each time
        try (Connection connection = JdbcHelper.getConnection(config.dmlEndpoints, config)) {
            SinkRecord first = records.iterator().next();
            String table = first.topic();
            boolean tableExists = JdbcHelper.tableExists(connection, table);
            if (!tableExists) {
                log.info(String.format("Table `%s` doesn't exist. Creating it", table));
                JdbcHelper.createTable(connection, table, first.valueSchema());
            }
            /*
            List<Object> values = records.stream()
                    .map(ConnectRecord::value)
                    .collect(Collectors.toList());
            */
            // TODO Do converting and writing
        }
    }

}
