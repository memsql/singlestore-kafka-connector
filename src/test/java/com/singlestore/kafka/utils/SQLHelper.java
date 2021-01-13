package com.singlestore.kafka.utils;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.singlestore.kafka.utils.JdbcHelper.getDDLConnection;

public class SQLHelper {
    protected static final Logger log = LoggerFactory.getLogger(SQLHelper.class);

    public static ResultSet executeQuery(SingleStoreSinkConfig config, String sql) throws SQLException {
        log.trace("Executing SQL:\n{}", sql);
        return getDDLConnection(config).createStatement().executeQuery(sql);
    }

    public static int executeUpdate(SingleStoreSinkConfig config, String sql) throws SQLException {
        log.trace("Executing SQL:\n{}", sql);
        return getDDLConnection(config).createStatement().executeUpdate(sql);
    }
}
