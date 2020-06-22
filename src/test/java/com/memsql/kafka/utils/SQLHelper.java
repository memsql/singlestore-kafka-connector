package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLSinkConfig;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.memsql.kafka.utils.JdbcHelper.getDDLConnection;

public class SQLHelper {

    public static ResultSet executeQuery(MemSQLSinkConfig config, String sql) throws SQLException {
        return getDDLConnection(config).createStatement().executeQuery(sql);
    }

    public static int executeUpdate(MemSQLSinkConfig config, String sql) throws SQLException {
        return getDDLConnection(config).createStatement().executeUpdate(sql);
    }
}
