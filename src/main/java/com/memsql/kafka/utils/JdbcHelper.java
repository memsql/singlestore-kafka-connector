package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDbWriter;
import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class JdbcHelper {

    private static final Logger log = LoggerFactory.getLogger(JdbcHelper.class);

    public static boolean tableExists(Connection connection, String table) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(MemSQLDialect.getTableExistsQuery(table));
        }
    }

    public static void createTable(Connection connection, String table, Schema schema) {
        // TODO Create table
    }

    public static boolean isReferenceTable(MemSQLSinkConfig config, String table) {
        String database = config.database;
        String sql = String.format("using %s show tables extended like `%s`", database, table);
        log.trace(String.format("Executing SQL:\n%s", sql));
        try (Connection connection = getConnection(Collections.singletonList(config.ddlEndpoint), config);
             Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(sql);
            if (resultSet.next()) {
                return !resultSet.getBoolean("distributed");
            } else {
                throw new IllegalArgumentException(String.format("Table `%s.%s` doesn't exist", database, table));
            }
        } catch (SQLException ex) {
            return false;
        }
    }

    public static Connection getConnection(List<String> hosts, MemSQLSinkConfig config) throws SQLException {
        Properties connectionProps = new Properties();
        String username = config.user;
        String password = config.password;
        if (username != null) {
            connectionProps.setProperty("user", username);
        }
        if (password != null) {
            connectionProps.setProperty("password", password);
        }
        connectionProps.putAll(config.sqlParams);
        return DriverManager.getConnection(
                getJDBCUrl(hosts, config.database),
                connectionProps);
    }

    private static String getJDBCUrl(List<String> hosts, String database) {
        return "jdbc:mysql://" +
                String.join(",", hosts) +
                "/" +
                database;
    }
}
