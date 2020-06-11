package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDbWriter;
import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class JdbcHelper {

    private static final Logger log = LoggerFactory.getLogger(JdbcHelper.class);

    public static void createTableIfNeeded(MemSQLSinkConfig config, String table, SinkRecord record) throws SQLException {
        try (Connection connection = getDDLConnection(config)) {
            boolean tableExists = JdbcHelper.tableExists(connection, table);
            if (!tableExists) {
                log.info(String.format("Table `%s` doesn't exist. Creating it", table));
                JdbcHelper.createTable(connection, table, record.valueSchema());
            }
        }
    }

    public static boolean tableExists(Connection connection, String table) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(MemSQLDialect.getTableExistsQuery(table));
        }
    }

    public static void createTable(Connection connection, String table, Schema schema) throws SQLException {
        String sql = String.format("CREATE TABLE `%s` %s}", table, schemaToString(schema));
        log.trace(String.format("Executing SQL:\n%s", sql));
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private static String schemaToString(Schema schema) {
        if (schema.type() == Schema.Type.STRUCT) {
            List<String> fieldSql = schema.fields().stream()
                    .map(field -> {
                        String name = String.format("`%s`", field.name());
                        String memsqlType = MemSQLDialect.getSqlType(field);
                        String collation = field.schema().type() == Schema.Type.STRING ? " COLLATE UTF8_BIN" : "";
                        String nullable = field.schema().isOptional() ? "" : " NOT NULL";
                        return String.format("%s %s%s%s", name, memsqlType, collation, nullable);
                    }).collect(Collectors.toList());
            return String.format("(\n  %s\n)", String.join(",\n  ", fieldSql));
            //TODO add tableKeys (different Keys)
        } else {
            // TODO handle case if schema is not STRUCT
            return null;
        }
    }

    public static boolean isReferenceTable(MemSQLSinkConfig config, String table) {
        String database = config.database;
        String sql = String.format("using %s show tables extended like `%s`", database, table);
        log.trace(String.format("Executing SQL:\n%s", sql));
        try (Connection connection = getDDLConnection(config);
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

    public static Connection getDDLConnection(MemSQLSinkConfig config) throws SQLException {
        return getConnection(Collections.singletonList(config.ddlEndpoint), config);
    }

    public static Connection getDMLConnection(MemSQLSinkConfig config) throws SQLException {
        return getConnection(config.dmlEndpoints, config);
    }

    private static Connection getConnection(List<String> hosts, MemSQLSinkConfig config) throws SQLException {
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
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(
                    getJDBCUrl(hosts, config.database),
                    connectionProps);
        } catch (ClassNotFoundException ex) {
            throw new SQLException("No sql driver found.");
        }
    }

    private static String getJDBCUrl(List<String> hosts, String database) {
        return "jdbc:mysql://" +
                String.join(",", hosts) +
                "/" +
                database;
    }
}
