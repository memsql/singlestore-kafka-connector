package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class JdbcHelper {

    private static final Logger log = LoggerFactory.getLogger(JdbcHelper.class);

    public static void createTableIfNeeded(MemSQLSinkConfig config, String table, Schema schema) throws SQLException {
        try (Connection connection = getDDLConnection(config)) {
            boolean tableExists = JdbcHelper.tableExists(connection, table);
            if (!tableExists) {
                log.info(String.format("Table `%s` doesn't exist. Creating it", table));
                JdbcHelper.createTable(connection, table, schema, config.tableKeys);
            }
            if (config.metadataTableAllow) {
                boolean metadataTableExists = JdbcHelper.tableExists(connection, config.metadataTableName);
                if (!metadataTableExists) {
                    log.info(String.format("Metadata table `%s` doesn't exist. Creating it", config.metadataTableName));
                    JdbcHelper.createTable(connection, config.metadataTableName, MemSQLDialect.getKafkaMetadataSchema());
                }
            }
        }
    }

    public static boolean tableExists(Connection connection, String table) {
        String query = MemSQLDialect.getTableExistsQuery(table);
        log.trace("Executing SQL:\n{}", query);
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(query);
        } catch (SQLException ex) {
            return false;
        }
    }

    public static boolean metadataRecordExists(Connection connection, String id, MemSQLSinkConfig config) {
        try (PreparedStatement stmt = MemSQLDialect.getMetadataRecordExistsQuery(connection, config.metadataTableName, id)) {
            log.trace("Executing SQL:\n{}", stmt);
            ResultSet resultSet = stmt.executeQuery();
            return resultSet.next();
        } catch (SQLException ex) {
            return false;
        }
    }

    private static void createTable(Connection connection, String table, Schema schema, List<TableKey> keys) throws SQLException {
        createTable(connection, table, MemSQLDialect.getSchemaForCrateTableQuery(schema, keys));
    }

    private static void createTable(Connection connection, String table, String schema) throws SQLException {
        String sql = MemSQLDialect.getCreateTableQuery(table, schema);
        log.trace("Executing SQL:\n{}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public static boolean isReferenceTable(MemSQLSinkConfig config, String table) {
        String database = config.database;
        try (Connection connection = getDDLConnection(config);
             PreparedStatement stmt = MemSQLDialect.showExtendedTables(connection, database, table)) {
            log.trace("Executing SQL:\n{}", stmt);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                return !resultSet.getBoolean("distributed");
            } else {
                throw new ConnectException(String.format("Table `%s.%s` doesn't exist", database, table));
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
        connectionProps.put("allowLoadLocalInfile", "true");
        connectionProps.putAll(config.sqlParams);
        try {
            Class.forName("org.mariadb.jdbc.Driver");
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
