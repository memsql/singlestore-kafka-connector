package com.singlestore.kafka.utils;

import com.singlestore.kafka.sink.SingleStoreDialect;
import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class JdbcHelper {

    private static final Logger log = LoggerFactory.getLogger(JdbcHelper.class);

    public static void createTableIfNeeded(SingleStoreSinkConfig config, String table, Schema schema) throws SQLException {
        try (Connection connection = getDDLConnection(config)) {
            boolean tableExists = JdbcHelper.tableExists(connection, table);
            if (!tableExists) {
                if (schema == null) {
                    log.error("Table {} doesn't exist and schema is not provided. Table creation is not supported without schema.", table);
                    throw new ConnectException(String.format("Table %s doesn't exist and schema is not provided. Table creation is not supported without schema.", table));
                }
                log.info(String.format("Table `%s` doesn't exist. Creating it", table));
                JdbcHelper.createTable(connection, table, schema, config.tableKeys, config.tableToColumnToFieldMap.get(table));
            }
            if (config.metadataTableAllow) {
                boolean metadataTableExists = JdbcHelper.tableExists(connection, config.metadataTableName);
                if (!metadataTableExists) {
                    log.info(String.format("Metadata table `%s` doesn't exist. Creating it", config.metadataTableName));
                    JdbcHelper.createTable(connection, config.metadataTableName, SingleStoreDialect.getKafkaMetadataSchema());
                }
            }
        }
    }

    public static boolean tableExists(Connection connection, String table) {
        String query = SingleStoreDialect.getTableExistsQuery(table);
        log.trace("Executing SQL:\n{}", query);
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(query);
        } catch (SQLException ex) {
            return false;
        }
    }

    public static boolean metadataRecordExists(Connection connection, String id, SingleStoreSinkConfig config) {
        try (PreparedStatement stmt = SingleStoreDialect.getMetadataRecordExistsQuery(connection, config.metadataTableName, id)) {
            log.trace("Executing SQL:\n{}", stmt);
            ResultSet resultSet = stmt.executeQuery();
            return resultSet.next();
        } catch (SQLException ex) {
            return false;
        }
    }

    public static String getTableName(SinkRecord record, SingleStoreSinkConfig config) {
        if (config.recordToTableMappingField != null) {
            Object value = new ValueWithSchema(record).getByPath(config.recordToTableMappingField).getValue();
            if (value == null) {
                return null;
            }
            return config.recordToTableMap.get(value.toString());
        }
        String topic = record.topic();
        return config.topicToTableMap.getOrDefault(topic, topic);
    }

    private static void createTable(Connection connection, String table, Schema schema, List<TableKey> keys, List<ColumnMapping> columnMappings) throws SQLException {
        createTable(connection, table, SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, columnMappings));
    }

    private static void createTable(Connection connection, String table, String schema) throws SQLException {
        String sql = SingleStoreDialect.getCreateTableQuery(table, schema);
        log.trace("Executing SQL:\n{}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public static boolean isReferenceTable(SingleStoreSinkConfig config, String table) {
        String database = config.database;
        try (Connection connection = getDDLConnection(config);
             PreparedStatement stmt = SingleStoreDialect.showExtendedTables(connection, database, table)) {
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

    public static Connection getDDLConnection(SingleStoreSinkConfig config) throws SQLException {
        return getConnection(Collections.singletonList(config.ddlEndpoint), config);
    }

    public static Connection getDMLConnection(SingleStoreSinkConfig config) throws SQLException {
        return getConnection(config.dmlEndpoints, config);
    }

    private static Connection getConnection(List<String> hosts, SingleStoreSinkConfig config) throws SQLException {
        Properties connectionProps = new Properties();
        String username = config.user;
        String password = config.password;
        if (username != null) {
            connectionProps.setProperty("user", username);
        }
        if (password != null) {
            connectionProps.setProperty("password", password);
        }
        connectionProps.put("connectionAttributes", 
        String.format("_connector_name:%s,_connector_version:%s,_product_version:%s", 
        "SingleStore Kafka Connector",
        VersionProvider.getVersion(),
        "3.2.0"));
        connectionProps.putAll(config.sqlParams);
        try {
            Class.forName("com.singlestore.jdbc.Driver");
            return DriverManager.getConnection(
                    getJDBCUrl(hosts, config.database),
                    connectionProps);
        } catch (ClassNotFoundException ex) {
            throw new SQLException("No sql driver found.");
        }
    }

    private static String getJDBCUrl(List<String> hosts, String database) {
        return "jdbc:singlestore://" +
                String.join(",", hosts) +
                "/" +
                database;
    }
}
