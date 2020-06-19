package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
                JdbcHelper.createTable(connection, table, record.valueSchema(), config.tableKeys);
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

    private static boolean tableExists(Connection connection, String table) {
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(MemSQLDialect.getTableExistsQuery(table));
        } catch (SQLException ex) {
            return false;
        }
    }

    public static boolean metadataRecordExists(Connection connection, String id, MemSQLSinkConfig config) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(String.format("SELECT * FROM `%s` WHERE `id` = '%s'", config.metadataTableName, id));
            return resultSet.next();
        } catch (SQLException ex) {
            return false;
        }
    }

    private static void createTable(Connection connection, String table, Schema schema, List<TableKey> keys) throws SQLException {
        createTable(connection, table, schemaToString(schema, keys));
    }

    private static void createTable(Connection connection, String table, String schema) throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS `%s` %s", table, schema);
        log.trace(String.format("Executing SQL:\n%s", sql));
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public static String getSchemaTables(Schema schema) {
        if (schema.type() == Schema.Type.STRUCT) {
            List<String> fieldSql = schema.fields().stream()
                    .map(Field::name)
                    .collect(Collectors.toList());
            return String.join(", ", fieldSql);
        } else {
            return schema.name() == null ? "data" : schema.name();
        }
    }

    private static String schemaToString(Schema schema, List<TableKey> keys) {
        List<Field> fields;
        if (schema.type() == Schema.Type.STRUCT) {
            fields = schema.fields();
        } else {
            fields = Collections.singletonList(new Field("data", 0, schema));
        }
        List<String> fieldsSql = fields.stream()
                .map(field -> formatSchemaField(field.name(), field.schema()))
                .collect(Collectors.toList());

        boolean containShardKey = false;
        for (TableKey key:keys) {
            if (key.type == TableKey.Type.SHARD) {
                containShardKey = true;
                break;
            }
        }
        if (containShardKey) {
            keys.add(new TableKey(TableKey.Type.COLUMNSTORE, "", fields.get(0).name()));
        }

        List<String> keysSql= keys.stream().map(TableKey::toString)
                .collect(Collectors.toList());

        fieldsSql.addAll(keysSql);
        return "(\n"+ String.join("\n,", fieldsSql) +"\n)";
    }

    private static String formatSchemaField(String fieldName, Schema schema) {
        String name = String.format("`%s`", fieldName);
        String memsqlType = MemSQLDialect.getSqlType(schema);
        String collation = schema.type() == Schema.Type.STRING ? " COLLATE UTF8_BIN" : "";
        String nullable = schema.isOptional() ? "" : " NOT NULL";
        return String.format("%s %s%s%s", name, memsqlType, collation, nullable);
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

    public static ResultSet executeQuery(MemSQLSinkConfig config, String sql) throws SQLException {
        return getDDLConnection(config).createStatement().executeQuery(sql);
    }

    public static int executeUpdate(MemSQLSinkConfig config, String sql) throws SQLException {
        return getDDLConnection(config).createStatement().executeUpdate(sql);
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
