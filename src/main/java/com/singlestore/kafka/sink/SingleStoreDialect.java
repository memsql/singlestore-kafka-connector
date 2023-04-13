package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.TableKey;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SingleStoreDialect {

    public static String quoteIdentifier(String colName) {
        return "`" + colName.replace("`", "``") + "`";
    }

    public static PreparedStatement getInsertIntoMetadataQuery(Connection conn, String metadataTableName, String metaId, Integer recordsCount) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                String.format("INSERT INTO %s (id, count) VALUES (?, ?)", quoteIdentifier(metadataTableName))
        );
        stmt.setString(1, metaId);
        stmt.setInt(2, recordsCount);
        return stmt;
    }

    public static String getKafkaMetadataSchema() {
        return "(\n  id VARCHAR(255) PRIMARY KEY,\n  count INT NOT NULL,\n  createdAt TIMESTAMP DEFAULT NOW()\n)";
    }

    public static PreparedStatement showExtendedTables(Connection conn, String database, String table) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                String.format("USING %s SHOW TABLES EXTENDED LIKE ?", quoteIdentifier(database))
        );
        stmt.setString(1, table.replace("\\", "\\\\"));
        return stmt;
    }

    public static String getTableExistsQuery(String table) {
        return String.format("SELECT * FROM %s WHERE 1=0", quoteIdentifier(table));
    }

    public static PreparedStatement getMetadataRecordExistsQuery(Connection conn, String metadataTableName, String id) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                String.format("SELECT * FROM %s WHERE `id` = ?", quoteIdentifier(metadataTableName))
        );
        stmt.setString(1, id);
        return stmt;
    }

    public static String getCreateTableQuery(String table, String schema) {
        return String.format("CREATE TABLE IF NOT EXISTS %s %s", quoteIdentifier(table), schema);
    }


    public static String escapeColumnNames(List<String> columns) {
        return columns.stream().map(SingleStoreDialect::quoteIdentifier)
            .collect(Collectors.joining(", "));
    }

    public static String getSchemaForCreateTableQuery(Schema schema, List<TableKey> keys, List<ColumnMapping> columnMappings) throws SQLException {
        List<Field> fields;
        if (columnMappings != null) {
            fields = new ArrayList<>();
            for (ColumnMapping mapping: columnMappings) {
                Schema columnSchema = new ValueWithSchema(schema).getByPath(mapping.getFieldPath()).getSchema();
                if (columnSchema == null) {
                    throw new SQLException(String.format("Failed to create the table. Cannot get the column type %s (%s)", mapping.getColumnName(), mapping.getFieldPath()));
                }
                fields.add(new Field(mapping.getColumnName(), 0, columnSchema));
            }
        } else if (schema.type() == Schema.Type.STRUCT) {
            fields = schema.fields();
        } else {
            fields = Collections.singletonList(new Field("data", 0, schema));
        }
        List<String> fieldsSql = fields.stream()
                .map(field -> formatSchemaField(field.name(), field.schema()))
                .collect(Collectors.toList());

        boolean allKeysAreShard = true;
        for (TableKey key: keys) {
            if (key.getType() != TableKey.Type.SHARD) {
                allKeysAreShard = false;
                break;
            }
        }

        // if all the keys are shard keys it means there are no other keys so we can default to columnstore
        // in 6.8 and below you *must* specify a sort key for this
        // so we just pick the first primitive column arbitrarily for now
        if (allKeysAreShard) {
            for (Field field: fields) {
                if (field.schema().type().isPrimitive()) {
                    keys.add(new TableKey(TableKey.Type.COLUMNSTORE, "", Collections.singletonList(field.name())));
                    break;
                }
            }
        }
        List<String> keysSql = keys.stream().map(TableKey::toString)
                .collect(Collectors.toList());
        fieldsSql.addAll(keysSql);
        return "(\n"+ String.join(",\n", fieldsSql) +"\n)";
    }

    private static String formatSchemaField(String fieldName, Schema schema) {
        String name = quoteIdentifier(fieldName);
        String singlestoreType = getSqlType(schema);
        String nullable = schema.isOptional() ? "" : " NOT NULL";
        return String.format("%s %s%s", name, singlestoreType, nullable);
    }

    private static String getSqlType(Schema fieldSchema) {
        switch (fieldSchema.type()) {
            case INT8:
            case BOOLEAN:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case STRING:
                return "TEXT";
            case BYTES:
                return "VARBINARY(1024)";
            case ARRAY:
            case MAP:
            case STRUCT:
                return "JSON";
            default:
                throw new ConnectException(String.format("%s (%s) type doesn't have a mapping to the SingleStore database column type", fieldSchema.name(), fieldSchema.type()));
        }
    }
}
