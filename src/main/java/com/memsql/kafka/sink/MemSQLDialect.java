package com.memsql.kafka.sink;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemSQLDialect {

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

    public static String getDefaultColumnName(Schema schema) {
        return schema.name() == null ? "data" : schema.name();
    }

    public static String getCreateTableQuery(String table, String schema) {
        return String.format("CREATE TABLE IF NOT EXISTS %s %s", quoteIdentifier(table), schema);
    }

    public static String getColumnNames(Schema schema) {
        if (schema.type() == Schema.Type.STRUCT) {
            return  schema.fields().stream()
                    .map(field -> quoteIdentifier(field.name()))
                    .collect(Collectors.joining(", "));
        } else {
            return quoteIdentifier(getDefaultColumnName(schema));
        }
    }

    public static String getSchemaForCrateTableQuery(Schema schema, List<TableKey> keys) {
        List<Field> fields;
        if (schema.type() == Schema.Type.STRUCT) {
            fields = schema.fields();
        } else {
            fields = Collections.singletonList(new Field("data", 0, schema));
        }
        List<String> fieldsSql = fields.stream()
                .map(field -> formatSchemaField(field.name(), field.schema()))
                .collect(Collectors.toList());

        boolean allKeysAreShard = true;
        for (TableKey key:keys) {
            if (key.type != TableKey.Type.SHARD) {
                allKeysAreShard = false;
                break;
            }
        }

        // if all the keys are shard keys it means there are no other keys so we can default to columnstore
        // in 6.8 and below you *must* specify a sort key for this
        // so we just pick the first primitive column arbitrarily for now
        if (allKeysAreShard) {
            for (Field field:fields) {
                if (field.schema().type().isPrimitive()) {
                    keys.add(new TableKey(TableKey.Type.COLUMNSTORE, "", MemSQLDialect.quoteIdentifier(field.name())));
                    break;
                }
            }
        }

        List<String> keysSql= keys.stream().map(TableKey::toString)
                .collect(Collectors.toList());

        fieldsSql.addAll(keysSql);
        return "(\n"+ String.join(",\n", fieldsSql) +"\n)";
    }

    private static String formatSchemaField(String fieldName, Schema schema) {
        String name = quoteIdentifier(fieldName);
        String memsqlType = getSqlType(schema);
        String nullable = schema.isOptional() ? "" : " NOT NULL";
        return String.format("%s %s%s", name, memsqlType, nullable);
    }

    private static String escapeCSV(String value) {
        if (value.indexOf('\\') != -1) {
            value = value.replace("\\", "\\\\");
        }
        if (value.indexOf('\n') != -1) {
            value = value.replace("\n", "\\n");
        }
        if (value.indexOf('\t') != -1) {
            value = value.replace("\t", "\\t");
        }
        return value;
    }

    private static String escapeCSV(Schema schema, Object value) throws IOException {
        if(schema.type().isPrimitive()) {
            if (value == null) {
                return "\\N";
            } else if (value instanceof Boolean) {
                return (Boolean) value ? "1" : "0";
            } else {
                return escapeCSV(value.toString());
            }
        } else {
            return escapeCSV(toJSON(schema, value));
        }
    }

    public static String getRecordValueCSV(SinkRecord record) throws IOException {
        if (record.valueSchema().type() != Schema.Type.STRUCT) {
            return escapeCSV(record.valueSchema(), record.value());
        }

        List<String> fields = new ArrayList<>();
        Struct struct = (Struct) record.value();
        Schema structSchema = struct.schema();
        for (Field field:structSchema.fields()) {
            fields.add(escapeCSV(field.schema(), struct.get(field.name())));
        }

        return String.join("\t", fields);
    }

    private static Object toAvroSupportedObject(Schema schema, Object value) throws IOException {
        if (schema.type().isPrimitive()) {
            if (value == null) {
                return null;
            } else switch(schema.type()) {
                case INT8:
                    return ((Byte)value).intValue();
                case INT16:
                    return ((Short)value).intValue();
                case BYTES:
                    return ByteBuffer.wrap((byte[])value);
                default:
                    return value;
            }
        }
        return toJSON(schema, value);
    }

    public static List<Object> getRecordValues(SinkRecord record) throws IOException {
        if (record.valueSchema().type() != Schema.Type.STRUCT) {
            return Collections.singletonList(toAvroSupportedObject(record.valueSchema(), record.value()));
        }

        List<Object> fields = new ArrayList<>();
        Struct struct = (Struct) record.value();
        Schema structSchema = struct.schema();
        for (Field field:structSchema.fields()) {
            fields.add(toAvroSupportedObject(field.schema(), struct.get(field.name())));
        }

        return fields;
    }

    public static String toJSON(Schema schema, Object value) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator jGenerator = new JsonFactory()
                .createGenerator(stream, JsonEncoding.UTF8);

        generateJSON(jGenerator, schema, value);
        jGenerator.close();
        return new String(stream.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void generateJSON(JsonGenerator jGenerator, Schema schema, Object value) throws IOException {
        if (value == null) {
            jGenerator.writeNull();
            return;
        }

        switch(schema.type()) {
            case INT8:
                jGenerator.writeNumber((byte) value);
                break;
            case INT16:
                jGenerator.writeNumber((short) value);
                break;
            case INT32:
                jGenerator.writeNumber((int) value);
                break;
            case INT64:
                jGenerator.writeNumber((long) value);
                break;
            case FLOAT32:
                jGenerator.writeNumber((float) value);
                break;
            case FLOAT64:
                jGenerator.writeNumber((double) value);
                break;
            case BOOLEAN:
                jGenerator.writeBoolean((boolean) value);
                break;
            case BYTES:
                jGenerator.writeBinary((byte[])value);
                break;
            case STRING:
                jGenerator.writeString((String)value);
                break;
            case ARRAY:
                jGenerator.writeStartArray();
                for(Object element:(List<?>)value) {
                    generateJSON(jGenerator, schema.valueSchema(), element);
                }
                jGenerator.writeEndArray();
                break;
            case MAP:
                jGenerator.writeStartArray();
                for (Map.Entry<?, ?> entry : ((Map<?,?>)value).entrySet()) {
                    jGenerator.writeStartObject();

                    jGenerator.writeFieldName("key");
                    generateJSON(jGenerator, schema.keySchema(), entry.getKey());

                    jGenerator.writeFieldName("value");
                    generateJSON(jGenerator, schema.valueSchema(), entry.getValue());

                    jGenerator.writeEndObject();
                }
                jGenerator.writeEndArray();
                break;
            case STRUCT:
                jGenerator.writeStartObject();
                for (Field field:schema.fields()) {
                    jGenerator.writeFieldName(field.name());
                    generateJSON(jGenerator, field.schema(), ((Struct)value).get(field.name()));
                }
                jGenerator.writeEndObject();
        }
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
                throw new ConnectException(String.format("%s (%s) type doesn't have a mapping to the MemSQL database column type", fieldSchema.name(), fieldSchema.type()));
        }
    }
}
