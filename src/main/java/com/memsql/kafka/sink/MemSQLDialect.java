package com.memsql.kafka.sink;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MemSQLDialect {

    private static final Logger log = LoggerFactory.getLogger(MemSQLDialect.class);

    public static String getKafkaMetadataSchema() {
        return "(\n  id VARCHAR(255) PRIMARY KEY COLLATE UTF8_BIN,\n  count INT NOT NULL\n)";
    }

    public static String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    public static String getTableExistsQuery(String table) {
        return String.format("SELECT * FROM `%s` WHERE 1=0", table);
    }

    public static String getRecordValueCSV(SinkRecord record) throws IOException {
        if (record.valueSchema().type() != Schema.Type.STRUCT) {
            if (record.valueSchema().type().isPrimitive()) {
                return record.value().toString();
            }
            return toJSON(record.valueSchema(), record.value());
        }

        List<String> fields = new ArrayList<>();
        Struct struct = (Struct) record.value();
        Schema structSchema = struct.schema();
        for (Field field:structSchema.fields()) {
            if (field.schema().type().isPrimitive()) {
                fields.add(struct.get(field.name()).toString());
            } else {
                fields.add(toJSON(field.schema(), struct.get(field.name())));
            }
        }

        return String.join("\t", fields);
    }

    public static List<Object> getRecordValues(SinkRecord record) throws IOException {
        if (record.valueSchema().type() != Schema.Type.STRUCT) {
            if (record.valueSchema().type().isPrimitive()) {
                return Collections.singletonList(record.value());
            }
            return Collections.singletonList(toJSON(record.valueSchema(), record.value()));
        }

        List<Object> fields = new ArrayList<>();
        Struct struct = (Struct) record.value();
        Schema structSchema = struct.schema();
        for (Field field:structSchema.fields()) {
            if (field.schema().type().isPrimitive()) {
                fields.add(struct.get(field.name()));
            } else {
                fields.add(toJSON(field.schema(), struct.get(field.name())));
            }
        }

        return fields;
    }

    public static String toJSON(Schema schema, Object value) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonFactory jfactory = new JsonFactory();
        JsonGenerator jGenerator = jfactory
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

    public static String getSqlType(Schema fieldSchema) {
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
