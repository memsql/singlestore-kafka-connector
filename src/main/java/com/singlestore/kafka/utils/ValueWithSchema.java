package com.singlestore.kafka.utils;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

// ValueWithSchema represents value and the schema of the Kafka record
// This class encapsulates the processing of Kafka records that have schema and records that don't
public class ValueWithSchema {
    Object value;
    Schema schema;

    Boolean isStruct;

    private static final ValueWithSchema NULL_VALUE = new ValueWithSchema(null, null);

    static String DEFAULT_COLUMN_NAME = "data";

    public ValueWithSchema(SinkRecord record) {
        value = record.value();
        schema = record.valueSchema();
    }

    public ValueWithSchema(Schema schema) {
        this.value = null;
        this.schema = schema;
    }

    private ValueWithSchema(Object value, Schema schema) {
        this.value = value;
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }
    public Object getValue() {
        return value;
    }

    private boolean isStruct() {
        if (isStruct == null) {
            if (schema != null) {
                isStruct = schema.type() == Schema.Type.STRUCT;
            } else {
                isStruct = value instanceof Map;
            }
        }

        return isStruct;
    }

    private static String getDefaultColumnName(Schema schema) {
        return (schema == null || schema.name() == null) ? DEFAULT_COLUMN_NAME : schema.name();
    }

    private ValueWithSchema getByKey(String key) {
        if (!isStruct()) {
            return NULL_VALUE;
        }

        if (schema != null) {
            try {
                Object fieldValue = value == null ? null : ((Struct) value).get(key);
                Schema fieldSchema = schema.field(key).schema();
                return new ValueWithSchema(fieldValue, fieldSchema);
            } catch (DataException ex) {
                return new ValueWithSchema(null, null);
            }
        } else {
            if (value == null) {
                return NULL_VALUE;
            }
            Map valueMap = (Map) value;
            return new ValueWithSchema(valueMap.get(key), null);
        }
    }

    public ValueWithSchema getByPath(String path) {
        ValueWithSchema res = this;
        for (String key: path.split("\\.")) {
            res = res.getByKey(key);
        }

        return res;
    }

    public String mapColumnsToCSV(List<ColumnMapping> columnMappings) throws IOException {
        ArrayList<String> fields = new ArrayList<>();
        for (ColumnMapping mapping: columnMappings) {
            fields.add(this.getByPath(mapping.getFieldPath()).toCSV());
        }

        return String.join("\t", fields);
    }

    public String toCSV(List<String> columns) throws IOException {
        ArrayList<String> fields = new ArrayList<>();

        if (schema != null && schema.type() != Schema.Type.STRUCT) {
            fields.add(this.toCSV());
        } else if (schema == null && !(value instanceof Map)) {
            fields.add(this.toCSV());
        } else {
            for (String column: columns) {
                fields.add(this.getByKey(column).toCSV());
            }
        }

        return String.join("\t", fields);
    }

    private String toCSV() throws IOException {
        if (value == null) {
            return "\\N";
        } else if (schema == null) {
            if (value instanceof Boolean) {
                return (Boolean) value ? "1" : "0";
            } else if (value instanceof Map || value instanceof List) {
                ObjectWriter ow = new ObjectMapper().writer();
                String json = ow.writeValueAsString(value);
                return escapeCSV(json);
            } else if (value instanceof byte[]) {
                return escapeCSV(new String((byte [])value, StandardCharsets.UTF_8));
            } else {
                return escapeCSV(value.toString());
            }
        } else {
            if(schema.type().isPrimitive()) {
                if (value instanceof Boolean) {
                    return (Boolean) value ? "1" : "0";
                } else if (value instanceof byte[]) {
                    return escapeCSV(new String((byte [])value, StandardCharsets.UTF_8));
                } else {
                    return escapeCSV(value.toString());
                }
            } else {
                return escapeCSV(toJSON(schema, value));
            }
        }
    }

    private static String escapeCSV(String value) {
        if (value.indexOf('\\') != -1) {
            value = value.replace("\\", "\\\\");
        }
        if (value.indexOf('\n') != -1) {
            value = value.replace("\n", "\\\n");
        }
        if (value.indexOf('\t') != -1) {
            value = value.replace("\t", "\\\t");
        }
        return value;
    }

    private static String toJSON(Schema schema, Object value) throws IOException {
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
        try {
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
                    jGenerator.writeBinary((byte[]) value);
                    break;
                case STRING:
                    jGenerator.writeString((String) value);
                    break;
                case ARRAY:
                    jGenerator.writeStartArray();
                    for(Object element: (List<?>) value) {
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
                    for (Field field: schema.fields()) {
                        jGenerator.writeFieldName(field.name());
                        generateJSON(jGenerator, field.schema(), ((Struct)value).get(field.name()));
                    }
                    jGenerator.writeEndObject();
            }
        } catch (ClassCastException ex) {
            throw new ConnectException(String.format("The object '%s' has an incorrect schema (%s)", value, schema.type()), ex);
        }
    }

    public List<String> getColumns() {
        if (!isStruct()) {
            return Collections.singletonList(getDefaultColumnName(schema));
        }

        if (schema != null) {
            return schema.fields().stream()
                .map(Field::name).collect(Collectors.toList());
        } else {
            Map<Object, Object> valueMap = (Map<Object, Object>) value;
            return valueMap.keySet().stream()
                .map(Objects::toString).collect(Collectors.toList());
        }
    }
}
