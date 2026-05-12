package com.singlestore.kafka.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;
import java.util.stream.Collectors;

public class DataTransform {
    private final HashSet<String> fieldsWhitelist;
    private final HashSet<String> fieldsBlacklist;

    public DataTransform(List<String> fieldsWhitelist, List<String> fieldsBlacklist) {
        this.fieldsWhitelist = fieldsWhitelist == null ? new HashSet<>() : new HashSet<>(fieldsWhitelist);
        this.fieldsBlacklist = fieldsBlacklist == null ? new HashSet<>() : new HashSet<>(fieldsBlacklist);
    }

    /**
     * Applies {@code fields.whitelist} (if non-empty) and {@code fields.blacklist} to each record value.
     */
    public Collection<SinkRecord> selectFields(Collection<SinkRecord> records) {
        if (records.size() == 0 || (fieldsWhitelist.size() == 0 && fieldsBlacklist.size() == 0)) {
            return records;
        }

        return records.stream().map(this::updateRecord).collect(Collectors.toList());
    }

    private boolean includeField(String name) {
        if (!fieldsWhitelist.isEmpty() && !fieldsWhitelist.contains(name)) {
            return false;
        }
        return !fieldsBlacklist.contains(name);
    }

    private static SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder builder) {
        builder.name(source.name());
        builder.version(source.version());
        builder.doc(source.doc());
        Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        return builder;
    }

    private Schema updateSchema(Schema schema) {
        final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (includeField(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    private SinkRecord updateRecord(SinkRecord record) {
        Schema schema = record.valueSchema();
        if (schema != null) {
            if (schema.type() != Schema.Type.STRUCT) {
                return record;
            }

            Schema updatedSchema = updateSchema(schema);
            final Struct updatedValue = new Struct(updatedSchema);
            Struct value = (Struct) record.value();
            for (Field field : updatedSchema.fields()) {
                updatedValue.put(field.name(), value.get(field.name()));
            }

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        } else {
            if (!(record.value() instanceof Map)) {
                return record;
            }

            Map<Object, Object> value = (Map<Object, Object>) record.value();
            Map<Object, Object> updatedValue = value.entrySet().stream()
                .filter(entry -> includeField(entry.getKey().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, updatedValue, record.timestamp());
        }
    }
}
