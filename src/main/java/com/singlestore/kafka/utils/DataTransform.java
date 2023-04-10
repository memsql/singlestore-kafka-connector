package com.singlestore.kafka.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.*;
import java.util.stream.Collectors;

public class DataTransform {
    HashSet<String> fieldsWhitelist;

    public DataTransform(List<String> fieldsWhiteList) {
        this.fieldsWhitelist = fieldsWhiteList == null ? new HashSet<>() : new HashSet<>(fieldsWhiteList);
    }

    public Collection<SinkRecord> selectWhitelistedFields(Collection<SinkRecord> records) {
        if (this.fieldsWhitelist.size() == 0 || records.size() == 0) {
            return records;
        }

        return records.stream().map(this::updateRecord).collect(Collectors.toList());
    }

    private Schema updateSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (this.fieldsWhitelist.contains(field.name())) {
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
                .filter(entry -> fieldsWhitelist.contains(entry.getKey().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, updatedValue, record.timestamp());
        }
    }
}
