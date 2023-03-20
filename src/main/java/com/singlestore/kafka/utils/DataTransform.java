package com.singlestore.kafka.utils;

import com.singlestore.kafka.SingleStoreSinkConnector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DataTransform {

    private static final Logger log = LoggerFactory.getLogger(DataTransform.class);

    HashSet<String> fieldsWhitelist;

    public DataTransform(List<String> fieldsWhiteList) {
        this.fieldsWhitelist = fieldsWhiteList == null ? new HashSet<>() : new HashSet<>(fieldsWhiteList);
    }

    public Collection<SinkRecord> selectWhitelistedFields(Collection<SinkRecord> records) {
        if (this.fieldsWhitelist.size() == 0 || records.size() == 0) {
            return records;
        }

        if (records.iterator().next().valueSchema() == null) {
            return records.stream().map(this::updateRecordWithoutSchema).collect(Collectors.toList());
        }

        Schema updatedSchema = this.updateSchema(records.iterator().next().valueSchema());
        return records.stream().map(record -> this.updateRecord(record, updatedSchema)).collect(Collectors.toList());
    }

    private SinkRecord updateRecordWithoutSchema(SinkRecord record) {
        if (!(record.value() instanceof Map)) {
            log.warn("Record {} doesn't have schema and is not a Map. fields.whitelist configuration is ignored.", record);
            return record;
        }

        Map<Object, Object> value = (Map<Object, Object>) record.value();
        Map<Object, Object> updatedValue = value.entrySet().stream()
            .filter(entry -> fieldsWhitelist.contains(entry.getKey().toString()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, updatedValue, record.timestamp());
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

    private SinkRecord updateRecord(SinkRecord record, Schema updatedSchema) {
        final Struct updatedValue = new Struct(updatedSchema);
        final Struct value = (Struct) record.value();

        for (Field field : updatedSchema.fields()) {
            updatedValue.put(field.name(), value.get(field.name()));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
