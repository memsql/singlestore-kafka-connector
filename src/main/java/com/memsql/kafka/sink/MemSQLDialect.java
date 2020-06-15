package com.memsql.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.stream.Collectors;

public class MemSQLDialect {

    public static final String KAFKA_METADATA_TABLE = "kafka-connect-transaction-metadata";

    public static String getKafkaMetadataSchema() {
        return "(\n  id VARCHAR(255) PRIMARY KEY COLLATE UTF8_BIN,\n  count INT NOT NULL\n)";
    }

    public static String getTableExistsQuery(String table) {
        return String.format("SELECT * FROM `%s` WHERE 1=0", table);
    }

    public static String getRecordValue(SinkRecord record) {
        switch (record.valueSchema().type()) {
            case STRUCT:
                Struct struct = (Struct)record.value();
                Schema structSchema = struct.schema();
                List<String> values = structSchema.fields().stream()
                        .map(field -> struct.get(field.name()).toString())
                        .collect(Collectors.toList());
                return String.join("\t", values);
            default:
                return record.value().toString();

        }
    }

    public static String getSqlType(Schema fieldSchema) {
        /*
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    // Maximum precision supported by MySQL is 65
                    int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
                    return "DECIMAL(65," + scale + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME(3)";
                case Timestamp.LOGICAL_NAME:
                    return "DATETIME(3)";
                default:
                    // pass through to primitive types
            }
        }
        */
        switch (fieldSchema.type()) {
            case INT8:
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
            case BOOLEAN:
                return "TINYINT";
            case STRING:
                return "TEXT";
            case BYTES:
                return "VARBINARY(1024)";
            default:
                throw new ConnectException(String.format("%s (%s) type doesn't have a mapping to the MemSQL database column type", fieldSchema.name(), fieldSchema.type()));
        }
    }
}
