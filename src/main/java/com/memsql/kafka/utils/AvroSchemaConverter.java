package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDialect;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;

import static org.apache.avro.Schema.Type.NULL;

public class AvroSchemaConverter {

    private static final Schema nullSchema = Schema.create(NULL);

    public static Schema toAvroType(org.apache.kafka.connect.data.Schema kafkaSchema) {
        SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
        SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = builder.record("topLevelRecord").namespace("").fields();
        if (kafkaSchema.type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
            kafkaSchema.fields().forEach(f -> {
                Schema fieldAvroType = toAvroType(f.schema(), f.schema().isOptional());
                fieldsAssembler.name(f.name()).type(fieldAvroType).noDefault();
            });
        } else {
            Schema fieldAvroType = toAvroType(kafkaSchema.schema(), kafkaSchema.schema().isOptional());
            String fieldName = MemSQLDialect.getDefaultColumnName(kafkaSchema);
            fieldsAssembler.name(fieldName).type(fieldAvroType).noDefault();
        }
        return fieldsAssembler.endRecord();
    }

    private static Schema toAvroType(org.apache.kafka.connect.data.Schema kafkaSchema,
                                     boolean nullable) {
        SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
        Schema schema;
        switch (kafkaSchema.type()) {
            case INT8:
            case INT16:
            case INT32:
                schema = builder.intType();
                break;
            case INT64:
                schema = builder.longType();
                break;
            case FLOAT32:
                schema = builder.floatType();
                break;
            case FLOAT64:
                schema = builder.doubleType();
                break;
            case BOOLEAN:
                schema = builder.booleanType();
                break;
            case BYTES:
                schema = builder.bytesType();
                break;
            case STRING:
            case MAP:
            case ARRAY:
            case STRUCT:
                schema = builder.stringType();
                break;
            default: throw new ConnectException(String.format("Unexpected kafka type `%s`.", kafkaSchema.type()));
        }
        if (nullable) {
            return Schema.createUnion(schema, nullSchema);
        } else {
            return schema;
        }
    }

    public static Schema resolveNullableType(Schema avroSchema) {
        if (avroSchema.getType() == NULL) {
            return avroSchema;
        }

        List<Schema> fields = avroSchema.getTypes();
        if (fields.size() < 2) return avroSchema;
        return (fields.get(0).getType() == NULL) ? fields.get(1) : fields.get(0);
    }
}
