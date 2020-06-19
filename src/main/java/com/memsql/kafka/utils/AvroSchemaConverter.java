package com.memsql.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;

import static org.apache.avro.Schema.Type.NULL;

public class AvroSchemaConverter {

    private static Schema nullSchema = Schema.create(NULL);

    public static Schema toAvroType(org.apache.kafka.connect.data.Schema kafkaSchema) {
        if (kafkaSchema.type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
            return toAvroType(kafkaSchema, false, "topLevelRecord", "");
        } else {
            String recordName = "topLevelRecord";
            SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
            SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = builder.record(recordName).namespace("").fields();
            Schema fieldAvroType =
                toAvroType(kafkaSchema.schema(), kafkaSchema.schema().isOptional(), kafkaSchema.name(), "topLevelRecord");
            String fieldName = kafkaSchema.name() == null ? "data" : kafkaSchema.name();
                    fieldsAssembler.name(fieldName).type(fieldAvroType).noDefault();
            return fieldsAssembler.endRecord();
        }
    }

    private static Schema toAvroType(org.apache.kafka.connect.data.Schema kafkaSchema,
                                     boolean nullable,
                                     String recordName,
                                     String nameSpace) {
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
                schema = builder.stringType();
                break;
            case STRUCT:
                String childNameSpace = (!nameSpace.equals("")) ? String.format("%s.%s", nameSpace, recordName) : recordName;
                SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields();
                kafkaSchema.fields().forEach(f -> {
                    Schema fieldAvroType =
                            toAvroType(f.schema(), f.schema().isOptional(), f.name(), childNameSpace);
                    fieldsAssembler.name(f.name()).type(fieldAvroType).noDefault();
                });
                schema = fieldsAssembler.endRecord();
                break;
            default: throw new ConnectException(String.format("Unexpected kafka type `%s`.", kafkaSchema.type()));
        }
        if (nullable) {
            return Schema.createUnion(schema, nullSchema);
        } else {
            return schema;
        }
    }

    public static Schema resolveNullableType(Schema avroSchema, boolean nullable) {
        if (nullable && avroSchema.getType() != NULL) {
            List<Schema> fields = avroSchema.getTypes();
            if (fields.size() < 2) return avroSchema;
            return (fields.get(0).getType() == NULL) ? fields.get(1) : fields.get(0);
        } else {
            return avroSchema;
        }
    }
}
