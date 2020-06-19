package com.memsql.kafka.sink;

import com.memsql.kafka.utils.AvroSchemaConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AvroSchemaConverterTest {

    @Test
    public void structSchemaConversion() {
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("admin", SchemaBuilder.bool().defaultValue(false).build())
                .build();
        org.apache.avro.Schema avroSchema = AvroSchemaConverter.toAvroType(schema);
        String avroSchemaString = avroSchema.toString();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"admin\",\"type\":\"boolean\"}]}";
        assertEquals(expectedSchema, avroSchemaString);
    }

    @Test
    public void mapSchemaConversion() {
        Schema schema = SchemaBuilder.array(
                SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                )
        );
        org.apache.avro.Schema avroSchema = AvroSchemaConverter.toAvroType(schema);
        String avroSchemaString = avroSchema.toString();
        System.out.println("");
    }
}
