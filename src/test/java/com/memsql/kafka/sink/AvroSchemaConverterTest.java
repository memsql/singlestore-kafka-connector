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
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"admin\",\"type\":\"boolean\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void structNestedSchemaConversion() {
        Schema nestedSchema = SchemaBuilder.struct()
                .field("key1", Schema.STRING_SCHEMA)
                .field("key2", Schema.INT32_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("nestedStruct", nestedSchema)
                .field("array", SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                ))
                .field("map", SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.STRING_SCHEMA
                ))
                .build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"nestedStruct\",\"type\":\"string\"},{\"name\":\"array\",\"type\":\"string\"},{\"name\":\"map\",\"type\":\"string\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void mapSchemaConversion() {
        Schema schema = SchemaBuilder.map(
                SchemaBuilder.string(),
                SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                )
        );
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void arraySchemaConversion() {
        Schema schema = SchemaBuilder.array(
                SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                )
        );
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void stringSchemaConversion() {
        Schema schema = SchemaBuilder.string().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void int8SchemaConversion() {
        Schema schema = SchemaBuilder.int8().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"int\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void int16SchemaConversion() {
        Schema schema = SchemaBuilder.int16().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"int\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void int32SchemaConversion() {
        Schema schema = SchemaBuilder.int32().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"int\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void int64SchemaConversion() {
        Schema schema = SchemaBuilder.int64().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"long\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void float32SchemaConversion() {
        Schema schema = SchemaBuilder.float32().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"float\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void float64SchemaConversion() {
        Schema schema = SchemaBuilder.float64().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"double\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void booleanSchemaConversion() {
        Schema schema = SchemaBuilder.bool().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"boolean\"}]}";
        assertSchema(schema, expectedSchema);
    }

    @Test
    public void bytesSchemaConversion() {
        Schema schema = SchemaBuilder.bytes().build();
        String expectedSchema = "{\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}";
        assertSchema(schema, expectedSchema);
    }

    private void assertSchema(Schema schema, String expectedSchema) {
        org.apache.avro.Schema avroSchema = AvroSchemaConverter.toAvroType(schema);
        String avroSchemaString = avroSchema.toString();
        assertEquals(expectedSchema, avroSchemaString);
    }
}
