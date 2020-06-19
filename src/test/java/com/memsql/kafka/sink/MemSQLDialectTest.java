package com.memsql.kafka.sink;

import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MemSQLDialectTest {

    @Test
    public void toJSON() throws IOException {
        assertEquals(MemSQLDialect.toJSON(Schema.INT8_SCHEMA, null), "null");

        assertEquals(MemSQLDialect.toJSON(Schema.INT8_SCHEMA, Byte.MIN_VALUE), "-128");
        assertEquals(MemSQLDialect.toJSON(Schema.INT8_SCHEMA, Byte.MAX_VALUE), "127");

        assertEquals(MemSQLDialect.toJSON(Schema.INT16_SCHEMA, Short.MIN_VALUE), "-32768");
        assertEquals(MemSQLDialect.toJSON(Schema.INT16_SCHEMA, Short.MAX_VALUE), "32767");

        assertEquals(MemSQLDialect.toJSON(Schema.INT32_SCHEMA, Integer.MIN_VALUE), "-2147483648");
        assertEquals(MemSQLDialect.toJSON(Schema.INT32_SCHEMA, Integer.MAX_VALUE), "2147483647");

        assertEquals(MemSQLDialect.toJSON(Schema.INT64_SCHEMA, Long.MIN_VALUE), "-9223372036854775808");
        assertEquals(MemSQLDialect.toJSON(Schema.INT64_SCHEMA, Long.MAX_VALUE), "9223372036854775807");

        assertEquals(MemSQLDialect.toJSON(Schema.FLOAT32_SCHEMA, Float.MIN_VALUE), "1.4E-45");
        assertEquals(MemSQLDialect.toJSON(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE), "3.4028235E38");

        assertEquals(MemSQLDialect.toJSON(Schema.FLOAT64_SCHEMA, Double.MIN_VALUE), "4.9E-324");
        assertEquals(MemSQLDialect.toJSON(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE), "1.7976931348623157E308");

        assertEquals(MemSQLDialect.toJSON(Schema.BOOLEAN_SCHEMA, true), "true");
        assertEquals(MemSQLDialect.toJSON(Schema.BOOLEAN_SCHEMA, false), "false");

        assertEquals(MemSQLDialect.toJSON(Schema.BYTES_SCHEMA, "some random bytes".getBytes(StandardCharsets.UTF_8)), "\"c29tZSByYW5kb20gYnl0ZXM=\"");

        assertEquals(MemSQLDialect.toJSON(Schema.STRING_SCHEMA, "some random string"), "\"some random string\"");

        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        assertEquals(MemSQLDialect.toJSON(schema, new Struct(schema).put("name", "Archie")), "{\"name\":\"Archie\"}");

        assertEquals(MemSQLDialect.toJSON(SchemaBuilder.array(Schema.STRING_SCHEMA),
                new ArrayList<>(Arrays.asList("Bob", "Alice"))),
                "[\"Bob\",\"Alice\"]");

        assertEquals(MemSQLDialect.toJSON(SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.INT32_SCHEMA
                ),
                new HashMap<String, Integer>() {{
                    put("Alice", 1);
                    put("Bob", 2);
                }}),
                "[{\"key\":\"Bob\",\"value\":2},{\"key\":\"Alice\",\"value\":1}]");

        assertEquals(MemSQLDialect.toJSON(SchemaBuilder.map(
                schema,
                SchemaBuilder.array(Schema.STRING_SCHEMA)
                ),
                new HashMap<Struct, List<String>>() {{
                    put(new Struct(schema).put("name", "Archie"), new ArrayList<>(Arrays.asList("Bob", "Alice")));
                }}),
                "[{\"key\":{\"name\":\"Archie\"},\"value\":[\"Bob\",\"Alice\"]}]");

        assertEquals(MemSQLDialect.toJSON(SchemaBuilder.array(
                SchemaBuilder.array(Schema.STRING_SCHEMA)
                ),
                new ArrayList<>(Arrays.asList(
                        new ArrayList<>(Arrays.asList("Bob", "Alice")),
                        new ArrayList<>(Arrays.asList("Alice", "Bob"))
                ))
                ),
                "[[\"Bob\",\"Alice\"],[\"Alice\",\"Bob\"]]");

        assertEquals(MemSQLDialect.toJSON(
                SchemaBuilder.map(
                        SchemaBuilder.map(
                                Schema.STRING_SCHEMA,
                                Schema.STRING_SCHEMA
                        ),
                        Schema.STRING_SCHEMA
                ),
                new HashMap<Map<String, String>, String>() {{
                    put(
                            new HashMap<String, String>() {{
                                put("nestedKey", "nestedValue");
                            }},
                            "value"
                    );
                }}),
                "[{\"key\":[{\"key\":\"nestedKey\",\"value\":\"nestedValue\"}],\"value\":\"value\"}]");

        Schema nestedSchema = SchemaBuilder.struct()
                .field("struct", schema)
                .build();

        assertEquals(MemSQLDialect.toJSON(
                nestedSchema,
                new Struct(nestedSchema).put(
                        "struct",
                        new Struct(schema).put("name", "Archie")
                )),
                "{\"struct\":{\"name\":\"Archie\"}}");
    }

    @Test
    public void successGetRecordValueStruct() throws IOException {
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("admin", SchemaBuilder.bool().defaultValue(false).build())
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Barbara Liskov")
                .put("age", 75);

        SinkRecord record = SinkRecordCreator.createRecord(schema, struct);
        String expectedValue = "Barbara Liskov\t75\tfalse";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueBoolean() throws IOException {
        Schema schema = SchemaBuilder.bool().name("schema").build();

        SinkRecord record = SinkRecordCreator.createRecord(schema, true);
        testGetRecord(record, "true");

        record = SinkRecordCreator.createRecord(schema, false);
        testGetRecord(record, "false");
    }

    @Test
    public void successGetRecordValueString() throws IOException {
        Schema schema = SchemaBuilder.string().name("schema").build();

        String value = "Some name";
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, value);

        value = "Some big string with a lot of words";
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, value);
    }

    @Test
    public void successGetRecordValueInt8() throws IOException {
        Schema schema = SchemaBuilder.int8().name("schema").build();
        testInt(schema);
    }

    @Test
    public void successGetRecordValueInt16() throws IOException {
        Schema schema = SchemaBuilder.int16().name("schema").build();
        testInt(schema);
    }

    @Test
    public void successGetRecordValueInt32() throws IOException {
        Schema schema = SchemaBuilder.int32().name("schema").build();
        testInt(schema);
    }

    @Test
    public void successGetRecordValueInt64() throws IOException {
        Schema schema = SchemaBuilder.int64().name("schema").build();
        testInt(schema);
    }

    @Test
    public void successGetRecordValueFloat32() throws IOException {
        Schema schema = SchemaBuilder.float32().name("schema").build();
        testFloat(schema);
    }

    @Test
    public void successGetRecordValueFloat64() throws IOException {
        Schema schema = SchemaBuilder.float64().name("schema").build();
        testFloat(schema);
    }

    private void testFloat(Schema schema) throws IOException {
        float value = 15.14f;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Float.toString(value));

        value = 5348.23523f;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Float.toString(value));
    }

    private void testInt(Schema schema) throws IOException {
        int value = 15;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));

        value = 555;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));
    }

    private void testGetRecord(SinkRecord record, String expectedValue) throws IOException {
        String value = MemSQLDialect.getRecordValueCSV(record);
        assertEquals(expectedValue, value);
    }
}
