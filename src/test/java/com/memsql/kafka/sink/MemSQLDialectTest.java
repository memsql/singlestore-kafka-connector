package com.memsql.kafka.sink;

import com.memsql.kafka.utils.SinkRecordCreator;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
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
        String expectedValue = "Barbara Liskov\t75\t0";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueStructWithJson() throws IOException {
        Schema nestedSchema = SchemaBuilder.struct().name("nestedSchema")
                .field("key1", Schema.STRING_SCHEMA)
                .field("key2", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("nested", nestedSchema)
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Barbara Liskov")
                .put("age", 75)
                .put("nested", new Struct(nestedSchema)
                        .put("key1", "value1")
                        .put("key2", 75));

        SinkRecord record = SinkRecordCreator.createRecord(schema, struct);
        String expectedValue = "Barbara Liskov\t75\t{\"key1\":\"value1\",\"key2\":75}";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueArray() throws IOException {
        Schema schema = SchemaBuilder.array(
                Schema.STRING_SCHEMA
        );

        List<String> values = new ArrayList<String>() {{
            add("Name");
            add("Age");
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[\"Name\",\"Age\"]";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueArrayComplex() throws IOException {
        Schema schema = SchemaBuilder.array(
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                )
        );

        List<Map<String, Integer>> values = new ArrayList<Map<String, Integer>>() {{
            add(new HashMap<String, Integer>() {{
                put("key1", 12);
                put("key2", 500);
            }});
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[[{\"key\":\"key1\",\"value\":12},{\"key\":\"key2\",\"value\":500}]]";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueMap() throws IOException {
        Schema schema = SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.STRING_SCHEMA
        );

        Map<String, String> values = new HashMap<String, String>() {{
            put("name", "Alice");
            put("surname", "Woffenden");
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[{\"key\":\"surname\",\"value\":\"Woffenden\"},{\"key\":\"name\",\"value\":\"Alice\"}]";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueMapComplex() throws IOException {
        Schema schema = SchemaBuilder.map(
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                ),
                SchemaBuilder.array(
                        Schema.BOOLEAN_SCHEMA
                )
        );
        Map<Map<String, Integer>, List<Boolean>> values = new HashMap<Map<String, Integer>, List<Boolean>>() {{
            put(new HashMap<String, Integer>() {{
                put("key1", 0);
                put("key2", 100);
            }}, new ArrayList<Boolean>() {{
                add(true);
                add(true);
                add(false);
            }});
        }};
        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[{\"key\":[{\"key\":\"key1\",\"value\":0},{\"key\":\"key2\",\"value\":100}],\"value\":[true,true,false]}]";
        testGetRecord(record, expectedValue);
    }

    @Test
    public void successGetRecordValueBoolean() throws IOException {
        Schema schema = SchemaBuilder.bool().name("schema").build();

        SinkRecord record = SinkRecordCreator.createRecord(schema, true);
        testGetRecord(record, "1");

        record = SinkRecordCreator.createRecord(schema, false);
        testGetRecord(record, "0");
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
        byte value = -128;
        SinkRecord record = SinkRecordCreator.createRecord(schema, (int) value);
        testGetRecord(record, Integer.toString(value));

        value = 127;
        record = SinkRecordCreator.createRecord(schema, (int) value);
        testGetRecord(record, Integer.toString(value));
    }

    @Test
    public void successGetRecordValueInt16() throws IOException {
        Schema schema = SchemaBuilder.int16().name("schema").build();
        short value = 32767;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));

        value = -32768;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));
    }

    @Test
    public void successGetRecordValueInt32() throws IOException {
        Schema schema = SchemaBuilder.int32().name("schema").build();
        int value = 1000000000;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 555;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordValueInt64() throws IOException {
        Schema schema = SchemaBuilder.int64().name("schema").build();
        long value = 1000000000000000000L;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 555L;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordValueFloat32() throws IOException {
        Schema schema = SchemaBuilder.float32().name("schema").build();
        float value = 15.14f;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Float.toString(value));

        value = 5348.23523f;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Float.toString(value));
    }

    @Test
    public void successGetRecordValueFloat64() throws IOException {
        Schema schema = SchemaBuilder.float64().name("schema").build();
        double value = 15.14d;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Double.toString(value));

        value = 5348.23523d;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Double.toString(value));
    }

    private void testGetRecord(SinkRecord record, String expectedValue) throws IOException {
        String value = MemSQLDialect.getRecordValueCSV(record);
        assertEquals(expectedValue, value);
    }

    @Test
    public void getSchemaForCrateTableQueryStruct() {
        Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.STRING_SCHEMA)
                .field("f3", Schema.STRING_SCHEMA)
                .field("f4", Schema.STRING_SCHEMA)
                .field("f5", Schema.STRING_SCHEMA)
                .field("f6", Schema.STRING_SCHEMA)
                .build();

        List<TableKey> keys = new ArrayList<>(Arrays.asList(
                new TableKey(TableKey.Type.COLUMNSTORE, "n1", "f1"),
                new TableKey(TableKey.Type.UNIQUE, "n2", "f2, f1"),
                new TableKey(TableKey.Type.PRIMARY, "n3", "f3"),
                new TableKey(TableKey.Type.SHARD, "n4", "f4"),
                new TableKey(TableKey.Type.KEY, "", "f5")
                ));

        assertEquals(MemSQLDialect.getSchemaForCrateTableQuery(schema, keys), "(\n" +
                "`f1` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "`f2` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "`f3` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "`f4` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "`f5` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "`f6` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "KEY `n1`(f1) USING CLUSTERED COLUMNSTORE,\n" +
                "UNIQUE KEY `n2`(f2, f1),\n" +
                "PRIMARY KEY `n3`(f3),\n" +
                "SHARD KEY `n4`(f4),\n" +
                "KEY (f5),\n" +
                "KEY (f1) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableQueryNotStruct() {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>(Collections.singletonList(
                new TableKey(TableKey.Type.COLUMNSTORE, "", "data")
        ));

        assertEquals(MemSQLDialect.getSchemaForCrateTableQuery(schema, keys), "(\n" +
                "`data` TEXT COLLATE UTF8_BIN NOT NULL,\n" +
                "KEY (data) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableQueryNoKeys() {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>();

        assertEquals(MemSQLDialect.getSchemaForCrateTableQuery(schema, keys), "(\n" +
                "`data` TEXT COLLATE UTF8_BIN NOT NULL\n" +
                ")");
    }

    @Test
    public void successGetRecordsValueStruct() throws IOException {
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
        testGetRecords(record, Arrays.asList("Barbara Liskov", 75, false));
    }

    @Test
    public void successGetRecordsValueStructWithJson() throws IOException {
        Schema nestedSchema = SchemaBuilder.struct().name("nestedSchema")
                .field("key1", Schema.STRING_SCHEMA)
                .field("key2", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("nested", nestedSchema)
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Barbara Liskov")
                .put("age", 75)
                .put("nested", new Struct(nestedSchema)
                        .put("key1", "value1")
                        .put("key2", 75));

        SinkRecord record = SinkRecordCreator.createRecord(schema, struct);
        testGetRecords(record, Arrays.asList("Barbara Liskov", 75, "{\"key1\":\"value1\",\"key2\":75}"));
    }

    @Test
    public void successGetRecordsValueArray() throws IOException {
        Schema schema = SchemaBuilder.array(
                Schema.STRING_SCHEMA
        );

        List<String> values = new ArrayList<String>() {{
            add("Name");
            add("Age");
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[\"Name\",\"Age\"]";
        testGetRecords(record, Collections.singletonList(expectedValue));
    }

    @Test
    public void successGetRecordsValueArrayComplex() throws IOException {
        Schema schema = SchemaBuilder.array(
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                )
        );

        List<Map<String, Integer>> values = new ArrayList<Map<String, Integer>>() {{
            add(new HashMap<String, Integer>() {{
                put("key1", 12);
                put("key2", 500);
            }});
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[[{\"key\":\"key1\",\"value\":12},{\"key\":\"key2\",\"value\":500}]]";
        testGetRecords(record, Collections.singletonList(expectedValue));
    }

    @Test
    public void successGetRecordsValueMap() throws IOException {
        Schema schema = SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                Schema.STRING_SCHEMA
        );

        Map<String, String> values = new HashMap<String, String>() {{
            put("name", "Alice");
            put("surname", "Woffenden");
        }};

        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[{\"key\":\"surname\",\"value\":\"Woffenden\"},{\"key\":\"name\",\"value\":\"Alice\"}]";
        testGetRecords(record, Collections.singletonList(expectedValue));
    }

    @Test
    public void successGetRecordsValueMapComplex() throws IOException {
        Schema schema = SchemaBuilder.map(
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                ),
                SchemaBuilder.array(
                        Schema.BOOLEAN_SCHEMA
                )
        );
        Map<Map<String, Integer>, List<Boolean>> values = new HashMap<Map<String, Integer>, List<Boolean>>() {{
            put(new HashMap<String, Integer>() {{
                put("key1", 0);
                put("key2", 100);
            }}, new ArrayList<Boolean>() {{
                add(true);
                add(true);
                add(false);
            }});
        }};
        SinkRecord record = SinkRecordCreator.createRecord(schema, values);
        String expectedValue = "[{\"key\":[{\"key\":\"key1\",\"value\":0},{\"key\":\"key2\",\"value\":100}],\"value\":[true,true,false]}]";
        testGetRecords(record, Collections.singletonList(expectedValue));
    }

    @Test
    public void successGetRecordsValueBoolean() throws IOException {
        Schema schema = SchemaBuilder.bool().name("schema").build();

        SinkRecord record = SinkRecordCreator.createRecord(schema, true);
        testGetRecords(record, Collections.singletonList(true));

        record = SinkRecordCreator.createRecord(schema, false);
        testGetRecords(record, Collections.singletonList(false));
    }

    @Test
    public void successGetRecordsValueString() throws IOException {
        Schema schema = SchemaBuilder.string().name("schema").build();

        String value = "Some name";
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = "Some big string with a lot of words";
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordsValueInt8() throws IOException {
        Schema schema = SchemaBuilder.int8().name("schema").build();
        byte value = 127;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList((int)value));

        value = -128;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList((int)value));
    }

    @Test
    public void successGetRecordsValueInt16() throws IOException {
        Schema schema = SchemaBuilder.int16().name("schema").build();
        short value = 32767;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList((int) value));

        value = -32768;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList((int)value));
    }

    @Test
    public void successGetRecordsValueInt32() throws IOException {
        Schema schema = SchemaBuilder.int32().name("schema").build();
        int value = 15;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 555;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordsValueInt64() throws IOException {
        Schema schema = SchemaBuilder.int64().name("schema").build();
        long value = 1000000000000000000L;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 555;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordsValueFloat32() throws IOException {
        Schema schema = SchemaBuilder.float32().name("schema").build();
        float value = 15.14f;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 5348.23523f;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    @Test
    public void successGetRecordsValueFloat64() throws IOException {
        Schema schema = SchemaBuilder.float64().name("schema").build();
        double value = 15.14d;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 5348.23523d;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    private void testIntRecords(Schema schema) throws IOException {
        int value = 15;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));

        value = 555;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecords(record, Collections.singletonList(value));
    }

    private void testGetRecords(SinkRecord record, List<Object> expectedValues) throws IOException {
        List<Object> values = MemSQLDialect.getRecordValues(record);
        assertArrayEquals(expectedValues.toArray(), values.toArray());
    }
}
