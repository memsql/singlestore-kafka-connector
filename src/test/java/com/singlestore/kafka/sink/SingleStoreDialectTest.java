package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.SinkRecordCreator;
import com.singlestore.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

public class SingleStoreDialectTest {

    protected static final Logger log = LoggerFactory.getLogger(SingleStoreDialectTest.class);

    @Test
    public void toJSON() {
        try {
            assertEquals(SingleStoreDialect.toJSON(Schema.INT8_SCHEMA, null), "null");

            assertEquals(SingleStoreDialect.toJSON(Schema.INT8_SCHEMA, Byte.MIN_VALUE), "-128");
            assertEquals(SingleStoreDialect.toJSON(Schema.INT8_SCHEMA, Byte.MAX_VALUE), "127");

            assertEquals(SingleStoreDialect.toJSON(Schema.INT16_SCHEMA, Short.MIN_VALUE), "-32768");
            assertEquals(SingleStoreDialect.toJSON(Schema.INT16_SCHEMA, Short.MAX_VALUE), "32767");

            assertEquals(SingleStoreDialect.toJSON(Schema.INT32_SCHEMA, Integer.MIN_VALUE), "-2147483648");
            assertEquals(SingleStoreDialect.toJSON(Schema.INT32_SCHEMA, Integer.MAX_VALUE), "2147483647");

            assertEquals(SingleStoreDialect.toJSON(Schema.INT64_SCHEMA, Long.MIN_VALUE), "-9223372036854775808");
            assertEquals(SingleStoreDialect.toJSON(Schema.INT64_SCHEMA, Long.MAX_VALUE), "9223372036854775807");

            assertEquals(SingleStoreDialect.toJSON(Schema.FLOAT32_SCHEMA, Float.MIN_VALUE), "1.4E-45");
            assertEquals(SingleStoreDialect.toJSON(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE), "3.4028235E38");

            assertEquals(SingleStoreDialect.toJSON(Schema.FLOAT64_SCHEMA, Double.MIN_VALUE), "4.9E-324");
            assertEquals(SingleStoreDialect.toJSON(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE), "1.7976931348623157E308");

            assertEquals(SingleStoreDialect.toJSON(Schema.BOOLEAN_SCHEMA, true), "true");
            assertEquals(SingleStoreDialect.toJSON(Schema.BOOLEAN_SCHEMA, false), "false");

            assertEquals(SingleStoreDialect.toJSON(Schema.BYTES_SCHEMA, "some random bytes".getBytes(StandardCharsets.UTF_8)), "\"c29tZSByYW5kb20gYnl0ZXM=\"");

            assertEquals(SingleStoreDialect.toJSON(Schema.STRING_SCHEMA, "some random string"), "\"some random string\"");

            Schema schema = SchemaBuilder.struct()
                    .field("name", Schema.STRING_SCHEMA)
                    .build();
            assertEquals(SingleStoreDialect.toJSON(schema, new Struct(schema).put("name", "Archie")), "{\"name\":\"Archie\"}");

            assertEquals(SingleStoreDialect.toJSON(SchemaBuilder.array(Schema.STRING_SCHEMA),
                    new ArrayList<>(Arrays.asList("Bob", "Alice"))),
                    "[\"Bob\",\"Alice\"]");

            assertEquals(SingleStoreDialect.toJSON(SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    Schema.INT32_SCHEMA
                    ),
                    new HashMap<String, Integer>() {{
                        put("Alice", 1);
                        put("Bob", 2);
                    }}),
                    "[{\"key\":\"Bob\",\"value\":2},{\"key\":\"Alice\",\"value\":1}]");

            assertEquals(SingleStoreDialect.toJSON(SchemaBuilder.map(
                    schema,
                    SchemaBuilder.array(Schema.STRING_SCHEMA)
                    ),
                    new HashMap<Struct, List<String>>() {{
                        put(new Struct(schema).put("name", "Archie"), new ArrayList<>(Arrays.asList("Bob", "Alice")));
                    }}),
                    "[{\"key\":{\"name\":\"Archie\"},\"value\":[\"Bob\",\"Alice\"]}]");

            assertEquals(SingleStoreDialect.toJSON(SchemaBuilder.array(
                    SchemaBuilder.array(Schema.STRING_SCHEMA)
                    ),
                    new ArrayList<>(Arrays.asList(
                            new ArrayList<>(Arrays.asList("Bob", "Alice")),
                            new ArrayList<>(Arrays.asList("Alice", "Bob"))
                    ))
                    ),
                    "[[\"Bob\",\"Alice\"],[\"Alice\",\"Bob\"]]");

            assertEquals(SingleStoreDialect.toJSON(
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

            assertEquals(SingleStoreDialect.toJSON(
                    nestedSchema,
                    new Struct(nestedSchema).put(
                            "struct",
                            new Struct(schema).put("name", "Archie")
                    )),
                    "{\"struct\":{\"name\":\"Archie\"}}");
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }

        try {
            SingleStoreDialect.toJSON(Schema.INT8_SCHEMA, true);
            fail("Exception should be thrown");
        } catch(ConnectException ex) {
            assertEquals(ex.getLocalizedMessage(), "The object 'true' has an incorrect schema (INT8)");
        } catch(IOException ignored) {
            fail("ConnectException should be thrown");
        }
    }

    @Test
    public void successGetRecordValueStruct() {
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
    public void successGetRecordValueStructWithJson() {
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
    public void successGetRecordValueArray() {
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
    public void successGetRecordValueArrayComplex() {
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
    public void successGetRecordValueMap() {
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
    public void successGetRecordValueMapComplex() {
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
    public void successGetRecordValueBoolean() {
        Schema schema = SchemaBuilder.bool().name("schema").build();

        SinkRecord record = SinkRecordCreator.createRecord(schema, true);
        testGetRecord(record, "1");

        record = SinkRecordCreator.createRecord(schema, false);
        testGetRecord(record, "0");
    }

    @Test
    public void successGetRecordValueString() {
        Schema schema = SchemaBuilder.string().name("schema").build();

        String value = "Some name";
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, value);

        value = "Some big string with a lot of words";
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, value);
    }

    @Test
    public void successGetRecordValueInt8() {
        Schema schema = SchemaBuilder.int8().name("schema").build();
        byte value = -128;
        SinkRecord record = SinkRecordCreator.createRecord(schema, (int) value);
        testGetRecord(record, Integer.toString(value));

        value = 127;
        record = SinkRecordCreator.createRecord(schema, (int) value);
        testGetRecord(record, Integer.toString(value));
    }

    @Test
    public void successGetRecordValueInt16() {
        Schema schema = SchemaBuilder.int16().name("schema").build();
        short value = 32767;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));

        value = -32768;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Integer.toString(value));
    }


    @Test
    public void successGetRecordValueFloat64() {
        Schema schema = SchemaBuilder.float64().name("schema").build();
        double value = 15.14d;
        SinkRecord record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Double.toString(value));

        value = 5348.23523d;
        record = SinkRecordCreator.createRecord(schema, value);
        testGetRecord(record, Double.toString(value));
    }

    private void testGetRecord(SinkRecord record, String expectedValue) {
        try {
            String value = SingleStoreDialect.getRecordValueCSV(record);
            assertEquals(expectedValue, value);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
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
                new TableKey(TableKey.Type.COLUMNSTORE, "n1", Collections.singletonList("f1")),
                new TableKey(TableKey.Type.UNIQUE, "n2", new ArrayList<>(Arrays.asList("f2", "f1"))),
                new TableKey(TableKey.Type.PRIMARY, "n3", Collections.singletonList("f3")),
                new TableKey(TableKey.Type.SHARD, "n4", Collections.singletonList("f4")),
                new TableKey(TableKey.Type.KEY, "", Collections.singletonList("f5"))
                ));

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys), "(\n" +
                "`f1` TEXT NOT NULL,\n" +
                "`f2` TEXT NOT NULL,\n" +
                "`f3` TEXT NOT NULL,\n" +
                "`f4` TEXT NOT NULL,\n" +
                "`f5` TEXT NOT NULL,\n" +
                "`f6` TEXT NOT NULL,\n" +
                "KEY `n1`(`f1`) USING CLUSTERED COLUMNSTORE,\n" +
                "UNIQUE KEY `n2`(`f2`, `f1`),\n" +
                "PRIMARY KEY `n3`(`f3`),\n" +
                "SHARD KEY `n4`(`f4`),\n" +
                "KEY (`f5`)\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableColumnstore() {
        Schema schema = SchemaBuilder.struct()
                .field("f1", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .field("f2", Schema.STRING_SCHEMA)
                .build();

        List<TableKey> keys = new ArrayList<>(Collections.emptyList());

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys), "(\n" +
                "`f1` JSON NOT NULL,\n" +
                "`f2` TEXT NOT NULL,\n" +
                "KEY (`f2`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }


    @Test
    public void getSchemaForCrateTableQueryNotStruct() {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>(Collections.singletonList(
                new TableKey(TableKey.Type.COLUMNSTORE, "", Collections.singletonList("data"))
        ));

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys), "(\n" +
                "`data` TEXT NOT NULL,\n" +
                "KEY (`data`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableQueryNoKeys() {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>();

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys), "(\n" +
                "`data` TEXT NOT NULL,\n" +
                "KEY (`data`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getColumnNames() {
        assertEquals(SingleStoreDialect.getColumnNames(SchemaBuilder.struct().field("qwe-rty", Schema.INT32_SCHEMA).field("`\\\\", Schema.STRING_SCHEMA)),
                "`qwe-rty`, ```\\\\`");
        assertEquals(SingleStoreDialect.getColumnNames(Schema.STRING_SCHEMA),
                "`data`");
        assertEquals(SingleStoreDialect.getColumnNames(SchemaBuilder.string().name("qwe-rty")),
                "`qwe-rty`");
    }
}
