package com.singlestore.kafka.integration;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.fail;

public class DataTypesTest extends IntegrationBase {
    @Test
    public void schemaless() throws SQLException {
        Map<Object, Object> mp = new HashMap<>();
        mp.put("boolean", false);
        mp.put("int", 10);
        mp.put("long", Long.MAX_VALUE);
        mp.put("float", 10.1f);
        mp.put("double", 10.1d);
        mp.put("bytes", "abc".getBytes(StandardCharsets.UTF_8));
        mp.put("string", "ab\\\t\nc");
        mp.put("array", Arrays.asList("asd", "bcd"));
        Map<Object, Object> nestedMap = new HashMap<>();
        nestedMap.put("c1", "v1");
        nestedMap.put("c2", "v2");
        mp.put("map", nestedMap);

        List<SinkRecord> records = Collections.singletonList(createRecord(null, mp, "schemaless"));
        Map<String, String> props = new HashMap<>();

        put(props, records, "CREATE TABLE testdb.schemaless(`boolean` BOOL, `int` INT, `long` LONG, `float` FLOAT, `double` DOUBLE, `bytes` BLOB, `string` TEXT, `array` JSON, `map` JSON)", true);
    }

    @Test
    public void int8() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.INT8_SCHEMA, Byte.MAX_VALUE, "int8"));
            records.add(createRecord( Schema.INT8_SCHEMA, Byte.MIN_VALUE, "int8"));
            put(props, records);

            records.clear();
            records.add(createRecord( Schema.OPTIONAL_INT8_SCHEMA, Byte.MIN_VALUE, "int8Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT8_SCHEMA, Byte.MAX_VALUE, "int8Optional"));
            records.add(createRecord( Schema.OPTIONAL_INT8_SCHEMA, null, "int8Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void int16() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord( Schema.INT16_SCHEMA, Short.MAX_VALUE, "int16"));
            records.add(createRecord(Schema.INT16_SCHEMA, Short.MIN_VALUE, "int16"));
            put(props, records);

            records.clear();
            records.add(createRecord( Schema.OPTIONAL_INT16_SCHEMA, Short.MIN_VALUE, "int16Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT16_SCHEMA, Short.MAX_VALUE, "int16Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT16_SCHEMA, null,"int16Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void int32() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord( Schema.INT32_SCHEMA, Integer.MAX_VALUE, "int32"));
            records.add(createRecord( Schema.INT32_SCHEMA, Integer.MIN_VALUE, "int32"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_INT32_SCHEMA, Integer.MIN_VALUE, "int32Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT32_SCHEMA, Integer.MAX_VALUE, "int32Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT32_SCHEMA, null, "int32Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void int64() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.INT64_SCHEMA, Long.MAX_VALUE, "int64"));
            records.add(createRecord(Schema.INT64_SCHEMA, Long.MIN_VALUE, "int64"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_INT64_SCHEMA, Long.MIN_VALUE, "int64Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT64_SCHEMA, Long.MAX_VALUE, "int64Optional"));
            records.add(createRecord(Schema.OPTIONAL_INT64_SCHEMA, null, "int64Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void float32() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE, "float32"));
            records.add(createRecord(Schema.FLOAT32_SCHEMA, Float.MIN_VALUE, "float32"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_FLOAT32_SCHEMA, Float.MIN_VALUE, "float32Optional"));
            records.add(createRecord(Schema.OPTIONAL_FLOAT32_SCHEMA, Float.MAX_VALUE, "float32Optional"));
            records.add(createRecord(Schema.OPTIONAL_FLOAT32_SCHEMA, null, "float32Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void float64() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE, "float64"));
            records.add(createRecord(Schema.FLOAT64_SCHEMA, Double.MIN_VALUE, "float64"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MIN_VALUE, "float64Optional"));
            records.add(createRecord(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MAX_VALUE, "float64Optional"));
            records.add(createRecord(Schema.OPTIONAL_FLOAT64_SCHEMA, null, "float64Optional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void booleanTest() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.BOOLEAN_SCHEMA, false, "boolean"));
            records.add(createRecord(Schema.BOOLEAN_SCHEMA, true, "boolean"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_BOOLEAN_SCHEMA, false, "booleanOptional"));
            records.add(createRecord(Schema.OPTIONAL_BOOLEAN_SCHEMA, true, "booleanOptional"));
            records.add(createRecord(Schema.OPTIONAL_BOOLEAN_SCHEMA, null, "booleanOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void bytes() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.BYTES_SCHEMA, "\t\t\t\n\0\"'\\\\\n\t`NULL\\\t\\..<>\n\t\\,@!#$%^&*(\"'".getBytes(StandardCharsets.UTF_8), "bytes"));
            records.add(createRecord(Schema.BYTES_SCHEMA, "".getBytes(StandardCharsets.UTF_8), "bytes"));
            byte[] allBytes = new byte[256];
            for (byte i = Byte.MIN_VALUE; ; i++) {
                allBytes[i-Byte.MIN_VALUE] = i;
                if (i == Byte.MAX_VALUE) {
                    break;
                }
            }
            records.add(createRecord(Schema.BYTES_SCHEMA, allBytes, "bytes"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_BYTES_SCHEMA, null, "bytesOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void string() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.STRING_SCHEMA, "\t\t\t\n\0\"'\\\\\n\t`NULL\\\t\\..<>\n\t\\,@!#$%^&*(\"'", "string"));
            records.add(createRecord(Schema.STRING_SCHEMA, "", "string"));
            byte[] allBytes = new byte[256];
            for (byte i = Byte.MIN_VALUE; ; i++) {
                allBytes[i-Byte.MIN_VALUE] = i;
                if (i == Byte.MAX_VALUE) {
                    break;
                }
            }
            records.add(createRecord(Schema.STRING_SCHEMA, new String(allBytes), "string"));
            put(props, records);

            records.clear();
            records.add(createRecord(Schema.OPTIONAL_STRING_SCHEMA, null, "stringOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void array() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord( SchemaBuilder.array(Schema.STRING_SCHEMA),
                    new ArrayList<>(Arrays.asList("Bob", "A\n\n\tlice")), "array"));
            records.add(createRecord(SchemaBuilder.array(Schema.STRING_SCHEMA), new ArrayList<>(), "array"));
            put(props, records);

            records.clear();
            records.add(createRecord( Schema.OPTIONAL_STRING_SCHEMA, null, "arrayOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void map() {
        try {
            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord( SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    Schema.INT32_SCHEMA
                    ),
                    new HashMap<String, Integer>() {{
                        put("Alice", 1);
                        put("Bob", 2);
                    }},
                    "map"));
            records.add(createRecord( SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    Schema.INT32_SCHEMA
                    ), new HashMap<String, Integer>(),
                    "map"));
            put(props, records);

            records.clear();
            records.add(createRecord( SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    Schema.INT32_SCHEMA
            ).optional(), null, "mapOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void struct() {
        try {
            Schema schema = SchemaBuilder.struct()
                    .field("name", Schema.STRING_SCHEMA)
                    .build();
            Schema mainSchema = SchemaBuilder.struct()
                    .field("data", schema)
                    .build();

            Map<String, String> props = new HashMap<>();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(mainSchema,
                    new Struct(mainSchema).put("data",
                            new Struct(schema).put("name", "Archie")), "struct"));

            put(props, records);

            records.clear();
            Schema optionalSchema = SchemaBuilder.struct()
                    .field("name", Schema.STRING_SCHEMA)
                    .optional()
                    .build();
            Schema mainOptionalSchema = SchemaBuilder.struct()
                    .field("data", optionalSchema)
                    .build();
            records.add(createRecord(mainOptionalSchema,
                    new Struct(mainOptionalSchema).put("data", null), "structOptional"));
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
