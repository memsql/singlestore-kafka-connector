package com.singlestore.kafka.sink;

import static org.junit.Assert.*;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.data.Schema;
import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ValueWithSchemaTest {

    @Test
    public void getColumnsNoSchema() {
        assertEquals(Collections.singletonList("data"), new ValueWithSchema(createRecord(null, 1)).getColumns());
        assertEquals(Collections.emptyList(), new ValueWithSchema(createRecord(null, new HashMap<>())).getColumns());
        Map<Object, Object> mp = new HashMap<>();
        mp.put("c1", "v1");
        mp.put("c2", "v2");
        mp.put("c3", "v2");
        assertEquals(Arrays.asList("c1", "c2", "c3"),
            new ValueWithSchema(createRecord(null, mp)).getColumns().stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void getColumnsWithSchema() {
        assertEquals(Collections.singletonList("data"), new ValueWithSchema(createRecord(Schema.INT32_SCHEMA, 1)).getColumns());
        Schema schema = SchemaBuilder.string().name("col1").build();
        assertEquals(Collections.singletonList("col1"), new ValueWithSchema(createRecord(schema, "asd")).getColumns());
        schema = SchemaBuilder.struct().build();
        assertEquals(Collections.emptyList(), new ValueWithSchema(createRecord(schema, new Struct(schema))).getColumns());
        schema = SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA)
            .field("c2", Schema.STRING_SCHEMA)
            .field("c3", Schema.STRING_SCHEMA)
            .build();
        assertEquals(Arrays.asList("c1", "c2", "c3"),
            new ValueWithSchema(createRecord(schema, new Struct(schema)
            .put("c1", "v1")
            .put("c2", "v2")
            .put("c3", "v2")
        )).getColumns());
    }

    @Test
    public void toCSVNoSchema() throws IOException {
        Map<Object, Object> mp = new HashMap<>();
        assertEquals("", new ValueWithSchema(createRecord(null, mp)).toCSV(Collections.emptyList()));
        assertEquals("\\N", new ValueWithSchema(createRecord(null, mp)).toCSV(Collections.singletonList("c1")));

        assertEquals("1", new ValueWithSchema(createRecord(null, true)).toCSV(Collections.singletonList("data")));
        assertEquals("1", new ValueWithSchema(createRecord(null, 1)).toCSV(Collections.singletonList("data")));
        assertEquals("9223372036854775807", new ValueWithSchema(createRecord(null, Long.MAX_VALUE)).toCSV(Collections.singletonList("data")));
        assertEquals("10.1", new ValueWithSchema(createRecord(null, 10.1f)).toCSV(Collections.singletonList("data")));
        assertEquals("10.1", new ValueWithSchema(createRecord(null, 10.1d)).toCSV(Collections.singletonList("data")));
        assertEquals("asd", new ValueWithSchema(createRecord(null, "asd")).toCSV(Collections.singletonList("data")));
        assertEquals("[\"asd\",\"bcd\"]", new ValueWithSchema(createRecord(null, Arrays.asList("asd", "bcd"))).toCSV(Collections.singletonList("data")));

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
        assertEquals("0\t10\t9223372036854775807\t10.1\t10.1\tabc\tab\\\\\\\t\\\nc\t[\"asd\",\"bcd\"]\t{\"c1\":\"v1\",\"c2\":\"v2\"}",
            new ValueWithSchema(createRecord(null, mp)).toCSV(Arrays.asList(
                "boolean",
                "int",
                "long",
                "float",
                "double",
                "bytes",
                "string",
                "array",
                "map"
            )));
    }

    @Test
    public void toCSVWithSchema() throws IOException {
        Schema schema = SchemaBuilder.struct().field("c1", Schema.STRING_SCHEMA).build();
        assertEquals("", new ValueWithSchema(createRecord(schema, "asd")).toCSV(Collections.emptyList()));
        assertEquals("\\N", new ValueWithSchema(createRecord(schema,
            new Struct(schema).put("c1", "asd"))
        ).toCSV(Collections.singletonList("c2")));

        schema = SchemaBuilder.bool().build();
        assertEquals("1", new ValueWithSchema(createRecord(schema, true)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.int8().build();
        assertEquals("10", new ValueWithSchema(createRecord(schema, (byte)10)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.int16().build();
        assertEquals("10", new ValueWithSchema(createRecord(schema, (short)10)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.int32().build();
        assertEquals("10", new ValueWithSchema(createRecord(schema, 10)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.int64().build();
        assertEquals("10", new ValueWithSchema(createRecord(schema, 10L)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.float32().build();
        assertEquals("10.1", new ValueWithSchema(createRecord(schema, 10.1f)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.float64().build();
        assertEquals("10.1", new ValueWithSchema(createRecord(schema, 10.1d)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.string().build();
        assertEquals("asd", new ValueWithSchema(createRecord(schema, "asd")).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.bytes().build();
        byte[] bytes = "asd".getBytes(StandardCharsets.UTF_8);
        assertEquals("asd", new ValueWithSchema(createRecord(schema, bytes)).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.array(SchemaBuilder.string().build()).build();
        assertEquals("[\"asd\",\"bcd\"]", new ValueWithSchema(createRecord(schema, Arrays.asList("asd", "bcd"))).toCSV(Collections.singletonList("data")));
        schema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.int32().build()).build();
        Map<String, Integer> mp = new HashMap<>();
        mp.put("c1", 1);
        assertEquals("[{\"key\":\"c1\",\"value\":1}]", new ValueWithSchema(createRecord(schema, mp)).toCSV(Collections.singletonList("data")));

        Schema nestedStructSchema = SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA);
        schema = SchemaBuilder.struct()
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.int32().build()).build())
            .field("struct", nestedStructSchema);

        assertEquals("1\t10\t10\t10\t10\t10.1\t10.1\tasd\tasd\t[\"asd\",\"bcd\"]\t[{\"key\":\"c1\",\"value\":1}]\t{\"c1\":\"v1\"}", new ValueWithSchema(createRecord(schema, new Struct(schema)
            .put("bool", true)
            .put("int8", (byte)10)
            .put("int16", (short)10)
            .put("int32", 10)
            .put("int64", 10L)
            .put("float32", 10.1f)
            .put("float64", 10.1d)
            .put("string", "asd")
            .put("bytes", "asd".getBytes(StandardCharsets.UTF_8))
            .put("array", Arrays.asList("asd", "bcd"))
            .put("map", mp)
            .put("struct", new Struct(nestedStructSchema)
                .put("c1", "v1"))
        ))
            .toCSV(Arrays.asList(
                "bool",
                "int8",
                "int16",
                "int32",
                "int64",
                "float32",
                "float64",
                "string",
                "bytes",
                "array",
                "map",
                "struct"
            )));
    }

    @Test
    public void mapColumnsToCSV() throws IOException {
        Schema nestedStructSchema = SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA);
        Schema schema = SchemaBuilder.struct()
            .field("f1", Schema.BOOLEAN_SCHEMA)
            .field("f2", Schema.INT8_SCHEMA)
            .field("f3", nestedStructSchema);

        assertEquals("1\tv1\t\\N", new ValueWithSchema(createRecord(schema, new Struct(schema)
            .put("f1", true)
            .put("f2", (byte)10)
            .put("f3", new Struct(nestedStructSchema)
                .put("c1", "v1"))
        ))
            .mapColumnsToCSV(Arrays.asList(
                new ColumnMapping("c1", "f1"),
                new ColumnMapping("c2", "f3.c1"),
                new ColumnMapping("c3", "f10")
            )));
    }

    @Test
    public void toSqlValuePreservesTypes() throws IOException {
        assertEquals((byte) 1, new ValueWithSchema(createRecord(null, true)).toSqlValue());
        assertEquals((byte) 0, new ValueWithSchema(createRecord(Schema.BOOLEAN_SCHEMA, false)).toSqlValue());
        assertEquals((byte) 10, new ValueWithSchema(createRecord(Schema.INT8_SCHEMA, (byte) 10)).toSqlValue());
        assertEquals((short) 10, new ValueWithSchema(createRecord(Schema.INT16_SCHEMA, (short) 10)).toSqlValue());
        assertEquals(10, new ValueWithSchema(createRecord(Schema.INT32_SCHEMA, 10)).toSqlValue());
        assertEquals(10L, new ValueWithSchema(createRecord(Schema.INT64_SCHEMA, 10L)).toSqlValue());
        assertEquals(10.5f, new ValueWithSchema(createRecord(Schema.FLOAT32_SCHEMA, 10.5f)).toSqlValue());
        assertEquals(10.5d, new ValueWithSchema(createRecord(Schema.FLOAT64_SCHEMA, 10.5d)).toSqlValue());
        assertNull(new ValueWithSchema(createRecord(Schema.OPTIONAL_STRING_SCHEMA, null)).toSqlValue());

        assertEquals("a\t\nb\\c", new ValueWithSchema(createRecord(Schema.STRING_SCHEMA, "a\t\nb\\c")).toSqlValue());

        byte[] bytes = new byte[] {0, 1, (byte) 255};
        assertArrayEquals(bytes, (byte[]) new ValueWithSchema(createRecord(Schema.BYTES_SCHEMA, bytes)).toSqlValue());
        assertArrayEquals(bytes, (byte[]) new ValueWithSchema(createRecord(null, bytes)).toSqlValue());

        assertEquals("[\"asd\",\"bcd\"]", new ValueWithSchema(createRecord(
                SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
                Arrays.asList("asd", "bcd"))).toSqlValue());

        Map<String, Integer> mp = new HashMap<>();
        mp.put("c1", 1);
        assertEquals("[{\"key\":\"c1\",\"value\":1}]", new ValueWithSchema(createRecord(
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), mp)).toSqlValue());

        Schema nested = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        assertEquals("{\"city\":\"Kyiv\"}", new ValueWithSchema(createRecord(nested,
                new Struct(nested).put("city", "Kyiv"))).toSqlValue());

        assertEquals("[\"asd\",\"bcd\"]", new ValueWithSchema(createRecord(null, Arrays.asList("asd", "bcd"))).toSqlValue());
        assertEquals("{\"k\":\"v\"}", new ValueWithSchema(createRecord(null, Collections.singletonMap("k", "v"))).toSqlValue());
    }

    @Test
    public void toSqlValuesUsesColumnOrderAndMappings() throws IOException {
        Schema nested = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("flag", Schema.BOOLEAN_SCHEMA)
                .field("nested", nested)
                .build();
        Struct value = new Struct(schema)
                .put("id", 7)
                .put("flag", true)
                .put("nested", new Struct(nested).put("city", "Kyiv"));

        List<Object> values = new ValueWithSchema(createRecord(schema, value))
                .toSqlValues(Arrays.asList("flag", "id", "missing"), null);
        assertEquals(Arrays.asList((byte) 1, 7, null), values);

        List<Object> mapped = new ValueWithSchema(createRecord(schema, value)).toSqlValues(null, Arrays.asList(
                new ColumnMapping("c1", "nested.city"),
                new ColumnMapping("c2", "flag"),
                new ColumnMapping("c3", "absent")
        ));
        assertEquals(Arrays.asList("Kyiv", (byte) 1, null), mapped);

        assertEquals(Collections.singletonList(10),
                new ValueWithSchema(createRecord(Schema.INT32_SCHEMA, 10))
                        .toSqlValues(Collections.singletonList("data"), null));
    }
}
