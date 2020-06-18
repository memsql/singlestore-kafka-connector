package com.memsql.kafka.sink;

import com.memsql.kafka.utils.DataCompression;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

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
                    put(new Struct(schema).put("name", "Adalbert"), new ArrayList<>(Arrays.asList("Alice", "Bob")));
                }}),
                "[{\"key\":{\"name\":\"Archie\"},\"value\":[\"Bob\",\"Alice\"]},{\"key\":{\"name\":\"Adalbert\"},\"value\":[\"Alice\",\"Bob\"]}]");

        assertEquals(MemSQLDialect.toJSON(SchemaBuilder.array(
                SchemaBuilder.array(Schema.STRING_SCHEMA)
                ),
                new ArrayList<>(Arrays.asList(
                        new ArrayList<>(Arrays.asList("Bob", "Alice")) ,
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
}
