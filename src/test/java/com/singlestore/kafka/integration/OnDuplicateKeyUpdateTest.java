package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.vdurmont.semver4j.Semver;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OnDuplicateKeyUpdateTest extends IntegrationBase {

    private static final String UPDATE_DATA = "`data` = VALUES(`data`)";

    @Test
    public void partialUpdateLeavesOtherColumnsUnchanged() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_partial";
        Map<String, String> props = props(UPDATE_DATA);

        put(props, Collections.singletonList(row(schema, 1, "a", "original", table)),
                rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT, extra TEXT"), true);
        executeQuery("UPDATE testdb.odku_partial SET extra = 'set-by-db' WHERE id = 1");

        put(props, Collections.singletonList(row(schema, 1, "b", "replaced", table)), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.odku_partial");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("b", rs.getString("data"));
        assertEquals("original", rs.getString("keep"));
        assertEquals("set-by-db", rs.getString("extra"));
        assertFalse(rs.next());
    }

    @Test
    public void insertsNewRowAndUpdatesExistingRowInOneBatch() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_mixed";
        Map<String, String> props = props(UPDATE_DATA);
        props.put(SingleStoreSinkConfig.INSERT_BATCH_SIZE, "2");

        List<SinkRecord> initial = new ArrayList<>();
        for (int id = 1; id <= 5; id++) {
            initial.add(row(schema, id, "v" + id, "k" + id, table));
        }
        put(props, initial, rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"), true);

        List<SinkRecord> next = new ArrayList<>();
        for (int id = 1; id <= 5; id++) {
            next.add(row(schema, id, "u" + id, "changed", table));
        }
        next.add(row(schema, 6, "u6", "new-keep", table));
        put(props, next, null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_mixed ORDER BY id");
        for (int id = 1; id <= 5; id++) {
            assertTrue(rs.next());
            assertEquals(id, rs.getInt("id"));
            assertEquals("u" + id, rs.getString("data"));
            assertEquals("k" + id, rs.getString("keep"));
        }
        assertTrue(rs.next());
        assertEquals(6, rs.getInt("id"));
        assertEquals("u6", rs.getString("data"));
        assertEquals("new-keep", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void duplicateKeyInSameStatementUsesLastInsertedData() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_same_batch";
        Map<String, String> props = props(UPDATE_DATA);

        put(props, Arrays.asList(
                row(schema, 1, "a", "first-keep", table),
                row(schema, 1, "b", "second-keep", table)
        ), rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"), true);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.odku_same_batch");
        assertTrue(rs.next());
        assertEquals("b", rs.getString("data"));
        assertEquals("first-keep", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void allDataTypesRoundTrip() throws SQLException {
        Schema nested = SchemaBuilder.struct().field("city", Schema.OPTIONAL_STRING_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("i8", Schema.OPTIONAL_INT8_SCHEMA)
                .field("i16", Schema.OPTIONAL_INT16_SCHEMA)
                .field("i32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("i64", Schema.OPTIONAL_INT64_SCHEMA)
                .field("f32", Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("f64", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("flag", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("payload", Schema.OPTIONAL_BYTES_SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
                .field("nested", nested)
                .field("note", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_types";
        Map<String, String> props = props("`i8` = VALUES(`i8`), `i16` = VALUES(`i16`), `i32` = VALUES(`i32`), "
                + "`i64` = VALUES(`i64`), `f32` = VALUES(`f32`), `f64` = VALUES(`f64`), `flag` = VALUES(`flag`), "
                + "`name` = VALUES(`name`), `payload` = VALUES(`payload`), `tags` = VALUES(`tags`), "
                + "`attrs` = VALUES(`attrs`), `nested` = VALUES(`nested`)");

        byte[] allBytes = new byte[256];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) (i + Byte.MIN_VALUE);
        }
        byte[] typicalPayload = new byte[] {1, 2, 3};
        Map<String, Integer> attrs = new HashMap<>();
        attrs.put("c1", 1);

        Struct typical = struct(schema)
                .put("id", 1)
                .put("i8", (byte) 8)
                .put("i16", (short) 16)
                .put("i32", 32)
                .put("i64", 64L)
                .put("f32", 1.5f)
                .put("f64", 2.5d)
                .put("flag", true)
                .put("name", "first")
                .put("payload", typicalPayload)
                .put("tags", new ArrayList<>(Arrays.asList("a", "b")))
                .put("attrs", attrs)
                .put("nested", new Struct(nested).put("city", "Kyiv"))
                .put("note", "keep-me");

        Struct extreme = struct(schema)
                .put("id", 2)
                .put("i8", Byte.MIN_VALUE)
                .put("i16", Short.MIN_VALUE)
                .put("i32", Integer.MIN_VALUE)
                .put("i64", Long.MIN_VALUE)
                .put("f32", Float.MAX_VALUE)
                .put("f64", Double.MAX_VALUE)
                .put("flag", true)
                .put("name", "Київ\t\"quote\"\\")
                .put("payload", allBytes)
                .put("tags", new ArrayList<>(Collections.singletonList("b\nc")))
                .put("attrs", new HashMap<String, Integer>())
                .put("nested", new Struct(nested).put("city", ""))
                .put("note", "keep-extreme");

        put(props, Arrays.asList(
                createRecord(schema, typical, table),
                createRecord(schema, extreme, table)
        ), rowstore(table, "id INT NOT NULL PRIMARY KEY, i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, "
                + "f32 FLOAT, f64 DOUBLE, flag TINYINT, name TEXT, payload VARBINARY(1024), "
                + "tags JSON, attrs JSON, nested JSON, note TEXT NOT NULL"), true);

        Map<String, Integer> updatedAttrs = new HashMap<>();
        updatedAttrs.put("c1", 9);
        Struct updated = struct(schema)
                .put("id", 1)
                .put("i8", Byte.MAX_VALUE)
                .put("i16", Short.MAX_VALUE)
                .put("i32", Integer.MAX_VALUE)
                .put("i64", Long.MAX_VALUE)
                .put("f32", -1.25f)
                .put("f64", -3.5d)
                .put("flag", false)
                .put("name", null)
                .put("payload", new byte[] {0})
                .put("tags", new ArrayList<String>())
                .put("attrs", updatedAttrs)
                .put("nested", null)
                .put("note", "should-stay");

        Struct nulls = struct(schema)
                .put("id", 3)
                .put("i8", null)
                .put("i16", null)
                .put("i32", null)
                .put("i64", null)
                .put("f32", null)
                .put("f64", null)
                .put("flag", null)
                .put("name", null)
                .put("payload", null)
                .put("tags", null)
                .put("attrs", null)
                .put("nested", null)
                .put("note", "null-row");

        put(props, Arrays.asList(
                createRecord(schema, updated, table),
                createRecord(schema, nulls, table)
        ), null, false);

        // FLOAT is returned as a rounded decimal string, so read it back as DOUBLE.
        ResultSet rs = executeQueryWithResultSet(
                "SELECT *, `f32` :> DOUBLE AS f32_exact FROM testdb.odku_types ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals(Byte.MAX_VALUE, rs.getByte("i8"));
        assertEquals(Short.MAX_VALUE, rs.getShort("i16"));
        assertEquals(Integer.MAX_VALUE, rs.getInt("i32"));
        assertEquals(Long.MAX_VALUE, rs.getLong("i64"));
        assertEquals(-1.25d, rs.getDouble("f32_exact"), 0d);
        assertEquals(-3.5d, rs.getDouble("f64"), 0d);
        assertEquals(0, rs.getInt("flag"));
        assertNull(rs.getString("name"));
        assertArrayEquals(new byte[] {0}, rs.getBytes("payload"));
        assertEquals("[]", rs.getString("tags"));
        assertEquals("[{\"key\":\"c1\",\"value\":9}]", rs.getString("attrs"));
        assertNull(rs.getString("nested"));
        assertEquals("keep-me", rs.getString("note"));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals(Byte.MIN_VALUE, rs.getByte("i8"));
        assertEquals(Short.MIN_VALUE, rs.getShort("i16"));
        assertEquals(Integer.MIN_VALUE, rs.getInt("i32"));
        assertEquals(Long.MIN_VALUE, rs.getLong("i64"));
        assertEquals((double) Float.MAX_VALUE, rs.getDouble("f32_exact"), 0d);
        assertEquals(Double.MAX_VALUE, rs.getDouble("f64"), 0d);
        assertEquals(1, rs.getInt("flag"));
        assertEquals("Київ\t\"quote\"\\", rs.getString("name"));
        assertArrayEquals(allBytes, rs.getBytes("payload"));
        assertEquals("[\"b\\nc\"]", rs.getString("tags"));
        assertEquals("[]", rs.getString("attrs"));
        assertEquals("{\"city\":\"\"}", rs.getString("nested"));
        assertEquals("keep-extreme", rs.getString("note"));

        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertNullNumber(rs, "i8");
        assertNullNumber(rs, "i16");
        assertNullNumber(rs, "i32");
        assertNullNumber(rs, "i64");
        assertNullNumber(rs, "f32");
        assertNullNumber(rs, "f64");
        assertNullNumber(rs, "flag");
        assertNull(rs.getString("name"));
        assertNull(rs.getBytes("payload"));
        assertNull(rs.getString("tags"));
        assertNull(rs.getString("attrs"));
        assertNull(rs.getString("nested"));
        assertEquals("null-row", rs.getString("note"));
        assertFalse(rs.next());
    }

    @Test
    public void schemalessValuesRoundTrip() throws SQLException {
        String table = "odku_schemaless";
        Map<String, String> props = props("`name` = VALUES(`name`), `payload` = VALUES(`payload`), `meta` = VALUES(`meta`)");

        Map<Object, Object> first = new LinkedHashMap<>();
        first.put("id", 1);
        first.put("name", "a");
        first.put("keep", "stay");
        first.put("payload", new byte[] {0, 1, (byte) 255});
        first.put("meta", Collections.singletonMap("k", "v"));

        put(props, Collections.singletonList(createRecord(null, first, table)),
                rowstore(table, "id INT PRIMARY KEY, name TEXT, keep TEXT, payload VARBINARY(256), meta JSON"), true);

        Map<Object, Object> second = new LinkedHashMap<>();
        second.put("id", 1);
        second.put("name", "b\t");
        second.put("keep", "changed");
        second.put("payload", "xyz".getBytes(StandardCharsets.UTF_8));
        second.put("meta", Arrays.asList("x", "y"));

        Map<Object, Object> inserted = new LinkedHashMap<>();
        inserted.put("id", 2);
        inserted.put("name", "c");
        inserted.put("keep", "fresh");
        inserted.put("payload", new byte[0]);
        inserted.put("meta", Collections.singletonMap("n", 1));

        put(props, Arrays.asList(createRecord(null, second, table), createRecord(null, inserted, table)), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.odku_schemaless ORDER BY id");
        assertTrue(rs.next());
        assertEquals("b\t", rs.getString("name"));
        assertEquals("stay", rs.getString("keep"));
        assertArrayEquals("xyz".getBytes(StandardCharsets.UTF_8), rs.getBytes("payload"));
        assertEquals("[\"x\",\"y\"]", rs.getString("meta"));

        assertTrue(rs.next());
        assertEquals("c", rs.getString("name"));
        assertEquals("fresh", rs.getString("keep"));
        assertArrayEquals(new byte[0], rs.getBytes("payload"));
        assertEquals("{\"n\":1}", rs.getString("meta"));
        assertFalse(rs.next());
    }

    @Test
    public void columnMappingPartialUpdate() throws SQLException {
        Schema nested = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .field("nested", nested)
                .build();
        String table = "odku_mapping";
        Map<String, String> props = props("`c_name` = VALUES(`c_name`)");
        props.put("singlestore.columnToField.odku_mapping.id", "id");
        props.put("singlestore.columnToField.odku_mapping.c_name", "nested.city");
        props.put("singlestore.columnToField.odku_mapping.c_keep", "keep");

        Struct first = new Struct(schema)
                .put("id", 1)
                .put("keep", "stay")
                .put("nested", new Struct(nested).put("city", "Kyiv"));
        put(props, Collections.singletonList(createRecord(schema, first, table)),
                rowstore(table, "id INT PRIMARY KEY, c_name TEXT, c_keep TEXT"), true);

        Struct second = new Struct(schema)
                .put("id", 1)
                .put("keep", "changed")
                .put("nested", new Struct(nested).put("city", "Lviv"));
        put(props, Collections.singletonList(createRecord(schema, second, table)), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.odku_mapping");
        assertTrue(rs.next());
        assertEquals("Lviv", rs.getString("c_name"));
        assertEquals("stay", rs.getString("c_keep"));
        assertFalse(rs.next());
    }

    @Test
    public void filterSkipsRowsBeforeUpdate() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_filter";
        Map<String, String> props = props(UPDATE_DATA);
        props.put(SingleStoreSinkConfig.FILTER, "id > 1");

        put(props, Arrays.asList(
                row(schema, 1, "skip", "nope", table),
                row(schema, 2, "a", "stay", table),
                row(schema, 3, "c", "third", table)
        ), rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"), true);

        put(props, Collections.singletonList(row(schema, 2, "b", "changed", table)), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_filter ORDER BY id");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("b", rs.getString("data"));
        assertEquals("stay", rs.getString("keep"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("c", rs.getString("data"));
        assertFalse(rs.next());
    }

    @Test
    public void metadataSkipsReplay() throws SQLException {
        executeQuery("DROP TABLE IF EXISTS testdb.`kafka_connect_transaction_metadata`");
        String table = "odku_metadata";
        executeQuery("DROP TABLE IF EXISTS testdb." + table);
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"));

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "true");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL, UPDATE_DATA);

        Struct first = new Struct(schema).put("id", 1).put("data", "a").put("keep", "keep");
        Struct second = new Struct(schema).put("id", 1).put("data", "b").put("keep", "changed");

        SingleStoreSinkTask task = new SingleStoreSinkTask();
        task.start(props);
        task.put(Collections.singletonList(new SinkRecord(table, 0, null, null, schema, first, 0)));
        task.put(Collections.singletonList(new SinkRecord(table, 0, null, null, schema, second, 0)));
        task.stop();

        ResultSet rs = executeQueryWithResultSet("SELECT data, keep FROM testdb.odku_metadata");
        assertTrue(rs.next());
        assertEquals("a", rs.getString("data"));
        assertEquals("keep", rs.getString("keep"));
        assertFalse(rs.next());

        ResultSet meta = executeQueryWithResultSet(
                "SELECT id, count FROM testdb.kafka_connect_transaction_metadata WHERE id = 'odku_metadata-0-0'");
        assertTrue(meta.next());
        assertEquals(1, meta.getInt("count"));
        assertFalse(meta.next());
    }

    @Test
    public void autoCreatedTablePartialUpdate() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_auto";
        Map<String, String> props = props(UPDATE_DATA);
        props.put("tableKey.primary", "id");

        put(props, Collections.singletonList(row(schema, 1, "a", "stay", table)), null, true);
        put(props, Collections.singletonList(row(schema, 1, "b", "changed", table)), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT data, keep FROM testdb.odku_auto");
        assertTrue(rs.next());
        assertEquals("b", rs.getString("data"));
        assertEquals("stay", rs.getString("keep"));
        assertFalse(rs.next());
    }

    private static Map<String, String> props(String onDuplicateKeySQL) {
        Map<String, String> props = new HashMap<>();
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL, onDuplicateKeySQL);
        return props;
    }

    private static SinkRecord row(Schema schema, int id, String data, String keep, String topic) {
        return createRecord(schema, new Struct(schema)
                .put("id", id)
                .put("data", data)
                .put("keep", keep), topic);
    }

    private static Struct struct(Schema schema) {
        return new Struct(schema);
    }

    private static String rowstore(String table, String columns) throws SQLException {
        String kind = getSingleStoreVersion().isGreaterThanOrEqualTo(new Semver("7.3.0"))
                ? "ROWSTORE TABLE"
                : "TABLE";
        return "CREATE " + kind + " testdb." + table + " (" + columns + ")";
    }

    private static void assertNullNumber(ResultSet rs, String column) throws SQLException {
        rs.getObject(column);
        assertTrue(column + " should be null", rs.wasNull());
    }
}
