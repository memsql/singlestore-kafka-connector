package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.vdurmont.semver4j.Semver;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OnDuplicateKeyUpdateTest extends IntegrationBase {

    private static final String UPDATE_DATA = "`data` = VALUES(`data`)";
    private static String rowstoreTableKind;

    @Test
    public void partialUpdateLeavesOtherColumnsUnchanged() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_partial";
        Map<String, String> props = props(UPDATE_DATA);

        executeQuery("DROP TABLE IF EXISTS testdb.odku_partial");
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT, extra TEXT"));
        executeQuery("INSERT INTO testdb.odku_partial VALUES (1, 'a', 'original', 'set-by-db')");
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

        executeQuery("DROP TABLE IF EXISTS testdb.odku_mixed");
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"));
        executeQuery("INSERT INTO testdb.odku_mixed VALUES (1, 'v1', 'k1'), (2, 'v2', 'k2')");

        put(props, Arrays.asList(
                row(schema, 1, "u1", "changed", table),
                row(schema, 2, "u2", "changed", table),
                row(schema, 3, "u3", "new-keep", table)
        ), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_mixed ORDER BY id");
        for (int id = 1; id <= 2; id++) {
            assertTrue(rs.next());
            assertEquals(id, rs.getInt("id"));
            assertEquals("u" + id, rs.getString("data"));
            assertEquals("k" + id, rs.getString("keep"));
        }
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("u3", rs.getString("data"));
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

        byte[] escapedPayload = "binary\tvalue\nwith\\\\escapes".getBytes(StandardCharsets.UTF_8);
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
                .put("payload", escapedPayload)
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
        assertArrayEquals(escapedPayload, rs.getBytes("payload"));
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

        executeQuery("DROP TABLE IF EXISTS testdb.odku_schemaless");
        executeQuery(rowstore(table, "id INT PRIMARY KEY, name TEXT, keep TEXT, payload VARBINARY(256), meta JSON"));
        executeQuery("INSERT INTO testdb.odku_schemaless VALUES (1, 'a', 'stay', X'0001FF', '{\"k\":\"v\"}')");

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

        executeQuery("DROP TABLE IF EXISTS testdb.odku_mapping");
        executeQuery(rowstore(table, "id INT PRIMARY KEY, c_name TEXT, c_keep TEXT"));
        executeQuery("INSERT INTO testdb.odku_mapping VALUES (1, 'Kyiv', 'stay')");

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
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_UPDATE, "true");
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
    public void failedBatchRollsBackMetadataAndRetrySucceeds() throws SQLException {
        executeQuery("DROP TABLE IF EXISTS testdb.`kafka_connect_transaction_metadata`");
        String table = "odku_rollback_meta";
        executeQuery("DROP TABLE IF EXISTS testdb." + table);
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data JSON"));

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .build();
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "true");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_UPDATE, "true");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL, "this is not valid SQL");

        SingleStoreSinkTask failingTask = new SingleStoreSinkTask();
        failingTask.initialize(new WorkerSinkTaskContext(null, null, null));
        failingTask.start(props);
        List<SinkRecord> records = Arrays.asList(
                new SinkRecord(table, 0, null, null, schema,
                        new Struct(schema).put("id", 1).put("data", "{\"a\":1}"), 0),
                new SinkRecord(table, 0, null, null, schema,
                        new Struct(schema).put("id", 2).put("data", "{\"b\":2}"), 1));
        assertThrows(RetriableException.class, () -> failingTask.put(records));
        failingTask.stop();

        ResultSet rs = executeQueryWithResultSet("SELECT COUNT(*) FROM testdb.odku_rollback_meta");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        ResultSet meta = executeQueryWithResultSet(
                "SELECT COUNT(*) FROM testdb.kafka_connect_transaction_metadata WHERE id = 'odku_rollback_meta-0-0'");
        assertTrue(meta.next());
        assertEquals(0, meta.getInt(1));

        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL, UPDATE_DATA);
        SingleStoreSinkTask retryTask = new SingleStoreSinkTask();
        retryTask.initialize(new WorkerSinkTaskContext(null, null, null));
        retryTask.start(props);
        retryTask.put(records);
        retryTask.stop();

        rs = executeQueryWithResultSet("SELECT COUNT(*) FROM testdb.odku_rollback_meta");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        meta = executeQueryWithResultSet(
                "SELECT count FROM testdb.kafka_connect_transaction_metadata WHERE id = 'odku_rollback_meta-0-0'");
        assertTrue(meta.next());
        assertEquals(2, meta.getInt("count"));
        assertFalse(meta.next());
    }

    @Test
    public void multiTableWrite() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("tableId", Schema.STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        Map<String, String> props = props(UPDATE_DATA);
        props.put("singlestore.recordToTable.mappingField", "tableId");
        props.put("singlestore.recordToTable.mapping.t1", "odku_multi1");
        props.put("singlestore.recordToTable.mapping.t2", "odku_multi2");
        String columns = "tableId TEXT, id INT PRIMARY KEY, data TEXT, keep TEXT";
        executeQuery("DROP TABLE IF EXISTS testdb.odku_multi1");
        executeQuery("DROP TABLE IF EXISTS testdb.odku_multi2");
        executeQuery(rowstore("odku_multi1", columns));
        executeQuery(rowstore("odku_multi2", columns));
        executeQuery("INSERT INTO testdb.odku_multi1 VALUES ('t1', 1, 'a', 'keep1')");
        executeQuery("INSERT INTO testdb.odku_multi2 VALUES ('t2', 1, 'b', 'keep2')");

        put(props, Arrays.asList(
                multiTableRow(schema, "t2", 1, "b2", "changed"),
                multiTableRow(schema, "t1", 1, "a2", "changed"),
                multiTableRow(schema, "t1", 2, "new", "fresh"),
                multiTableRow(schema, "unmapped", 3, "skip", "skip")
        ), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_multi1 ORDER BY id");
        assertTrue(rs.next());
        assertEquals("a2", rs.getString("data"));
        assertEquals("keep1", rs.getString("keep"));
        assertTrue(rs.next());
        assertEquals("new", rs.getString("data"));
        assertEquals("fresh", rs.getString("keep"));
        assertFalse(rs.next());

        rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_multi2");
        assertTrue(rs.next());
        assertEquals("b2", rs.getString("data"));
        assertEquals("keep2", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void filterSkipsInsertsAndUpdates() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_filter_batches";
        Map<String, String> props = props(UPDATE_DATA);
        props.put(SingleStoreSinkConfig.FILTER, "id % 2 = 0");

        executeQuery("DROP TABLE IF EXISTS testdb.odku_filter_batches");
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data TEXT, keep TEXT"));
        executeQuery("INSERT INTO testdb.odku_filter_batches VALUES "
                + "(1, 'old-filtered', 'stay-filtered'), (2, 'old', 'stay')");

        List<SinkRecord> records = new ArrayList<>();
        for (int id = 1; id <= 5; id++) {
            records.add(row(schema, id, "v" + id, "k" + id, table));
        }
        put(props, records, null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT id, data, keep FROM testdb.odku_filter_batches ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("old-filtered", rs.getString("data"));
        assertEquals("stay-filtered", rs.getString("keep"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("v2", rs.getString("data"));
        assertEquals("stay", rs.getString("keep"));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt("id"));
        assertEquals("v4", rs.getString("data"));
        assertEquals("k4", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void primitiveRecords() throws SQLException {
        String table = "odku_primitive";
        Map<String, String> props = props("`hits` = `hits` + 1");
        executeQuery("DROP TABLE IF EXISTS testdb." + table);
        executeQuery(rowstore(table, "data VARCHAR(10) PRIMARY KEY, hits INT DEFAULT 0"));

        put(props, Arrays.asList(
                createRecord(null, "a", table),
                createRecord(null, "b", table),
                createRecord(null, "a", table),
                createRecord(Schema.STRING_SCHEMA, "b", table)
        ), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT data, hits FROM testdb.odku_primitive ORDER BY data");
        assertTrue(rs.next());
        assertEquals("a", rs.getString("data"));
        assertEquals(1, rs.getInt("hits"));
        assertTrue(rs.next());
        assertEquals("b", rs.getString("data"));
        assertEquals(1, rs.getInt("hits"));
        assertFalse(rs.next());
    }

    @Test
    public void expressionAssignment() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("counter", Schema.INT32_SCHEMA)
                .build();
        String table = "odku_expression";
        Map<String, String> props = props("`counter` = `counter` + 1");
        SinkRecord record = createRecord(schema, new Struct(schema).put("id", 1).put("counter", 1), table);

        put(props, Arrays.asList(record, record, record),
                rowstore(table, "id INT PRIMARY KEY, counter INT"), true);

        ResultSet rs = executeQueryWithResultSet("SELECT counter FROM testdb.odku_expression");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("counter"));
        assertFalse(rs.next());
    }

    @Test
    public void schemalessMissingFieldWritesNull() throws SQLException {
        String table = "odku_missing_field";
        Map<String, String> props = props("`name` = VALUES(`name`)");

        Map<Object, Object> existing = new LinkedHashMap<>();
        existing.put("id", 1);
        existing.put("name", "a");

        Map<Object, Object> first = new LinkedHashMap<>();
        first.put("id", 2);
        first.put("name", "b");
        put(props, Arrays.asList(
                createRecord(null, existing, table),
                createRecord(null, first, table),
                createRecord(null, Collections.singletonMap("id", 1), table),
                createRecord(null, Collections.singletonMap("id", 3), table)),
                rowstore(table, "id INT PRIMARY KEY, name TEXT DEFAULT 'default'"), true);

        ResultSet rs = executeQueryWithResultSet("SELECT id, name FROM testdb.odku_missing_field ORDER BY id");
        assertTrue(rs.next());
        assertNull(rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("b", rs.getString("name"));
        assertTrue(rs.next());
        assertNull(rs.getString("name"));
        assertFalse(rs.next());
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

    @Test
    public void autoIncrementColumnKeepsExistingValueAndGeneratesNewOnes() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("biz", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        String table = "odku_auto_increment";
        Map<String, String> props = props(UPDATE_DATA);

        executeQuery("DROP TABLE IF EXISTS testdb." + table);
        executeQuery(rowstore(table,
                "id BIGINT AUTO_INCREMENT, biz INT NOT NULL, data TEXT, keep TEXT, PRIMARY KEY (biz), KEY (id)"));
        executeQuery("INSERT INTO testdb." + table + " (biz, data, keep) VALUES (1, 'a', 'stay')");
        ResultSet rs = executeQueryWithResultSet("SELECT id FROM testdb." + table + " WHERE biz = 1");
        assertTrue(rs.next());
        long existingId = rs.getLong("id");

        put(props, Arrays.asList(
                createRecord(schema, new Struct(schema).put("biz", 1).put("data", "b").put("keep", "changed"), table),
                createRecord(schema, new Struct(schema).put("biz", 2).put("data", "c").put("keep", "new"), table)
        ), null, false);

        rs = executeQueryWithResultSet("SELECT id, biz, data, keep FROM testdb." + table + " ORDER BY biz");
        assertTrue(rs.next());
        assertEquals(existingId, rs.getLong("id"));
        assertEquals("b", rs.getString("data"));
        assertEquals("stay", rs.getString("keep"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("biz"));
        assertTrue(rs.getLong("id") > 0);
        assertTrue(rs.getLong("id") != existingId);
        assertEquals("c", rs.getString("data"));
        assertEquals("new", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void perTableAssignmentLists() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("tableId", Schema.STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("keep", Schema.STRING_SCHEMA)
                .build();
        Map<String, String> props = new HashMap<>();
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_UPDATE, "true");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL + ".odku_per_table1", "`data` = VALUES(`data`)");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL + ".odku_per_table2", "`keep` = VALUES(`keep`)");
        props.put("singlestore.recordToTable.mappingField", "tableId");
        props.put("singlestore.recordToTable.mapping.t1", "odku_per_table1");
        props.put("singlestore.recordToTable.mapping.t2", "odku_per_table2");
        String columns = "tableId TEXT, id INT PRIMARY KEY, data TEXT, keep TEXT";
        executeQuery("DROP TABLE IF EXISTS testdb.odku_per_table1");
        executeQuery("DROP TABLE IF EXISTS testdb.odku_per_table2");
        executeQuery(rowstore("odku_per_table1", columns));
        executeQuery(rowstore("odku_per_table2", columns));
        executeQuery("INSERT INTO testdb.odku_per_table1 VALUES ('t1', 1, 'a', 'keep1')");
        executeQuery("INSERT INTO testdb.odku_per_table2 VALUES ('t2', 1, 'b', 'keep2')");

        put(props, Arrays.asList(
                multiTableRow(schema, "t1", 1, "a2", "changed"),
                multiTableRow(schema, "t2", 1, "b2", "changed")
        ), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT data, keep FROM testdb.odku_per_table1");
        assertTrue(rs.next());
        assertEquals("a2", rs.getString("data"));
        assertEquals("keep1", rs.getString("keep"));
        assertFalse(rs.next());

        rs = executeQueryWithResultSet("SELECT data, keep FROM testdb.odku_per_table2");
        assertTrue(rs.next());
        assertEquals("b", rs.getString("data"));
        assertEquals("changed", rs.getString("keep"));
        assertFalse(rs.next());
    }

    @Test
    public void mixedCaseOrderColumnUsesArrivalOrder() throws SQLException {
        String order = "_SINGLESTORE_KAFKA_ORDER";
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field(order, Schema.INT32_SCHEMA)
                .build();
        String table = "odku_order_case";
        Map<String, String> props = props("`data` = VALUES(`data`)");
        executeQuery("DROP TABLE IF EXISTS testdb." + table);
        executeQuery(rowstore(table, "id INT PRIMARY KEY, data TEXT, `" + order + "` INT"));
        executeQuery("INSERT INTO testdb." + table + " VALUES (1, 'old', 0)");

        Struct first = new Struct(schema).put("id", 1).put("data", "first").put(order, 2);
        Struct second = new Struct(schema).put("id", 1).put("data", "second").put(order, 1);
        put(props, Arrays.asList(
                createRecord(schema, first, table),
                createRecord(schema, second, table)
        ), null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT data, `" + order + "` FROM testdb." + table);
        assertTrue(rs.next());
        assertEquals("second", rs.getString("data"));
        assertEquals(0, rs.getInt(order));
        assertFalse(rs.next());
    }

    private static Map<String, String> props(String onDuplicateKeySQL) {
        Map<String, String> props = new HashMap<>();
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_UPDATE, "true");
        props.put(SingleStoreSinkConfig.ON_DUPLICATE_KEY_SQL, onDuplicateKeySQL);
        return props;
    }

    private static SinkRecord row(Schema schema, int id, String data, String keep, String topic) {
        return createRecord(schema, new Struct(schema)
                .put("id", id)
                .put("data", data)
                .put("keep", keep), topic);
    }

    private static SinkRecord multiTableRow(Schema schema, String tableId, int id, String data, String keep) {
        return createRecord(schema, new Struct(schema)
                .put("tableId", tableId)
                .put("id", id)
                .put("data", data)
                .put("keep", keep), "odku_multi");
    }

    private static Struct struct(Schema schema) {
        return new Struct(schema);
    }

    private static String rowstore(String table, String columns) throws SQLException {
        if (rowstoreTableKind == null) {
            rowstoreTableKind = getSingleStoreVersion().isGreaterThanOrEqualTo(new Semver("7.3.0"))
                    ? "ROWSTORE TABLE"
                    : "TABLE";
        }
        return "CREATE " + rowstoreTableKind + " testdb." + table + " (" + columns + ")";
    }

    private static void assertNullNumber(ResultSet rs, String column) throws SQLException {
        rs.getObject(column);
        assertTrue(column + " should be null", rs.wasNull());
    }
}
