package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.SQLHelper;
import com.singlestore.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableKeyTest extends IntegrationBase {

    public static String defaultTableType = "";

    // This is needed because key types may mismatch for columnstore tables, please see DB-50840 and
    // https://docs.singlestore.com/db/v7.5/en/create-your-database/physical-database-schema-design/procedures-for-physical-database-schema-design/creating-a-columnstore-table.html
    @BeforeClass
    public static void ensureTableType() throws SQLException {
        boolean supportDefaultTableTypeVariable = true;
        try {
            ResultSet res = executeQueryWithResultSet("SELECT @@default_table_type as default_table_type");
            res.next();
            defaultTableType = res.getString("default_table_type");
        } catch (Exception e) {
            supportDefaultTableTypeVariable = false;
        }

        if (supportDefaultTableTypeVariable) {
            executeQuery("SET GLOBAL default_table_type=rowstore");
        }
    }

    @AfterClass
    public static void restoreDefaultTableType() throws SQLException {
        if (defaultTableType.isEmpty()) {
            return;
        }

        executeQuery("SET GLOBAL default_table_type = " + defaultTableType);
    }


    public void testKey(Map<String, String> keys, TableKey.Type type) {
        try {
            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
            keys.forEach(props::put);

            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA);
            records.add(createRecord(schema, new Struct(schema).put("id", 1), "keys"));

            executeQuery("DROP TABLE IF EXISTS testdb.keys");

            SingleStoreSinkTask task = new SingleStoreSinkTask();
            task.start(props);
            task.put(records);
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new SingleStoreSinkConfig(props), "EXPLAIN testdb.keys");

            while(res.next()) {
                switch(type) {
                    case PRIMARY:
                    case COLUMNSTORE:
                    case SHARD:
                        assertEquals(res.getString("Key"), "PRI");
                        break;
                    case KEY:
                        assertEquals(res.getString("Key"), "MUL");
                        break;
                    case UNIQUE:
                        assertEquals(res.getString("Key"), "UNI");
                        break;
                }
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void primary() {
        testKey(new HashMap<String, String>() {{
            put("tableKey.primary.name", "id");
        }}, TableKey.Type.PRIMARY);

        testKey(new HashMap<String, String>() {{
            put("tableKey.primary", "id");
        }}, TableKey.Type.PRIMARY);
    }

    @Test
    public void columnstore() {
        testKey(new HashMap<String, String>() {{
            put("tableKey.columnstore.name", "id");
        }}, TableKey.Type.COLUMNSTORE);

        testKey(new HashMap<String, String>() {{
            put("tableKey.columnstore", "id");
        }}, TableKey.Type.COLUMNSTORE);
    }

    @Test
    public void unique() {
        testKey(new HashMap<String, String>() {{
            put("tableKey.unique.name", "id");
            put("tableKey.shard", "id");
        }}, TableKey.Type.UNIQUE);

        testKey(new HashMap<String, String>() {{
            put("tableKey.unique", "id");
            put("tableKey.shard", "id");
        }}, TableKey.Type.UNIQUE);
    }

    @Test
    public void shard() {
        testKey(new HashMap<String, String>() {{
            put("tableKey.shard.name", "id");
        }}, TableKey.Type.SHARD);

        testKey(new HashMap<String, String>() {{
            put("tableKey.shard", "id");
        }}, TableKey.Type.SHARD);
    }

    @Test
    public void key() {
        testKey(new HashMap<String, String>() {{
            put("tableKey.key.name", "id");
        }}, TableKey.Type.KEY);

        testKey(new HashMap<String, String>() {{
            put("tableKey.key", "id");
        }}, TableKey.Type.KEY);
    }
}
