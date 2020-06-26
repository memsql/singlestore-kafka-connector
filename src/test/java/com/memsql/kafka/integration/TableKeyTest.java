package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.SQLHelper;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.memsql.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableKeyTest extends IntegrationBase {
    public void testKey(Map<String, String> keys, TableKey.Type type) {
        try {
            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
            keys.forEach(props::put);

            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA);
            records.add(createRecord(schema, new Struct(schema).put("id", 1), "keys"));

            executeQuery("DROP TABLE IF EXISTS testdb.keys");

            MemSQLSinkTask task = new MemSQLSinkTask();
            task.start(props);
            task.put(records);
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new MemSQLSinkConfig(props), "EXPLAIN testdb.keys");

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
