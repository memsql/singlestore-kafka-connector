package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.*;

import static com.memsql.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.fail;

public class TableNameTest extends IntegrationBase {

    @Test
    public void withoutTableName() {
        String topicName = "topicName";

        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA);
        SinkRecord record = createRecord(schema, new Struct(schema).put("id", 1), topicName);

        testTableName(new HashMap<>(), record, topicName);
    }

    @Test
    public void withTableName() {
        String tableName = "tableName";
        String topicName = "topicName";

        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA);
        SinkRecord record = createRecord(schema, new Struct(schema).put("id", 1), topicName);

        testTableName(new HashMap<String, String>() {{
            put("memsql.tableName." + topicName, tableName);
        }}, record, tableName);
    }

    public void testTableName(Map<String, String> keys, SinkRecord record, String tableExpected) {
        try {
            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
            keys.forEach(props::put);

            executeQuery("DROP TABLE IF EXISTS testdb." + tableExpected);

            MemSQLSinkTask task = new MemSQLSinkTask();
            task.start(props);
            task.put(Collections.singleton(record));
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new MemSQLSinkConfig(props), "SELECT * from testdb." + tableExpected);
            if (res.next()) {
                assert(res.getInt("id") == 1);
            } else {
                fail("Table " + tableExpected + " should contains 1 row -");
            }

        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
