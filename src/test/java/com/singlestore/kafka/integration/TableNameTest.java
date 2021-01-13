package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.*;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
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
            put("singlestore.tableName." + topicName, tableName);
        }}, record, tableName);
    }

    public void testTableName(Map<String, String> keys, SinkRecord record, String tableExpected) {
        try {
            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
            keys.forEach(props::put);

            executeQuery("DROP TABLE IF EXISTS testdb." + tableExpected);

            SingleStoreSinkTask task = new SingleStoreSinkTask();
            task.start(props);
            task.put(Collections.singleton(record));
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new SingleStoreSinkConfig(props), "SELECT * from testdb." + tableExpected);
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
