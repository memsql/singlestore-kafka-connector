package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.fail;

public class DataTransformTest extends IntegrationBase {
    SinkRecord record;

    @Before
    public void setUp() {
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("name", Schema.STRING_SCHEMA);
        record = createRecord(schema, new Struct(schema).put("id", 1).put("name", "John"), "topic");
    }

    @Test
    public void noWhitelistInConfig() {
        testFieldsWhitelist(new HashMap<>(), true);
    }

    @Test
    public void whitelistInConfig() {
        testFieldsWhitelist(new HashMap<String, String>() {{
            put("fields.whitelist", "name");
        }}, false);
    }

    public void testFieldsWhitelist(Map<String, String> keys, boolean shouldHaveID) {
        try {
            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
            keys.forEach(props::put);

            executeQuery("DROP TABLE IF EXISTS testdb." + record.topic());

            SingleStoreSinkTask task = new SingleStoreSinkTask();
            task.start(props);
            task.put(Collections.singleton(record));
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new SingleStoreSinkConfig(props), "SELECT * from testdb." + record.topic());
            if (res.next()) {
                assert(res.getString("name").equals("John"));
                if (shouldHaveID) {
                    assert(res.getInt("id") == 1);
                } else {
                    try {
                        res.getInt("id");
                        fail("must throw an exception");
                    } catch (SQLException e) {
                    } catch (Exception e) {
                        fail("invalid exception");
                    }
                }
            } else {
                fail("Table " + record.topic() + " should contains 1 row -");
            }

        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
