package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import java.time.*;

import java.sql.ResultSet;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.memsql.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MetadataTableTest extends IntegrationBase {
    @Test
    public void checkValues() {
        try {
            executeQuery("USING testdb DROP TABLE IF EXISTS `kafka_connect_transaction_metadata`");

            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            List<SinkRecord> records = new ArrayList<>();
            records.add(createRecord(Schema.INT32_SCHEMA, 1, "checkValues"));
            records.add(createRecord(Schema.INT32_SCHEMA, 2, "checkValues"));

            MemSQLSinkTask task = new MemSQLSinkTask();
            task.start(props);
            LocalDateTime start = LocalDateTime.now();
            // Sleep to make the difference between creation time and start time greater then 1 second, so we can compare them
            Thread.sleep(1000);
            task.put(records);
            // Sleep to make the difference between creation time and end time greater then 1 second, so we can compare them
            Thread.sleep(1000);
            LocalDateTime end = LocalDateTime.now();
            task.stop();

            ResultSet res = SQLHelper.executeQuery(new MemSQLSinkConfig(props), "USING testdb SELECT * FROM `kafka_connect_transaction_metadata`");
            while(res.next()) {
                LocalDateTime created = LocalDateTime.parse(res.getString("createdAt"),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0"));
                assert(start.isBefore(created));
                assert(created.isBefore(end));

                assertEquals(2, res.getInt("count"));
                assertEquals("checkValues-0-0", res.getString("id"));
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
