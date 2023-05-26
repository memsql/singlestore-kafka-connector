package com.singlestore.kafka.integration;

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

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilterTest extends IntegrationBase {
    @Test
    public void filter() {
        try {
            executeQuery("DROP TABLE IF EXISTS testdb.filter");
            executeQuery("CREATE TABLE testdb.filter (" +
                "`a` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
                "`b` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
                "PRIMARY KEY (`a`, `b`)" +
                ") COLLATE utf8_general_ci;");
            Map<String, String> props = new HashMap<>();

            props.put("singlestore.columnToField.filter.a", "a");
            props.put("singlestore.columnToField.filter.b", "b");
            props.put("singlestore.filterNullValues", "a,b");

            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct()
                .field("a", SchemaBuilder.string().optional().build())
                .field("b", SchemaBuilder.string().optional().build())
                .build();

            records.add(createRecord(schema, new Struct(schema)
                .put("a", null)
                .put("b", "abc"),
                "filter"));
            
            put(props, records);
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
