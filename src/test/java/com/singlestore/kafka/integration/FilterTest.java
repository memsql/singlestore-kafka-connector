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
            Map<String, String> props = new HashMap<>();
            props.put("singlestore.filter", "a > 1");
            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .build();

            records.add(createRecord(schema, new Struct(schema)
                .put("a", 1)
                .put("b", "abc"), "filter"));
            records.add(createRecord(schema, new Struct(schema)
                .put("a", 2)
                .put("b", "abcd"), "filter"));

            put(props, records);

            ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.filter");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
