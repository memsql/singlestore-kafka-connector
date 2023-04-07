package com.singlestore.kafka.integration;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpsertTest extends IntegrationBase{
    @Test
    public void upsert() throws SQLException {
        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("data", Schema.STRING_SCHEMA);

        List<SinkRecord> records = Collections.singletonList(createRecord(schema, new Struct(schema)
                .put("id", 1)
                .put("data", "a"),
            "upsert"
        ));

        Map<String, String> props = new HashMap<>();
        props.put("singlestore.upsert", "true");

        put(props, records, "CREATE TABLE testdb.upsert(id INT PRIMARY KEY, data TEXT)", true);

        records = Collections.singletonList(createRecord(schema, new Struct(schema)
                .put("id", 1)
                .put("data", "b"),
            "upsert"
        ));
        put(props, records, null, false);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.upsert");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("b", rs.getString("data"));
        assertFalse(rs.next());
    }
}
