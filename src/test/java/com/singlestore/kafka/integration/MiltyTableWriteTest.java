package com.singlestore.kafka.integration;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MiltyTableWriteTest extends IntegrationBase {
    @Test
    public void multiTableWrite() throws SQLException {
        Schema schema1 = SchemaBuilder.struct()
            .field("tableId", Schema.STRING_SCHEMA)
            .field("c1", Schema.INT32_SCHEMA);
        SinkRecord record1 = createRecord(schema1, new Struct(schema1)
            .put("tableId", "t1")
            .put("c1", 10));

        Schema schema2 = SchemaBuilder.struct()
            .field("tableId", Schema.STRING_SCHEMA)
            .field("c3", Schema.INT32_SCHEMA)
            .field("c4", Schema.STRING_SCHEMA);
        SinkRecord record2 = createRecord(schema2, new Struct(schema2)
            .put("tableId", "t2")
            .put("c3", 10)
            .put("c4", "abc"));

        Schema schema3 = SchemaBuilder.struct()
            .field("c1", Schema.INT32_SCHEMA);
        SinkRecord record3 = createRecord(schema3, new Struct(schema3)
            .put("c1", 10));

        SinkRecord record4 = createRecord(schema1, new Struct(schema1)
            .put("tableId", "invalid")
            .put("c1", 11));

        List<SinkRecord> records = Arrays.asList(record1, record2, record3, record4);

        Map<String, String> props = new HashMap<>();
        props.put("singlestore.recordToTable.mappingField", "tableId");
        props.put("singlestore.recordToTable.mapping.t1", "multiTableWrite1");
        props.put("singlestore.recordToTable.mapping.t2", "multiTableWrite2");

        put(props, records);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.multiTableWrite1");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt("c1"));
        assertFalse(rs.next());
        rs.close();

        rs = executeQueryWithResultSet("SELECT * FROM testdb.multiTableWrite2");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt("c3"));
        assertEquals("abc", rs.getString("c4"));
        assertFalse(rs.next());
        rs.close();
    }
}
