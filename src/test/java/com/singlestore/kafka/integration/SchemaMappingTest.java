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

public class SchemaMappingTest extends IntegrationBase {
    @Test
    public void schemaMapping() throws SQLException {
        Schema nestedStructSchema = SchemaBuilder.struct()
            .field("c1", Schema.STRING_SCHEMA);
        Schema schema = SchemaBuilder.struct()
            .field("f1", Schema.BOOLEAN_SCHEMA)
            .field("f2", Schema.INT8_SCHEMA)
            .field("f3", nestedStructSchema);

        List<SinkRecord> records = Collections.singletonList(createRecord(schema, new Struct(schema)
            .put("f1", true)
            .put("f2", (byte)10)
            .put("f3", new Struct(nestedStructSchema)
                .put("c1", "v1")),
            "schemaMapping"
        ));

        Map<String, String> props = new HashMap<>();
        props.put("singlestore.columnToField.schemaMapping.c1", "f1");
        props.put("singlestore.columnToField.schemaMapping.c2", "f3.c1");

        put(props, records);

        ResultSet rs = executeQueryWithResultSet("SELECT * FROM testdb.schemaMapping");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean("c1"));
        assertEquals("v1", rs.getString("c2"));
        assertFalse(rs.next());
    }
}
