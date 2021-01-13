package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreDialect;
import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.JdbcHelper;
import com.singlestore.kafka.utils.SQLHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.*;

public class EscapingTest extends IntegrationBase {
    private static final String weirdName = "\\....1234567890,,,\\\\\t!@#$%^&*()-+=;;```~`[]{};:\"\"qwerty";
    private static final String weirdMetadataName = "metadata" + weirdName;
    private static final String weirdReferenceName = "ref" + weirdName;
    private static SingleStoreSinkConfig conf;

    @Before
    public void createTables() {
        try {
            executeQuery("USING testdb DROP TABLE IF EXISTS "+ SingleStoreDialect.quoteIdentifier(weirdName));
            executeQuery("USING testdb DROP TABLE IF EXISTS "+ SingleStoreDialect.quoteIdentifier(weirdMetadataName));
            executeQuery("USING testdb DROP TABLE IF EXISTS "+ SingleStoreDialect.quoteIdentifier(weirdReferenceName));

            Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.METADATA_TABLE_NAME, weirdMetadataName);
            props.put("tableKey.primary."+weirdName, SingleStoreDialect.quoteIdentifier(weirdName));
            conf = new SingleStoreSinkConfig(props);

            List<SinkRecord> records = new ArrayList<>();
            Schema schema = SchemaBuilder.struct().field(weirdName, Schema.STRING_SCHEMA);
            records.add(createRecord(schema, new Struct(schema).put(weirdName, weirdName), weirdName));

            SingleStoreSinkTask task = new SingleStoreSinkTask();
            task.start(props);
            // create table, create metadata table, insert row into table, insert row into metadata table
            task.put(records);
            task.stop();
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void checkTableContent() {
        try {
            ResultSet res = SQLHelper.executeQuery(conf, "USING testdb SELECT * FROM "+ SingleStoreDialect.quoteIdentifier(weirdName));
            while(res.next()) {
                assertEquals(res.getString(weirdName), weirdName);
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void checkMetadataTableContent() {
        try {
            ResultSet res = SQLHelper.executeQuery(conf, "USING testdb SELECT * FROM "+ SingleStoreDialect.quoteIdentifier(weirdMetadataName));
            while(res.next()) {
                assertEquals(res.getString("id"), weirdName + "-0-0");
                assertEquals(res.getInt("count"), 1);
            }
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void metadataRecordExists() {
        try {
            Connection conn = JdbcHelper.getDDLConnection(conf);
            assertTrue(JdbcHelper.metadataRecordExists(conn, weirdName + "-0-0", conf));
            assertFalse(JdbcHelper.metadataRecordExists(conn, "nonexistent metadata record", conf));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void tableExists() {
        try {
            Connection conn = JdbcHelper.getDDLConnection(conf);
            assertTrue(JdbcHelper.tableExists(conn, weirdName));
            assertTrue(JdbcHelper.tableExists(conn, weirdMetadataName));
            assertFalse(JdbcHelper.tableExists(conn, "nonexistent table"));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void isRefereceTable() {
        try {
            assertFalse(JdbcHelper.isReferenceTable(conf, weirdName));
            assertFalse(JdbcHelper.isReferenceTable(conf, weirdMetadataName));
            executeQuery(String.format("USING testdb CREATE REFERENCE TABLE %s (a int, PRIMARY KEY(a))", SingleStoreDialect.quoteIdentifier(weirdReferenceName)));
            assertTrue(JdbcHelper.isReferenceTable(conf, weirdReferenceName));
        } catch (Exception e) {
            log.error("", e);
            fail("Should not have thrown any exception");
        }
    }
}
