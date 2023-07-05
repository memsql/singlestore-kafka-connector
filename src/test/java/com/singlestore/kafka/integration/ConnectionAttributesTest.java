package com.singlestore.kafka.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.JdbcHelper;
import com.singlestore.kafka.utils.VersionProvider;
import com.vdurmont.semver4j.Semver;

public class ConnectionAttributesTest extends IntegrationBase {
    protected static final Logger log = LoggerFactory.getLogger(ConnectionAttributesTest.class);

    @Test
    public void connectionAttributes() throws SQLException {
        assumeTrue(getSingleStoreVersion().isGreaterThanOrEqualTo(new Semver("8.1.0")));

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("_connector_name", "SingleStore Kafka Connector");
        attributes.put("_connector_version", VersionProvider.getVersion());
        attributes.put("_product_version", VersionProvider.getKafkaVersion());
        
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        try (
            Connection conn = JdbcHelper.getDDLConnection(new SingleStoreSinkConfig(props));
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from information_schema.mv_connection_attributes");
        ) {
            while(rs.next()) {
                String attribute = rs.getString(3);
                String value = rs.getString(4);
                if (attributes.containsKey(attribute)) {
                    assertEquals(attributes.get(attribute), value);
                    attributes.remove(attribute);
                }
            }    
        }

        assertTrue(attributes.isEmpty());
    }
}
