package com.memsql.kafka.sink;

import com.memsql.kafka.MemSQLSinkConnector;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemSQLSinkConnectorTest {

    @Test
    public void testVersion() {
        String version = new MemSQLSinkConnector().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void connectorType() {
        Connector connector = new MemSQLSinkConnector();
        assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
    }
}
