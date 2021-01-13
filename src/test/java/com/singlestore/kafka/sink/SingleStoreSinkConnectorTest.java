package com.singlestore.kafka.sink;

import com.singlestore.kafka.SingleStoreSinkConnector;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.*;

public class SingleStoreSinkConnectorTest {

    @Test
    public void testVersion() {
        String version = new SingleStoreSinkConnector().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void connectorType() {
        Connector connector = new SingleStoreSinkConnector();
        assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
    }
}
