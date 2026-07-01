package com.singlestore.kafka.integration;

import com.singlestore.kafka.SingleStoreSinkConnector;
import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class MetricsTest extends IntegrationBase {

    @Test
    public void shouldReportTaskMetricsOnHappyPath() throws Exception {
        executeQuery("DROP TABLE IF EXISTS testdb.topic");
        executeQuery("DROP TABLE IF EXISTS testdb.kafka_connect_transaction_metadata");

        SingleStoreSinkConnector connector = new SingleStoreSinkConnector();
        Map<String, String> connectorProps = ConfigHelper.getMinimalRequiredParameters();
        connectorProps.put(SingleStoreSinkConnector.CONNECTOR_NAME_CONFIG, "metrics-test-happy");
        connector.start(connectorProps);

        Map<String, String> taskProps = connector.taskConfigs(1).get(0);
        String connectorName = taskProps.get(SingleStoreSinkConnector.CONNECTOR_NAME_CONFIG);
        String taskId = taskProps.get(SingleStoreSinkConnector.TASK_ID_CONFIG);
        ObjectName metricsBean = new ObjectName(
            "singlestore.kafka:type=connector-metrics,context=sink,connector-name=" + connectorName + ",task=" + taskId
        );
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        SingleStoreSinkTask task = new SingleStoreSinkTask();
        SinkTaskContext context = new WorkerSinkTaskContext(null, null, null);
        task.initialize(context);
        task.start(taskProps);

        assertEquals("running", mBeanServer.getAttribute(metricsBean, "TaskStatus"));
        assertEquals(0L, mBeanServer.getAttribute(metricsBean, "RecordsProcessedTotal"));
        assertEquals(0L, mBeanServer.getAttribute(metricsBean, "WriteErrorsTotal"));

        List<SinkRecord> records = SinkRecordCreator.createRecords(4);

        task.put(records.subList(0, 2));

        assertEquals("running", mBeanServer.getAttribute(metricsBean, "TaskStatus"));
        assertEquals(2L, mBeanServer.getAttribute(metricsBean, "RecordsProcessedTotal"));
        assertEquals(0L, mBeanServer.getAttribute(metricsBean, "WriteErrorsTotal"));

        task.put(records.subList(2, 4));

        assertEquals("running", mBeanServer.getAttribute(metricsBean, "TaskStatus"));
        assertEquals(4L, mBeanServer.getAttribute(metricsBean, "RecordsProcessedTotal"));
        assertEquals(0L, mBeanServer.getAttribute(metricsBean, "WriteErrorsTotal"));

        task.stop();
        connector.stop();
    }

    @Test
    public void shouldReportTaskMetricsOnErrorPath() throws Exception {
        executeQuery("DROP TABLE IF EXISTS testdb.topic");
        executeQuery("DROP TABLE IF EXISTS testdb.kafka_connect_transaction_metadata");

        // Create table with wrong schema to trigger error
        executeQuery("CREATE TABLE testdb.topic (id INT PRIMARY KEY)");

        SingleStoreSinkConnector connector = new SingleStoreSinkConnector();
        Map<String, String> connectorProps = ConfigHelper.getMinimalRequiredParameters();
        connectorProps.put(SingleStoreSinkConnector.CONNECTOR_NAME_CONFIG, "metrics-test-error");
        connector.start(connectorProps);

        Map<String, String> taskProps = connector.taskConfigs(1).get(0);
        String connectorName = taskProps.get(SingleStoreSinkConnector.CONNECTOR_NAME_CONFIG);
        String taskId = taskProps.get(SingleStoreSinkConnector.TASK_ID_CONFIG);
        ObjectName metricsBean = new ObjectName(
            "singlestore.kafka:type=connector-metrics,context=sink,connector-name=" + connectorName + ",task=" + taskId
        );
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        SingleStoreSinkTask task = new SingleStoreSinkTask();
        SinkTaskContext context = new WorkerSinkTaskContext(null, null, null);
        task.initialize(context);
        task.start(taskProps);

        List<SinkRecord> records = SinkRecordCreator.createRecords(2);
        for (long i = 0; i < 10; i++) {
            assertThrows(RetriableException.class, () -> task.put(records));
            assertEquals("running", mBeanServer.getAttribute(metricsBean, "TaskStatus"));
            assertEquals(i + 1, mBeanServer.getAttribute(metricsBean, "WriteErrorsTotal"));
        }

        assertThrows(ConnectException.class, () -> task.put(records));
        assertEquals("failed", mBeanServer.getAttribute(metricsBean, "TaskStatus"));
        assertEquals(11L, mBeanServer.getAttribute(metricsBean, "WriteErrorsTotal"));

        task.stop();
        connector.stop();
    }
}

