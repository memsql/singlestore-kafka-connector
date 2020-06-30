package com.memsql.kafka.sink;

import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MemSQLSinkTaskTest {

    @Test
    public void testTaskType() {
        SinkTask task = new MemSQLSinkTask();
        SinkTask.class.isAssignableFrom(task.getClass());
    }

    @Test
    public void testVersion() {
        String version = new MemSQLSinkTask().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
        assertEquals(version, "1.0.0-SNAPSHOT");
    }

    @Test
    public void testEmptyData() {
        MemSQLSinkTask task = new MemSQLSinkTask();
        task.put(Collections.emptyList());
    }

    @Test
    public void testRetryableException() {
        MemSQLSinkTask task = new MemSQLSinkTask();
        SinkTaskContext context = new WorkerSinkTaskContext(null,null,null);
        task.initialize(context);
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "nonexistingDatabase");
        task.start(props);
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        List<SinkRecord> records = SinkRecordCreator.createRecords(10);
        for (int i = 0; i < config.maxRetries; i++) {
            try {
                task.put(records);
                fail("RetriableException should be thrown");
            } catch (RetriableException ex) {}
            catch (Exception ex) {
                fail("RetriableException should be thrown");
            }
        }
        try {
            task.put(records);
            fail("ConnectException should be thrown");
        } catch (ConnectException ex) {}
        catch (Exception ex) {
            fail("ConnectException should be thrown");
        }
    }
}
