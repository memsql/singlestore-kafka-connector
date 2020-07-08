package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.sink.MemSQLSinkTask;
import com.memsql.kafka.utils.ConfigHelper;
import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class RetryableExceptionTest extends IntegrationBase{
    @Test
    public void testRetryableException() {
        MemSQLSinkTask task = new MemSQLSinkTask();
        SinkTaskContext context = new WorkerSinkTaskContext(null,null,null);
        task.initialize(context);
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
        task.start(props);
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);

        try {
            executeQuery("DROP DATABASE testdb");
        } catch(Exception ex) {
            fail("Should not have thrown any exception");
        }

        List<SinkRecord> records = SinkRecordCreator.createRecords(10);
        for (int i = 0; i < config.maxRetries; i++) {
            try {
                task.put(records);
                fail("RetriableException should be thrown");
            } catch (RetriableException ignored) {}
            catch (Exception ex) {
                fail("RetriableException should be thrown");
            }
        }
        try {
            task.put(records);
            fail("ConnectException should be thrown");
        } catch (ConnectException ignored) {}
        catch (Exception ex) {
            fail("ConnectException should be thrown");
        }
    }
}
