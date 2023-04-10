package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import com.singlestore.kafka.utils.ConfigHelper;
import com.singlestore.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExceptionTest extends IntegrationBase {

    @Test
    public void testRetryableException() {
        SingleStoreSinkTask task = new SingleStoreSinkTask();
        SinkTaskContext context = new WorkerSinkTaskContext(null,null,null);
        task.initialize(context);
        Map<String, String> props = ConfigHelper.getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testdb");
        task.start(props);
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);

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
