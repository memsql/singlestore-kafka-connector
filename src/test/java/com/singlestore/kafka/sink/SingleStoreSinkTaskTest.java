package com.singlestore.kafka.sink;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class SingleStoreSinkTaskTest {

    @Test
    public void testTaskType() {
        SinkTask task = new SingleStoreSinkTask();
        SinkTask.class.isAssignableFrom(task.getClass());
    }

    @Test
    public void testVersion() {
        String version = new SingleStoreSinkTask().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void testEmptyData() {
        SingleStoreSinkTask task = new SingleStoreSinkTask();
        task.put(Collections.emptyList());
    }
}
