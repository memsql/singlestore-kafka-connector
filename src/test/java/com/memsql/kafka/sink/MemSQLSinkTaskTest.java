package com.memsql.kafka.sink;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Test;

import java.util.Collections;

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
    }

    @Test
    public void testEmptyData() {
        MemSQLSinkTask task = new MemSQLSinkTask();
        task.put(Collections.emptyList());
    }
}
