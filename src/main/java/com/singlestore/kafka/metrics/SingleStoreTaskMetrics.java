package com.singlestore.kafka.metrics;

import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.LinkedHashMap;
import java.util.Map;

public class SingleStoreTaskMetrics implements SingleStoreTaskMetricsMXBean {
    private static final String DEFAULT_CONNECTOR_NAME = "unknown";
    private static final String DEFAULT_TASK_ID = "unknown";
    private final ObjectName objectName;
    private final String connectorName;
    private final String taskId;

    private volatile TaskStatus taskStatus = TaskStatus.UNASSIGNED;
    private volatile long recordsProcessedTotal = 0;
    private volatile long writeErrorsTotal = 0;

    public SingleStoreTaskMetrics(String connectorName, String taskId, Map<String, String> customMetricTags) {
        this.connectorName = connectorName == null ? DEFAULT_CONNECTOR_NAME : connectorName;
        this.taskId = taskId == null ? DEFAULT_TASK_ID : taskId;
        this.objectName = createObjectName(customMetricTags);
    }

    public void register() {
        JmxUtils.registerMXBean(objectName, this);
    }

    public void unregister() {
        JmxUtils.unregisterMXBean(objectName);
    }

    public void markTaskRunning() {
        this.taskStatus = TaskStatus.RUNNING;
    }

    public void markTaskStopped() {
        this.taskStatus = TaskStatus.STOPPED;
    }

    public void markTaskFailed() {
        this.taskStatus = TaskStatus.FAILED;
    }

    public boolean isFailed() {
        return this.taskStatus == TaskStatus.FAILED;
    }

    public void incrementRecordsProcessed(long count) {
        this.recordsProcessedTotal += count;
    }

    public void incrementWriteProcessingErrors() {
        this.writeErrorsTotal += 1;
    }

    @Override
    public String getTaskStatus() {
        return taskStatus.value();
    }

    @Override
    public long getRecordsProcessedTotal() {
        return recordsProcessedTotal;
    }

    @Override
    public long getWriteErrorsTotal() {
        return writeErrorsTotal;
    }

    private ObjectName createObjectName(Map<String, String> customMetricTags) {
        String name = String.format(
            "singlestore.kafka:type=connector-metrics,context=sink,connector-name=%s,task=%s",
            Sanitizer.jmxSanitize(connectorName),
            Sanitizer.jmxSanitize(taskId)
        );
        if (customMetricTags != null && !customMetricTags.isEmpty()) {
            String customTags = customMetricTags.entrySet().stream()
                .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
                .collect(Collectors.joining(","));
            name += "," + customTags;
        }

        try {
            return new ObjectName(name);
        } catch (MalformedObjectNameException ex) {
            throw new RuntimeException("Unable to create metric object name", ex);
        }
    }

    private enum TaskStatus {
        UNASSIGNED("unassigned"),
        RUNNING("running"),
        FAILED("failed"),
        STOPPED("stopped");

        private final String value;

        TaskStatus(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}

