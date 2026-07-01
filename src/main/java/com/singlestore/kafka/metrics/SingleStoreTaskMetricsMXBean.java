package com.singlestore.kafka.metrics;

public interface SingleStoreTaskMetricsMXBean {
    String getTaskStatus();

    long getRecordsProcessedTotal();

    long getWriteErrorsTotal();
}

