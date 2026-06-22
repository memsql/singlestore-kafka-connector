package com.singlestore.kafka.sink;

import com.singlestore.kafka.SingleStoreSinkConnector;
import com.singlestore.kafka.metrics.SingleStoreTaskMetrics;
import com.singlestore.kafka.utils.VersionProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class SingleStoreSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SingleStoreSinkTask.class);
    private SingleStoreSinkConfig config;
    private SingleStoreDbWriter writer;
    private int retriesLeft;
    private SingleStoreTaskMetrics metrics;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SingleStore Sink Task");
        this.config = new SingleStoreSinkConfig(props);
        this.writer = new SingleStoreDbWriter(config);
        this.retriesLeft = config.maxRetries;
        String connectorName = props.get(SingleStoreSinkConnector.CONNECTOR_NAME_CONFIG);
        String taskId = props.get(SingleStoreSinkConnector.TASK_ID_CONFIG);
        this.metrics = new SingleStoreTaskMetrics(connectorName, taskId, config.customMetricTags);
        this.metrics.register();
        metrics.markTaskRunning();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        metrics.markTaskRunning();
        if (!records.isEmpty()) {
            try {
                SinkRecord first = records.iterator().next();
                log.debug(
                        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                                + "database",
                        records.size(), first.topic(), first.kafkaPartition(), first.kafkaOffset()
                );
                writer.write(records);
            } catch (SQLException ex) {
                metrics.incrementWriteProcessingErrors();
                log.warn(String.format("Write of %s records failed, retriesLeft=%s", records.size(), this.retriesLeft));
                String sqlExceptions = "";

                Throwable e;
                for(Iterator<Throwable> exIter = ex.iterator(); exIter.hasNext(); sqlExceptions += e + System.lineSeparator()) {
                    e = exIter.next();
                }

                if (this.retriesLeft == 0) {
                    metrics.markTaskFailed();
                    log.error(sqlExceptions);
                    throw new ConnectException(new SQLException(sqlExceptions));
                }
                this.retriesLeft -= 1;
                this.context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqlExceptions));
            } catch (RuntimeException ex) {
                metrics.incrementWriteProcessingErrors();
                metrics.markTaskFailed();
                throw ex;
            }
            metrics.incrementRecordsProcessed(records.size());
            this.retriesLeft = config.maxRetries;
        }
    }

    @Override
    public void stop() {
        log.info("Stopping SingleStore Sink Task");
        if (metrics != null) {
            if (!metrics.isFailed()) {
                metrics.markTaskStopped();
            }
            metrics.unregister();
        }
    }

    @Override
    public String version() {
        return VersionProvider.getVersion();
    }
}
