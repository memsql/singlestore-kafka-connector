package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ValueWithSchema;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SingleStoreSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SingleStoreSinkTask.class);
    private SingleStoreSinkConfig config;
    private SingleStoreDbWriter writer;
    private int retriesLeft;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SingleStore Sink Task");
        this.config = new SingleStoreSinkConfig(props);
        this.writer = new SingleStoreDbWriter(config);
        this.retriesLeft = config.maxRetries;
    }

    private boolean takeRecord(SinkRecord record) {
        ValueWithSchema vs = new ValueWithSchema(record);
        if (config.filterNullValues != null) {
            for (String path: config.filterNullValues) {
                if (vs.getByPath(path).getValue() == null) {
                    return false;
                }
            }    
        }
        return true;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records = records.stream().filter(record -> {
            return takeRecord(record);
        }).collect(Collectors.toList());
    
        if (!records.isEmpty()) {
            SinkRecord first = records.iterator().next();
            log.debug(
                    "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                            + "database",
                    records.size(), first.topic(), first.kafkaPartition(), first.kafkaOffset()
            );

            try {
                writer.write(records);
            } catch (SQLException ex) {
                log.warn(String.format("Write of %s records failed, retriesLeft=%s", records.size(), this.retriesLeft));
                String sqlExceptions = "";

                Throwable e;
                for(Iterator<Throwable> exIter = ex.iterator(); exIter.hasNext(); sqlExceptions += e + System.lineSeparator()) {
                    e = exIter.next();
                }

                if (this.retriesLeft == 0) {
                    log.error(sqlExceptions);
                    throw new ConnectException(new SQLException(sqlExceptions));
                }
                this.retriesLeft -= 1;
                this.context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqlExceptions));
            }
            this.retriesLeft = config.maxRetries;
        }
    }

    @Override
    public void stop() {
        log.info("Stopping SingleStore Sink Task");
    }

    @Override
    public String version() {
        return VersionProvider.getVersion();
    }
}
