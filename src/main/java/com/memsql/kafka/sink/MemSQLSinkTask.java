package com.memsql.kafka.sink;

import com.memsql.kafka.utils.VersionProvider;
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

public class MemSQLSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MemSQLSinkTask.class);
    private MemSQLSinkConfig config;
    private MemSQLDbWriter writer;
    private int retriesLeft;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting MemSQL Sink Task");
        this.config = new MemSQLSinkConfig(props);
        this.writer = new MemSQLDbWriter(config);
        this.retriesLeft = config.maxRetries;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (!records.isEmpty()) {
            SinkRecord first = records.iterator().next();
            if (first.valueSchema() == null) {
                log.error("No value schema was provided for the data record: {}", first);
                throw new ConnectException(String.format("No value schema was provided for the data record: %s", first.toString()));
            }
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
        log.info("Stopping MemSQL Sink Task");
    }

    @Override
    public String version() {
        return VersionProvider.getVersion();
    }
}
