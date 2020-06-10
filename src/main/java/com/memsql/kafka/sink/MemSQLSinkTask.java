package com.memsql.kafka.sink;

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
    int retriesLeft;

    @Override
    public void start(Map<String, String> props) {
        this.config = new MemSQLSinkConfig(props);
        this.writer = new MemSQLDbWriter(config);
        this.retriesLeft = config.maxRetries;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (!records.isEmpty()) {
            try {
                writer.write(records);
            } catch (SQLException ex) {
                String sqlExceptions = "";

                Throwable e;
                for(Iterator<Throwable> exIter = ex.iterator(); exIter.hasNext(); sqlExceptions += e + System.lineSeparator()) {
                    e = exIter.next();
                }

                if (this.retriesLeft == 0) {
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
        //TODO investigate if we should close some connections or make some other work during stopping job
    }

    @Override
    public String version() {
        //TODO make it in more flexible way
        return "0.0.1-beta";
    }
}
