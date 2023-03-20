package com.singlestore.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class CsvDbWriter {

    private static final Logger log = LoggerFactory.getLogger(CsvDbWriter.class);

    private final SinkRecord record;

    public CsvDbWriter(SinkRecord record) {
        this.record = record;
    }

    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = SingleStoreDialect.getColumnNames(record);
        String queryEnding = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        return String.join(" ", queryPrefix, queryEnding);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            byte[] value = SingleStoreDialect.getRecordValueCSV(record).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
