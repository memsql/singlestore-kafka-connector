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

    private final Schema schema;

    public CsvDbWriter(SinkRecord record) {
        this.schema = record.valueSchema();
    }

    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = SingleStoreDialect.getColumnNames(schema);
        String queryEnding = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        return String.join(" ", queryPrefix, queryEnding);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            if (record.valueSchema() == null) {
                log.error("No value schema was provided for the data record: {}", record);
                throw new ConnectException(String.format("No value schema was provided for the data record: %s", record.toString()));
            }
            byte[] value = SingleStoreDialect.getRecordValueCSV(record).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
