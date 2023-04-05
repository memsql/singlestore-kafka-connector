package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public class CsvDbWriter {

    private static final Logger log = LoggerFactory.getLogger(CsvDbWriter.class);

    List<String> columns;
    String filter;

    public CsvDbWriter(SingleStoreSinkConfig config, SinkRecord record) {
        this.columns = new ValueWithSchema(record).getColumns();
        this.filter = config.filter;
    }

    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = SingleStoreDialect.escapeColumnNames(columns);
        String queryTable = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        String queryFilter = filter == null ? "" : String.format("WHERE %s", filter);
        return String.join(" ", queryPrefix, queryTable, queryFilter);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            byte[] value = new ValueWithSchema(record).toCSV(columns).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
