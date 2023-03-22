package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class CsvDbWriter {

    private final List<ColumnMapping> columns;


    public CsvDbWriter(SingleStoreSinkConfig config, String table, SinkRecord record) {
        this.columns = SingleStoreDialect.getColumns(config, table, record);
    }

    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = columns.stream()
            .map(ColumnMapping::getColumnName)
            .map(SingleStoreDialect::quoteIdentifier)
            .collect(Collectors.joining(", "));
        String queryEnding = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        return String.join(" ", queryPrefix, queryEnding);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            byte[] value = SingleStoreDialect.getRecordValueCSV(record, columns).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
