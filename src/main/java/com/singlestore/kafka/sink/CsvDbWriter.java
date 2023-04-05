package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class CsvDbWriter {

    List<String> columns;
    List<ColumnMapping> columnMappings;
    String filter;
    String table;

    public CsvDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table) {
        this.columnMappings = config.tableToColumnToFieldMap.get(table);
        if (columnMappings != null) {
            this.columns = columnMappings.stream().map(ColumnMapping::getColumnName).collect(Collectors.toList());
        } else {
            this.columns = new ValueWithSchema(record).getColumns();
        }
        this.filter = config.filter;
        this.table = table;
    }

    public String generateQuery(String ext) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = SingleStoreDialect.escapeColumnNames(columns);
        String queryTable = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        String queryFilter = filter == null ? "" : String.format("WHERE %s", filter);
        return String.join(" ", queryPrefix, queryTable, queryFilter);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            byte[] value = columnMappings != null ?
                new ValueWithSchema(record).mapColumnsToCSV(columnMappings).getBytes(StandardCharsets.UTF_8) :
                new ValueWithSchema(record).toCSV(columns).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
