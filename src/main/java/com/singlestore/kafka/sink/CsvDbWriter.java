package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class CsvDbWriter {

    List<String> columns;
    List<String> loadColumns;
    List<ColumnMapping> columnMappings;
    String filter;
    String table;
    boolean upsert;
    String orderColumn;

    public CsvDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table) {
        this(config, record, table, table, config.filter, config.upsert, null, null);
    }

    CsvDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table, String mappingTable,
                String filter, boolean upsert, String orderColumn, List<String> loadColumns) {
        this.columnMappings = config.tableToColumnToFieldMap.get(mappingTable);
        if (columnMappings != null) {
            this.columns = columnMappings.stream().map(ColumnMapping::getColumnName).collect(Collectors.toList());
        } else {
            this.columns = new ValueWithSchema(record).getColumns();
        }
        this.loadColumns = loadColumns == null ? this.columns : loadColumns;
        this.filter = filter;
        this.table = table;
        this.upsert = upsert;
        this.orderColumn = orderColumn;
    }

    public String generateQuery(String ext) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        List<String> queryColumns = new ArrayList<>(loadColumns);
        if (orderColumn != null) {
            queryColumns.add(0, orderColumn);
        }
        String columnNames = SingleStoreDialect.escapeColumnNames(queryColumns);
        String queryReplace = upsert ? "REPLACE" : "";
        String queryTable = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        String queryFilter = filter == null ? "" : String.format("WHERE %s", filter);
        return String.join(" ", queryPrefix, queryReplace, queryTable, queryFilter);
    }

    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        long order = 0;
        for (SinkRecord record: records) {
            if (orderColumn != null) {
                outputStream.write(Long.toString(order++).getBytes(StandardCharsets.UTF_8));
                outputStream.write('\t');
            }
            byte[] value = columnMappings != null ?
                new ValueWithSchema(record).mapColumnsToCSV(columnMappings).getBytes(StandardCharsets.UTF_8) :
                new ValueWithSchema(record).toCSV(columns).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
