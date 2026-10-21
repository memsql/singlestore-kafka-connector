package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.errors.ConnectException;
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
    List<ColumnMapping> columnMappings;
    String filter;
    String table;
    boolean upsert;
    String orderColumn;

    public CsvDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table) {
        this(config, table, table, resolveColumns(config, record, table), config.filter, config.upsert, null);
    }

    CsvDbWriter(SingleStoreSinkConfig config, String table, String mappingTable, List<String> columns,
                String filter, boolean upsert, String orderColumn) {
        this.columnMappings = config.tableToColumnToFieldMap.get(mappingTable);
        this.columns = columns;
        this.filter = filter;
        this.table = table;
        this.upsert = upsert;
        this.orderColumn = orderColumn;
    }

    public String generateQuery(String ext) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        List<String> queryColumns = new ArrayList<>(columns);
        if (orderColumn != null) {
            queryColumns.add(0, orderColumn);
        }
        String columnNames = SingleStoreDialect.escapeColumnNames(queryColumns);
        String queryReplace = upsert ? "REPLACE" : "";
        String queryTable = String.format("INTO TABLE %s (%s)", SingleStoreDialect.quoteIdentifier(table), columnNames);
        // TODO: LOAD DATA checks NOT NULL before WHERE, so a filtered-out NULL in a
        // NOT NULL column fails the batch. The duplicate-key path avoids this by
        // making staging columns nullable. This direct load does not.
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

    static List<String> resolveColumns(SingleStoreSinkConfig config, SinkRecord record, String table) {
        List<ColumnMapping> columnMappings = config.tableToColumnToFieldMap.get(table);
        List<String> columns = columnMappings != null
                ? columnMappings.stream().map(ColumnMapping::getColumnName).collect(Collectors.toList())
                : new ValueWithSchema(record).getColumns();
        if (columns.isEmpty()) {
            throw new ConnectException(String.format("No columns to write for table `%s`", table));
        }
        return columns;
    }
}
