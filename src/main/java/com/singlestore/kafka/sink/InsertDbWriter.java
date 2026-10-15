package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes rows with batched INSERT ... ON DUPLICATE KEY UPDATE statements.
 * Columns omitted from the update list keep their current values when a key already exists.
 */
public class InsertDbWriter {

    private static final Logger log = LoggerFactory.getLogger(InsertDbWriter.class);

    private final List<String> columns;
    private final List<ColumnMapping> columnMappings;
    private final String filter;
    private final String table;
    private final String onDuplicateKeySQL;
    private final int batchSize;

    public InsertDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table) {
        this(resolveColumns(config, record, table),
                config.tableToColumnToFieldMap.get(table),
                config.filter,
                table,
                config.onDuplicateKeySQL,
                config.insertBatchSize);
    }

    InsertDbWriter(List<String> columns, List<ColumnMapping> columnMappings, String filter, String table,
                   String onDuplicateKeySQL, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("insertBatchSize must be at least 1");
        }
        if (onDuplicateKeySQL == null || onDuplicateKeySQL.trim().isEmpty()) {
            throw new IllegalArgumentException("onDuplicateKeySQL must not be empty");
        }
        this.columns = columns;
        this.columnMappings = columnMappings;
        this.filter = filter;
        this.table = table;
        this.onDuplicateKeySQL = onDuplicateKeySQL;
        this.batchSize = batchSize;
    }

    public void write(Connection connection, Collection<SinkRecord> records) throws SQLException {
        for (List<SinkRecord> batch : partition(records, batchSize)) {
            executeBatch(connection, batch);
        }
    }

    public String generateQuery(int rowCount) {
        if (rowCount < 1) {
            throw new IllegalArgumentException("rowCount must be at least 1");
        }
        String quotedTable = SingleStoreDialect.quoteIdentifier(table);
        String columnNames = SingleStoreDialect.escapeColumnNames(columns);
        String update = "ON DUPLICATE KEY UPDATE " + onDuplicateKeySQL;
        if (filter == null) {
            return String.format("INSERT INTO %s (%s) VALUES %s %s",
                    quotedTable, columnNames, valueTuples(rowCount), update);
        }
        return String.format("INSERT INTO %s (%s) SELECT %s FROM (%s) AS `_singlestore_kafka_incoming` WHERE %s %s",
                quotedTable, columnNames, columnNames, filteredRow(), filter, update);
    }

    static List<List<SinkRecord>> partition(Collection<SinkRecord> records, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("insertBatchSize must be at least 1");
        }
        List<List<SinkRecord>> batches = new ArrayList<>();
        List<SinkRecord> batch = new ArrayList<>();
        for (SinkRecord record : records) {
            batch.add(record);
            if (batch.size() == batchSize) {
                batches.add(batch);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            batches.add(batch);
        }
        return batches;
    }

    private void executeBatch(Connection connection, List<SinkRecord> batch) throws SQLException {
        String query = generateQuery(filter == null ? batch.size() : 1);
        log.trace("Executing SQL:\n{}", query);
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            if (filter == null) {
                int index = 1;
                for (SinkRecord record : batch) {
                    index = bindRecord(stmt, record, index);
                }
                stmt.executeUpdate();
            } else {
                for (SinkRecord record : batch) {
                    bindRecord(stmt, record, 1);
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }
    }

    private int bindRecord(PreparedStatement stmt, SinkRecord record, int index) throws SQLException {
        List<Object> values;
        try {
            values = new ValueWithSchema(record).toSqlValues(columns, columnMappings);
        } catch (IOException ex) {
            throw new ConnectException("Failed to serialize record for INSERT", ex);
        }
        for (Object value : values) {
            stmt.setObject(index++, value);
        }
        return index;
    }

    private String valueTuples(int rowCount) {
        String tuple = "(" + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")";
        return String.join(", ", Collections.nCopies(rowCount, tuple));
    }

    private String filteredRow() {
        return "SELECT " + columns.stream()
                .map(column -> "? AS " + SingleStoreDialect.quoteIdentifier(column))
                .collect(Collectors.joining(", "));
    }

    private static List<String> resolveColumns(SingleStoreSinkConfig config, SinkRecord record, String table) {
        List<ColumnMapping> columnMappings = config.tableToColumnToFieldMap.get(table);
        if (columnMappings != null) {
            return columnMappings.stream().map(ColumnMapping::getColumnName).collect(Collectors.toList());
        }
        return new ValueWithSchema(record).getColumns();
    }
}
