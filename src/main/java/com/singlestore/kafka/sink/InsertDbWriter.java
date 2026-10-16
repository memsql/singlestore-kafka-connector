package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.ValueWithSchema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Builds the temporary staging table and INSERT ... SELECT statement used for
 * ON DUPLICATE KEY UPDATE writes.
 */
public class InsertDbWriter {

    // Temporary tables do not preserve LOAD DATA order. This column records the Kafka record
    // order so duplicate keys are applied in that order: the later record wins for VALUES(...)
    // assignments, and expressions such as `age` = `age` + 1 run once per record.
    private static final String ORDER_COLUMN = "_singlestore_kafka_order";
    private static final String STAGING_COLUMN_PREFIX = "_singlestore_kafka_column_";
    private static final String TEMPORARY_TABLE_PREFIX = "_singlestore_kafka_incoming_";

    private final List<String> columns;
    private final List<String> stagingColumns;
    private final String filter;
    private final String table;
    private final String onDuplicateKeySQL;
    private final String temporaryTable;
    private final String orderColumn;

    public InsertDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table) {
        this(resolveColumns(config, record, table),
                config.filter,
                table,
                config.onDuplicateKeySQL,
                TEMPORARY_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", ""));
    }

    InsertDbWriter(List<String> columns, String filter, String table,
                   String onDuplicateKeySQL, String temporaryTable) {
        if (onDuplicateKeySQL == null || onDuplicateKeySQL.trim().isEmpty()) {
            throw new IllegalArgumentException("onDuplicateKeySQL must not be empty");
        }
        this.columns = columns;
        this.filter = filter;
        this.table = table;
        this.onDuplicateKeySQL = onDuplicateKeySQL;
        this.temporaryTable = temporaryTable;
        this.orderColumn = uniqueOrderColumn(columns);
        this.stagingColumns = IntStream.range(0, columns.size())
                .mapToObj(index -> STAGING_COLUMN_PREFIX + index)
                .collect(Collectors.toList());
    }

    public String generateCreateTemporaryTableQuery() {
        return String.format(
                "CREATE ROWSTORE TEMPORARY TABLE %s AS SELECT NULL :> BIGINT AS %s, %s FROM %s WHERE 1=0",
                SingleStoreDialect.quoteIdentifier(temporaryTable),
                SingleStoreDialect.quoteIdentifier(orderColumn),
                aliasedColumns(columns, stagingColumns),
                SingleStoreDialect.quoteIdentifier(table));
    }

    public String generateInsertQuery() {
        String where = filter == null ? "" : " WHERE " + filter;
        return String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s%s ORDER BY %s ON DUPLICATE KEY UPDATE %s",
                SingleStoreDialect.quoteIdentifier(table),
                SingleStoreDialect.escapeColumnNames(columns),
                aliasedColumns(stagingColumns, columns),
                SingleStoreDialect.quoteIdentifier(temporaryTable),
                where,
                SingleStoreDialect.quoteIdentifier(orderColumn),
                onDuplicateKeySQL);
    }

    public String getTemporaryTable() {
        return temporaryTable;
    }

    public String getOrderColumn() {
        return orderColumn;
    }

    public List<String> getStagingColumns() {
        return stagingColumns;
    }

    private static String aliasedColumns(List<String> source, List<String> aliases) {
        return IntStream.range(0, source.size())
                .mapToObj(index -> SingleStoreDialect.quoteIdentifier(source.get(index))
                        + " AS " + SingleStoreDialect.quoteIdentifier(aliases.get(index)))
                .collect(Collectors.joining(", "));
    }

    private static String uniqueOrderColumn(List<String> columns) {
        String result = ORDER_COLUMN;
        while (columns.contains(result)) {
            result = "_" + result;
        }
        return result;
    }

    private static List<String> resolveColumns(SingleStoreSinkConfig config, SinkRecord record, String table) {
        List<ColumnMapping> columnMappings = config.tableToColumnToFieldMap.get(table);
        if (columnMappings != null) {
            return columnMappings.stream().map(ColumnMapping::getColumnName).collect(Collectors.toList());
        }
        return new ValueWithSchema(record).getColumns();
    }
}
