package com.singlestore.kafka.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Builds the temporary staging table and INSERT ... SELECT statement used for
 * ON DUPLICATE KEY UPDATE writes.
 */
public class InsertDbWriter {

    // Temporary tables do not preserve LOAD DATA order. This column records the Kafka record
    // order so duplicate keys are applied in that order: the later record wins for VALUES(...)
    // assignments, and expressions such as `age` = `age` + 1 run once per record.
    private static final String ORDER_COLUMN = "_singlestore_kafka_order";
    private static final String TEMPORARY_TABLE_PREFIX = "_singlestore_kafka_incoming_";

    private final List<String> columns;
    private final String filter;
    private final String table;
    private final String onDuplicateKeySQL;
    private final String temporaryTable;
    private final String orderColumn;
    private final ShowCreateTable showCreateTable;

    public InsertDbWriter(SingleStoreSinkConfig config, SinkRecord record, String table,
                          String onDuplicateKeySQL, String showCreateTable) {
        this(CsvDbWriter.resolveColumns(config, record, table),
                config.filter,
                table,
                onDuplicateKeySQL,
                TEMPORARY_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", ""),
                showCreateTable);
    }

    InsertDbWriter(List<String> columns, String filter, String table,
                   String onDuplicateKeySQL, String temporaryTable, String showCreateTable) {
        this.columns = columns;
        this.filter = filter;
        this.table = table;
        this.onDuplicateKeySQL = Objects.requireNonNull(onDuplicateKeySQL, "onDuplicateKeySQL");
        this.temporaryTable = temporaryTable;
        this.showCreateTable = ShowCreateTable.parse(showCreateTable);
        this.orderColumn = uniqueOrderColumn(this.showCreateTable.columnNames());
    }

    public String generateCreateTemporaryTableQuery() {
        return showCreateTable.toCreateTemporaryTable(temporaryTable, orderColumn);
    }

    public String generateDropTemporaryTableQuery() {
        return "DROP TEMPORARY TABLE IF EXISTS " + SingleStoreDialect.quoteIdentifier(temporaryTable);
    }

    public String generateInsertQuery() {
        String where = filter == null ? "" : " WHERE " + filter;
        return String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s%s ORDER BY %s ON DUPLICATE KEY UPDATE %s",
                SingleStoreDialect.quoteIdentifier(table),
                SingleStoreDialect.escapeColumnNames(columns),
                SingleStoreDialect.escapeColumnNames(columns),
                SingleStoreDialect.quoteIdentifier(temporaryTable),
                where,
                SingleStoreDialect.quoteIdentifier(orderColumn),
                onDuplicateKeySQL);
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getTemporaryTable() {
        return temporaryTable;
    }

    public String getOrderColumn() {
        return orderColumn;
    }

    private static String uniqueOrderColumn(List<String> columns) {
        String result = ORDER_COLUMN;
        while (containsIgnoreCase(columns, result)) {
            result = "_" + result;
        }
        return result;
    }

    private static boolean containsIgnoreCase(List<String> columns, String candidate) {
        for (String column : columns) {
            if (column.equalsIgnoreCase(candidate)) {
                return true;
            }
        }
        return false;
    }
}
