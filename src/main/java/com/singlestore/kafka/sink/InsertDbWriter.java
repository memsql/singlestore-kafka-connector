package com.singlestore.kafka.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Builds the temporary staging table and INSERT ... SELECT statement used for
 * ON DUPLICATE KEY UPDATE writes.
 */
public class InsertDbWriter {

    // Temporary tables do not preserve LOAD DATA order. This column records the Kafka record
    // order so duplicate keys are applied in that order: the later record wins for VALUES(...)
    // assignments, and expressions such as `age` = `age` + 1 run once per record.
    private static final String ORDER_COLUMN = "_singlestore_kafka_order";
    // Staging rows are projected under these names before ON DUPLICATE KEY UPDATE. The
    // temporary table keeps destination column names so LOAD DATA and singlestore.filter
    // can use them, but an unqualified name in the assignment list would otherwise match
    // both tables.
    private static final String STAGING_COLUMN_PREFIX = "_singlestore_kafka_column_";
    private static final String SOURCE_ALIAS = "_singlestore_kafka_src";
    // The temporary table name must stay the same across batches so SingleStore can reuse
    // compiled plans for the LOAD DATA and INSERT ... SELECT statements.
    private static final String TEMPORARY_TABLE_PREFIX = "_ssk_";
    private static final int MAX_IDENTIFIER_LENGTH = 256;

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
                temporaryTableName(table),
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
        List<String> stagingColumns = stagingColumns();
        String where = filter == null ? "" : " WHERE " + filter;
        return String.format(
                "INSERT INTO %s (%s) SELECT %s FROM (SELECT %s, %s FROM %s%s) AS %s ORDER BY %s ON DUPLICATE KEY UPDATE %s",
                SingleStoreDialect.quoteIdentifier(table),
                SingleStoreDialect.escapeColumnNames(columns),
                SingleStoreDialect.escapeColumnNames(stagingColumns),
                aliasedColumns(columns, stagingColumns),
                SingleStoreDialect.quoteIdentifier(orderColumn),
                SingleStoreDialect.quoteIdentifier(temporaryTable),
                where,
                SingleStoreDialect.quoteIdentifier(SOURCE_ALIAS),
                SingleStoreDialect.quoteIdentifier(orderColumn),
                onDuplicateKeySQL);
    }

    private List<String> stagingColumns() {
        List<String> stagingColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String alias = STAGING_COLUMN_PREFIX + i;
            while (conflicts(alias, stagingColumns)) {
                alias = "_" + alias;
            }
            stagingColumns.add(alias);
        }
        return stagingColumns;
    }

    private boolean conflicts(String alias, List<String> stagingColumns) {
        return containsIgnoreCase(showCreateTable.columnNames(), alias)
                || alias.equalsIgnoreCase(orderColumn)
                || containsIgnoreCase(stagingColumns, alias);
    }

    private static String aliasedColumns(List<String> source, List<String> aliases) {
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < source.size(); i++) {
            joiner.add(SingleStoreDialect.quoteIdentifier(source.get(i))
                    + " AS " + SingleStoreDialect.quoteIdentifier(aliases.get(i)));
        }
        return joiner.toString();
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

    static String temporaryTableName(String table) {
        String name = TEMPORARY_TABLE_PREFIX + table;
        if (name.length() <= MAX_IDENTIFIER_LENGTH) {
            return name;
        }
        int end = MAX_IDENTIFIER_LENGTH;
        if (Character.isHighSurrogate(name.charAt(end - 1))) {
            end--;
        }
        return name.substring(0, end);
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
