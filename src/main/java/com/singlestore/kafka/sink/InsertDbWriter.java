package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ShowCreateTableHelper;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    // assignments, and expressions such as `age` = `age` + 1 run once per record. The ORDER BY
    // makes SingleStore gather the rows on the aggregator and insert them from there, instead of
    // inserting on each partition in parallel.
    private static final String ORDER_COLUMN = "_singlestore_kafka_order";
    // Staging rows are projected under these names before ON DUPLICATE KEY UPDATE. The
    // temporary table keeps destination column names so LOAD DATA and singlestore.filter
    // can use them, but an unqualified name in the assignment list would otherwise match
    // both tables.
    private static final String STAGING_COLUMN_PREFIX = "_singlestore_kafka_column_";
    private static final String SOURCE_ALIAS = "_singlestore_kafka_src";
    // Followed by a SHA-256 hash of the exact destination name, so every destination table gets
    // its own fixed-length staging name regardless of the destination name's length or case.
    private static final String TEMPORARY_TABLE_PREFIX = "_ssk_";

    private final List<String> columns;
    private final String filter;
    private final String table;
    private final String onDuplicateKeySQL;
    private final String temporaryTable;
    private final String orderColumn;
    private final ShowCreateTableHelper showCreateTable;

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
        if (columns == null || columns.isEmpty()) {
            throw new ConnectException(String.format("No columns to write for table `%s`", table));
        }
        this.columns = columns;
        this.filter = filter;
        this.table = table;
        this.onDuplicateKeySQL = Objects.requireNonNull(onDuplicateKeySQL, "onDuplicateKeySQL");
        this.temporaryTable = temporaryTable;
        this.showCreateTable = ShowCreateTableHelper.parse(showCreateTable);
        this.orderColumn = uniqueOrderColumn(this.showCreateTable.columnNames());
    }

    public String generateCreateTemporaryTableQuery() {
        // LOAD DATA rejects NULL in a NOT NULL column before applying singlestore.filter.
        // Nullable staging columns let that row load; the INSERT WHERE then discards it.
        return showCreateTable.toCreateTemporaryTable(temporaryTable, orderColumn, filter != null);
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
        return TEMPORARY_TABLE_PREFIX + sha256(table);
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                result.append(String.format("%02x", b & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
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
