package com.singlestore.kafka.utils;

import com.singlestore.kafka.sink.SingleStoreDialect;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Reads the column definitions and shard key out of a SHOW CREATE TABLE statement.
 * Indexes and other key clauses are omitted. The shard key is adjusted so a
 * rowstore temporary table can use it.
 */
public class ShowCreateTableHelper {

    private final List<String> columnDefinitions;
    private final List<String> columnNames;
    private final String shardKey;

    private ShowCreateTableHelper(List<String> columnDefinitions, List<String> columnNames, String shardKey) {
        this.columnDefinitions = Collections.unmodifiableList(new ArrayList<>(columnDefinitions));
        this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
        this.shardKey = shardKey;
    }

    public static ShowCreateTableHelper parse(String statement) {
        if (statement == null || statement.trim().isEmpty()) {
            throw new ConnectException("SHOW CREATE TABLE returned an empty statement");
        }
        List<String> columnDefinitions = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        String shardKey = null;
        for (String part : splitDefinitions(statement)) {
            String clause = part.trim();
            if (clause.isEmpty()) {
                continue;
            }
            if (isShardKey(clause)) {
                if (shardKey != null) {
                    throw new ConnectException("SHOW CREATE TABLE contains more than one SHARD KEY");
                }
                shardKey = clause;
            } else if (clause.charAt(0) == '`') {
                columnDefinitions.add(stripAutoIncrement(clause));
                columnNames.add(readQuotedIdentifier(clause));
            }
        }
        if (columnDefinitions.isEmpty()) {
            throw new ConnectException("SHOW CREATE TABLE did not contain any columns");
        }
        return new ShowCreateTableHelper(columnDefinitions, columnNames, shardKey);
    }

    public List<String> columnNames() {
        return columnNames;
    }

    public String toCreateTemporaryTable(String temporaryTable, String orderColumn) {
        return toCreateTemporaryTable(temporaryTable, orderColumn, false);
    }

    /**
     * @param nullableColumns removes {@code NOT NULL} from copied columns. {@code LOAD DATA}
     * checks that constraint before {@code WHERE}, so a filtered-out NULL fails the load
     * unless the staging column accepts it. The INSERT still applies the filter.
     */
    public String toCreateTemporaryTable(String temporaryTable, String orderColumn, boolean nullableColumns) {
        StringJoiner definitions = new StringJoiner(", ");
        for (String column : columnDefinitions) {
            definitions.add(nullableColumns ? makeNullable(column) : column);
        }
        definitions.add(SingleStoreDialect.quoteIdentifier(orderColumn) + " BIGINT NULL");
        String compatibleShardKey = rowstoreShardKey();
        if (compatibleShardKey != null) {
            definitions.add(compatibleShardKey);
        }
        return "CREATE ROWSTORE TEMPORARY TABLE "
                + SingleStoreDialect.quoteIdentifier(temporaryTable)
                + " (" + definitions + ")";
    }

    /**
     * METADATA_ONLY is only valid on columnstore tables. TEXT, BLOB, and JSON cannot
     * be rowstore shard-key columns. Drop the option, or the whole key when a column
     * has one of those types, so the temporary table can be created.
     */
    private String rowstoreShardKey() {
        if (shardKey == null) {
            return null;
        }
        for (String column : shardKeyColumns(shardKey)) {
            if (isUnsupportedRowstoreShardType(columnDataType(columnDefinition(column)))) {
                return null;
            }
        }
        return removeKeywords(shardKey, "METADATA_ONLY");
    }

    private String columnDefinition(String name) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(name)) {
                return columnDefinitions.get(i);
            }
        }
        return null;
    }

    /**
     * Splits the CREATE TABLE body on top-level commas. Parentheses and commas in
     * column types, expressions, defaults, comments, and quoted identifiers are preserved.
     */
    private static List<String> splitDefinitions(String statement) {
        List<String> definitions = new ArrayList<>();
        int bodyStart = -1;
        int definitionStart = -1;
        int depth = 0;
        for (int i = 0; i < statement.length(); i++) {
            char c = statement.charAt(i);
            if (isQuote(c)) {
                i = quotedEnd(statement, i, c);
                continue;
            }

            if (bodyStart < 0) {
                if (c == '(') {
                    bodyStart = i;
                    definitionStart = i + 1;
                }
                continue;
            }

            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) {
                    depth--;
                } else {
                    definitions.add(statement.substring(definitionStart, i));
                    return definitions;
                }
            } else if (c == ',' && depth == 0) {
                definitions.add(statement.substring(definitionStart, i));
                definitionStart = i + 1;
            }
        }
        if (bodyStart < 0) {
            throw new ConnectException("SHOW CREATE TABLE did not contain a column list");
        }
        throw new ConnectException("SHOW CREATE TABLE has unbalanced parentheses");
    }

    private static boolean isQuote(char c) {
        return c == '`' || c == '\'' || c == '"';
    }

    private static int quotedEnd(String value, int start, char quote) {
        for (int i = start + 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && quote != '`' && i + 1 < value.length()) {
                i++;
            } else if (c == quote) {
                if (i + 1 < value.length() && value.charAt(i + 1) == quote) {
                    i++;
                } else {
                    return i;
                }
            }
        }
        throw new ConnectException("SHOW CREATE TABLE has an unterminated quoted value");
    }

    private static String readQuotedIdentifier(String clause) {
        return identifierName(clause, 0, skipIdentifier(clause, 0));
    }

    private static List<String> shardKeyColumns(String clause) {
        int index = afterShardKey(clause);
        if (index < 0) {
            return Collections.emptyList();
        }
        index = skipWhitespace(clause, index);
        if (index < clause.length() && clause.charAt(index) != '(') {
            index = skipWhitespace(clause, skipIdentifier(clause, index));
        }
        if (index >= clause.length() || clause.charAt(index) != '(') {
            return Collections.emptyList();
        }
        return readIdentifierList(clause, index + 1);
    }

    private static int skipIdentifier(String value, int index) {
        if (index < value.length() && value.charAt(index) == '`') {
            return quotedEnd(value, index, '`') + 1;
        }
        while (index < value.length() && isIdentifierCharacter(value.charAt(index))) {
            index++;
        }
        return index;
    }

    private static String identifierName(String value, int start, int end) {
        if (value.charAt(start) == '`') {
            return value.substring(start + 1, end - 1).replace("``", "`");
        }
        return value.substring(start, end);
    }

    private static List<String> readIdentifierList(String clause, int start) {
        List<String> names = new ArrayList<>();
        int i = start;
        while (i < clause.length()) {
            i = skipWhitespace(clause, i);
            if (i >= clause.length() || clause.charAt(i) == ')') {
                break;
            }
            if (clause.charAt(i) == ',') {
                i++;
                continue;
            }
            int end = skipIdentifier(clause, i);
            if (end == i) {
                break;
            }
            names.add(identifierName(clause, i, end));
            i = end;
        }
        return names;
    }

    private static String columnDataType(String column) {
        if (column == null) {
            return null;
        }
        int index = skipWhitespace(column, skipIdentifier(column, 0));
        if (startsWithKeywords(column, index, "AS")) {
            index = keywordsEnd(column, index, "AS");
            index = skipWhitespace(column, index);
            if (index < column.length() && column.charAt(index) == '(') {
                index = skipParentheses(column, index);
            }
            index = skipWhitespace(column, index);
            int persistedEnd = keywordsEnd(column, index, "PERSISTED");
            if (persistedEnd >= 0) {
                index = skipWhitespace(column, persistedEnd);
            }
        }
        int end = skipIdentifier(column, index);
        return end == index ? null : column.substring(index, end);
    }

    private static int skipParentheses(String value, int open) {
        int depth = 0;
        for (int i = open; i < value.length(); i++) {
            char c = value.charAt(i);
            if (isQuote(c)) {
                i = quotedEnd(value, i, c);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
        }
        throw new ConnectException("SHOW CREATE TABLE has unbalanced parentheses");
    }

    private static boolean isUnsupportedRowstoreShardType(String dataType) {
        return dataType != null && (dataType.equalsIgnoreCase("json")
                || endsWithIgnoreCase(dataType, "text")
                || endsWithIgnoreCase(dataType, "blob"));
    }

    private static boolean isShardKey(String clause) {
        return afterShardKey(clause) >= 0;
    }

    /**
     * Returns the index after {@code SHARD KEY}, or -1 when {@code clause} is not one.
     * A constraint symbol may be a quoted identifier or an unquoted token.
     */
    private static int afterShardKey(String clause) {
        int index = keywordsEnd(clause, 0, "SHARD", "KEY");
        if (index >= 0) {
            return index;
        }
        index = keywordsEnd(clause, 0, "CONSTRAINT");
        if (index < 0) {
            return -1;
        }
        index = skipWhitespace(clause, index);
        int key = keywordsEnd(clause, index, "SHARD", "KEY");
        if (key >= 0) {
            return key;
        }
        return keywordsEnd(clause, skipIdentifier(clause, index), "SHARD", "KEY");
    }

    /**
     * Removes AUTO_INCREMENT [AS SEQUENCE] from a column definition and makes that column
     * nullable. SingleStore requires an AUTO_INCREMENT column to be part of an index, and
     * indexes are not copied to the staging table. A nullable staging column keeps a NULL
     * from the record as NULL, so the destination table generates a value for it.
     */
    private static String stripAutoIncrement(String column) {
        String stripped = removeKeywords(column, "AUTO_INCREMENT", "AS", "SEQUENCE");
        stripped = removeKeywords(stripped, "AUTO_INCREMENT");
        return stripped == column ? column : makeNullable(stripped);
    }

    /**
     * Removes one keyword sequence that appears outside quotes. Returns {@code value}
     * itself when nothing was removed.
     */
    private static String removeKeywords(String value, String... keywords) {
        StringBuilder result = new StringBuilder();
        boolean removed = false;
        int i = 0;
        while (i < value.length()) {
            char c = value.charAt(i);
            if (isQuote(c)) {
                int end = quotedEnd(value, i, c) + 1;
                result.append(value, i, end);
                i = end;
                continue;
            }
            int end = i == 0 || !isIdentifierCharacter(value.charAt(i - 1))
                    ? keywordsEnd(value, i, keywords) : -1;
            if (end >= 0) {
                while (result.length() > 0 && Character.isWhitespace(result.charAt(result.length() - 1))) {
                    result.setLength(result.length() - 1);
                }
                i = end;
                removed = true;
                continue;
            }
            result.append(c);
            i++;
        }
        return removed ? result.toString() : value;
    }

    private static String makeNullable(String column) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < column.length()) {
            char c = column.charAt(i);
            if (isQuote(c)) {
                int end = quotedEnd(column, i, c) + 1;
                result.append(column, i, end);
                i = end;
                continue;
            }
            if (!Character.isWhitespace(c) && (i == 0 || !isIdentifierCharacter(column.charAt(i - 1)))) {
                int end = keywordsEnd(column, i, "NOT", "NULL");
                if (end >= 0) {
                    result.append("NULL");
                    i = end;
                    continue;
                }
            }
            result.append(c);
            i++;
        }
        return result.toString();
    }

    private static boolean startsWithKeywords(String clause, int start, String... keywords) {
        return keywordsEnd(clause, start, keywords) >= 0;
    }

    /**
     * Returns the index after the matched keywords, or -1 when the clause does not
     * contain them at {@code start}.
     */
    private static int keywordsEnd(String clause, int start, String... keywords) {
        int index = start;
        for (String keyword : keywords) {
            index = skipWhitespace(clause, index);
            if (!clause.regionMatches(true, index, keyword, 0, keyword.length())) {
                return -1;
            }
            int end = index + keyword.length();
            if (end < clause.length() && isIdentifierCharacter(clause.charAt(end))) {
                return -1;
            }
            index = end;
        }
        return index;
    }

    private static boolean isIdentifierCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    private static boolean endsWithIgnoreCase(String value, String suffix) {
        return value.length() >= suffix.length()
                && value.regionMatches(true, value.length() - suffix.length(), suffix, 0, suffix.length());
    }

    private static int skipWhitespace(String value, int index) {
        while (index < value.length() && Character.isWhitespace(value.charAt(index))) {
            index++;
        }
        return index;
    }
}
