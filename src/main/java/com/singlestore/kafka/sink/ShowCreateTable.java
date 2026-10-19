package com.singlestore.kafka.sink;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Reads the column definitions and shard key out of a SHOW CREATE TABLE statement.
 * Indexes and other key clauses are omitted.
 */
class ShowCreateTable {

    private final List<String> columnDefinitions;
    private final List<String> columnNames;
    private final String shardKey;

    private ShowCreateTable(List<String> columnDefinitions, List<String> columnNames, String shardKey) {
        this.columnDefinitions = Collections.unmodifiableList(new ArrayList<>(columnDefinitions));
        this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
        this.shardKey = shardKey;
    }

    static ShowCreateTable parse(String statement) {
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
        return new ShowCreateTable(columnDefinitions, columnNames, shardKey);
    }

    List<String> columnNames() {
        return columnNames;
    }

    String toCreateTemporaryTable(String temporaryTable, String orderColumn) {
        StringJoiner definitions = new StringJoiner(", ");
        for (String column : columnDefinitions) {
            definitions.add(column);
        }
        definitions.add(SingleStoreDialect.quoteIdentifier(orderColumn) + " BIGINT NULL");
        if (shardKey != null) {
            definitions.add(shardKey);
        }
        return "CREATE ROWSTORE TEMPORARY TABLE "
                + SingleStoreDialect.quoteIdentifier(temporaryTable)
                + " (" + definitions + ")";
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
        int end = quotedEnd(clause, 0, '`');
        return clause.substring(1, end).replace("``", "`");
    }

    private static boolean isShardKey(String clause) {
        if (startsWithKeywords(clause, 0, "SHARD", "KEY")) {
            return true;
        }
        if (!startsWithKeywords(clause, 0, "CONSTRAINT")) {
            return false;
        }
        int index = skipWhitespace(clause, "CONSTRAINT".length());
        if (startsWithKeywords(clause, index, "SHARD", "KEY")) {
            return true;
        }

        // A constraint symbol may be a quoted identifier or an unquoted token.
        index = skipWhitespace(clause, index);
        if (index < clause.length() && clause.charAt(index) == '`') {
            index = quotedEnd(clause, index, '`') + 1;
        } else {
            while (index < clause.length() && !Character.isWhitespace(clause.charAt(index))) {
                index++;
            }
        }
        return startsWithKeywords(clause, index, "SHARD", "KEY");
    }

    /**
     * Removes AUTO_INCREMENT [AS SEQUENCE] from a column definition and makes that column
     * nullable. SingleStore requires an AUTO_INCREMENT column to be part of an index, and
     * indexes are not copied to the staging table. A nullable staging column keeps a NULL
     * from the record as NULL, so the destination table generates a value for it.
     */
    private static String stripAutoIncrement(String column) {
        StringBuilder result = new StringBuilder();
        boolean removed = false;
        int i = 0;
        while (i < column.length()) {
            char c = column.charAt(i);
            if (isQuote(c)) {
                int end = quotedEnd(column, i, c) + 1;
                result.append(column, i, end);
                i = end;
                continue;
            }
            if (i == 0 || !isIdentifierCharacter(column.charAt(i - 1))) {
                int end = keywordsEnd(column, i, "AUTO_INCREMENT");
                if (end >= 0) {
                    int sequenceEnd = keywordsEnd(column, end, "AS", "SEQUENCE");
                    i = sequenceEnd >= 0 ? sequenceEnd : end;
                    while (result.length() > 0 && Character.isWhitespace(result.charAt(result.length() - 1))) {
                        result.setLength(result.length() - 1);
                    }
                    removed = true;
                    continue;
                }
            }
            result.append(c);
            i++;
        }
        return removed ? makeNullable(result.toString()) : result.toString();
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

    private static int skipWhitespace(String value, int index) {
        while (index < value.length() && Character.isWhitespace(value.charAt(index))) {
            index++;
        }
        return index;
    }
}
