package com.singlestore.kafka.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ShowCreateTableTest {

    @Test
    public void readsColumnsAndOmitsIndexes() {
        ShowCreateTable table = ShowCreateTable.parse("CREATE TABLE `t` (\n"
                + "  `id` bigint(20) NOT NULL,\n"
                + "  `doc` JSON COLLATE utf8mb4_bin,\n"
                + "  `vec` vector(3, F32) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE KEY `u` (`id`) USING HASH,\n"
                + "  KEY `k` (`id`),\n"
                + "  INDEX `i` (`id`),\n"
                + "  FULLTEXT USING VERSION 2 `ft` (`doc`),\n"
                + "  VECTOR INDEX `vi` (`vec`) INDEX_OPTIONS '{\"index_type\":\"AUTO\"}',\n"
                + "  MULTI VALUE INDEX (`doc`) INDEX_OPTIONS '{\"path\":[\"a\"]}',\n"
                + "  SORT KEY `__UNORDERED` (`id` DESC),\n"
                + "  COLUMN GROUP `cg` (*),\n"
                + "  FOREIGN KEY (`id`) REFERENCES `other` (`id`),\n"
                + "  CONSTRAINT `pk` PRIMARY KEY (`id`),\n"
                + "  PROJECTION `p` (`id`),\n"
                + "  UNKNOWN CLAUSE (`id`),\n"
                + "  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE\n"
                + ") AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTO_INCREMENT=10 SQL_MODE='STRICT_ALL_TABLES'");

        assertEquals(Arrays.asList("id", "doc", "vec"), table.columnNames());
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `tmp` (`id` bigint(20) NOT NULL, "
                        + "`doc` JSON COLLATE utf8mb4_bin, `vec` vector(3, F32) DEFAULT NULL, `ord` BIGINT NULL)",
                table.toCreateTemporaryTable("tmp", "ord"));
    }

    @Test
    public void keepsCommasAndParenthesesInsideColumnDefinitions() {
        ShowCreateTable table = ShowCreateTable.parse("CREATE TABLE `t` (\n"
                + "  `e` enum('a,b','c)') DEFAULT 'a,b',\n"
                + "  `d` decimal(10,2) DEFAULT '1.00',\n"
                + "  `s` varchar(10) DEFAULT 'it''s, (x)',\n"
                + "  `b` varchar(10) DEFAULT 'back\\'slash, )',\n"
                + "  `q` varchar(10) DEFAULT \"dq, )\",\n"
                + "  `c` AS (concat(`e`, ',', `s`)) PERSISTED text\n"
                + ")");

        assertEquals(Arrays.asList("e", "d", "s", "b", "q", "c"), table.columnNames());
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `tmp` (`e` enum('a,b','c)') DEFAULT 'a,b', "
                        + "`d` decimal(10,2) DEFAULT '1.00', "
                        + "`s` varchar(10) DEFAULT 'it''s, (x)', "
                        + "`b` varchar(10) DEFAULT 'back\\'slash, )', "
                        + "`q` varchar(10) DEFAULT \"dq, )\", "
                        + "`c` AS (concat(`e`, ',', `s`)) PERSISTED text, `ord` BIGINT NULL)",
                table.toCreateTemporaryTable("tmp", "ord"));
    }

    @Test
    public void unescapesQuotedIdentifiers() {
        ShowCreateTable table = ShowCreateTable.parse(
                "CREATE TABLE `a``b(` (`c``,d` int, `KEY` int, `(x)` int, PRIMARY KEY (`c``,d`))");
        assertEquals(Arrays.asList("c`,d", "KEY", "(x)"), table.columnNames());
    }

    @Test
    public void keepsShardKeyVariants() {
        assertShardKey("SHARD KEY `__SHARDKEY` (`id`)");
        assertShardKey("SHARD KEY ()");
        assertShardKey("shard key (`id`) METADATA_ONLY");
        assertShardKey("CONSTRAINT SHARD KEY (`id`)");
        assertShardKey("CONSTRAINT `s k` SHARD KEY (`id`)");
        assertShardKey("CONSTRAINT sk SHARD KEY (`id`)");
    }

    @Test
    public void omitsConstraintsThatAreNotShardKeys() {
        ShowCreateTable table = ShowCreateTable.parse(
                "CREATE TABLE `t` (`id` int, CONSTRAINT `SHARD` UNIQUE KEY (`id`), CONSTRAINT `KEY` PRIMARY KEY (`id`))");
        assertEquals("CREATE ROWSTORE TEMPORARY TABLE `tmp` (`id` int, `ord` BIGINT NULL)",
                table.toCreateTemporaryTable("tmp", "ord"));
    }

    @Test
    public void stripsAutoIncrement() {
        assertColumn("`id` bigint(20) NOT NULL AUTO_INCREMENT", "`id` bigint(20) NULL");
        assertColumn("`id` bigint(20) NOT NULL auto_increment", "`id` bigint(20) NULL");
        assertColumn("`id` int(11) NOT NULL AUTO_INCREMENT AS SEQUENCE", "`id` int(11) NULL");
        assertColumn("`id` bigint(20) AUTO_INCREMENT NOT NULL", "`id` bigint(20) NULL");
        assertColumn("`id` int(11) AUTO_INCREMENT  AS  SEQUENCE SPARSE", "`id` int(11) SPARSE");
        assertColumn("`id` bigint(20) AUTO_INCREMENT COMMENT 'NOT NULL'", "`id` bigint(20) COMMENT 'NOT NULL'");
    }

    @Test
    public void keepsAutoIncrementInsideQuotesAndIdentifiers() {
        assertColumn("`auto_increment` int DEFAULT NULL", "`auto_increment` int DEFAULT NULL");
        assertColumn("`c` varchar(20) DEFAULT 'AUTO_INCREMENT'", "`c` varchar(20) DEFAULT 'AUTO_INCREMENT'");
        assertColumn("`c` int COMMENT \"auto_increment AS SEQUENCE\"", "`c` int COMMENT \"auto_increment AS SEQUENCE\"");
        assertColumn("`c` int NOT_AUTO_INCREMENT", "`c` int NOT_AUTO_INCREMENT");
        assertColumn("`c` int AUTO_INCREMENTS", "`c` int AUTO_INCREMENTS");
    }

    @Test
    public void rejectsInvalidStatements() {
        assertRejected(null, "empty statement");
        assertRejected("  ", "empty statement");
        assertRejected("CREATE TABLE `t`", "did not contain a column list");
        assertRejected("CREATE TABLE `t` (`id` int", "unbalanced parentheses");
        assertRejected("CREATE TABLE `t` (`id` int DEFAULT 'x)", "unterminated quoted value");
        assertRejected("CREATE TABLE `t` (PRIMARY KEY (`id`))", "did not contain any columns");
        assertRejected("CREATE TABLE `t` (`id` int, SHARD KEY (`id`), SHARD KEY (`id`))", "more than one SHARD KEY");
    }

    @Test
    public void columnNamesAreImmutable() {
        ShowCreateTable table = ShowCreateTable.parse("CREATE TABLE `t` (`id` int)");
        assertThrows(UnsupportedOperationException.class, () -> table.columnNames().add("x"));
        assertEquals(Collections.singletonList("id"), table.columnNames());
    }

    private static void assertShardKey(String shardKey) {
        ShowCreateTable table = ShowCreateTable.parse(
                "CREATE TABLE `t` (`id` int, PRIMARY KEY (`id`), " + shardKey + ")");
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `tmp` (`id` int, `ord` BIGINT NULL, " + shardKey + ")",
                table.toCreateTemporaryTable("tmp", "ord"));
    }

    private static void assertColumn(String definition, String expected) {
        ShowCreateTable table = ShowCreateTable.parse(
                "CREATE TABLE `t` (" + definition + ", KEY (`id`))");
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `tmp` (" + expected + ", `ord` BIGINT NULL)",
                table.toCreateTemporaryTable("tmp", "ord"));
    }

    private static void assertRejected(String statement, String message) {
        ConnectException ex = assertThrows(ConnectException.class, () -> ShowCreateTable.parse(statement));
        assertTrue(ex.getMessage(), ex.getMessage().contains(message));
    }
}
