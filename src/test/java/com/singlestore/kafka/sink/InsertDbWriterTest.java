package com.singlestore.kafka.sink;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class InsertDbWriterTest {

    private static final String UPDATE = "`data` = VALUES(`data`)";
    private static final String TEMPORARY_TABLE = "_incoming";
    private static final String PARTIAL_SHOW_CREATE = "CREATE TABLE `partial` (\n"
            + "  `id` int(11) NOT NULL,\n"
            + "  `data` text CHARACTER SET utf8 COLLATE utf8_general_ci,\n"
            + "  `status` enum('a,b','c') DEFAULT NULL,\n"
            + "  `amount` decimal(10,2) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  SHARD KEY `__SHARDKEY` (`id`),\n"
            + "  KEY `status` (`status`) USING HASH,\n"
            + "  SORT KEY `__UNORDERED` (`data`),\n"
            + "  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE\n"
            + ") AUTOSTATS_CARDINALITY_MODE=INCREMENTAL";

    @Test
    public void generateQueries() {
        InsertDbWriter writer = writer(Arrays.asList("id", "data"), null, "partial");

        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` (`id` int(11) NOT NULL, "
                        + "`data` text CHARACTER SET utf8 COLLATE utf8_general_ci, "
                        + "`status` enum('a,b','c') DEFAULT NULL, `amount` decimal(10,2) DEFAULT NULL, "
                        + "`_singlestore_kafka_order` BIGINT NULL, SHARD KEY `__SHARDKEY` (`id`))",
                writer.generateCreateTemporaryTableQuery());
        assertEquals("DROP TEMPORARY TABLE IF EXISTS `_incoming`", writer.generateDropTemporaryTableQuery());
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) SELECT `_singlestore_kafka_column_0`, `_singlestore_kafka_column_1` "
                        + "FROM (SELECT `id` AS `_singlestore_kafka_column_0`, `data` AS `_singlestore_kafka_column_1`, "
                        + "`_singlestore_kafka_order` FROM `_incoming`) AS `_singlestore_kafka_src` "
                        + "ORDER BY `_singlestore_kafka_order` ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateInsertQuery());
    }

    @Test
    public void generateQueryEscapesIdentifiers() {
        String showCreate = "CREATE TABLE `a``b` (\n"
                + "  `c``d` text,\n"
                + "  UNIQUE KEY `c` (`c``d`),\n"
                + "  SHARD KEY (`c``d`)\n"
                + ")";
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("c`d"), null, "a`b", "`age` = `age` + 1", "t`mp", showCreate);
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `t``mp` (`c``d` text, "
                        + "`_singlestore_kafka_order` BIGINT NULL, SHARD KEY (`c``d`))",
                writer.generateCreateTemporaryTableQuery());
        assertEquals("DROP TEMPORARY TABLE IF EXISTS `t``mp`", writer.generateDropTemporaryTableQuery());
        assertEquals(
                "INSERT INTO `a``b` (`c``d`) SELECT `_singlestore_kafka_column_0` "
                        + "FROM (SELECT `c``d` AS `_singlestore_kafka_column_0`, `_singlestore_kafka_order` FROM `t``mp`) "
                        + "AS `_singlestore_kafka_src` ORDER BY `_singlestore_kafka_order` "
                        + "ON DUPLICATE KEY UPDATE `age` = `age` + 1",
                writer.generateInsertQuery());
    }

    @Test
    public void omitsShardKeyWhenTheTableHasNone() {
        String showCreate = "CREATE ROWSTORE REFERENCE TABLE `ref` (\n"
                + "  `id` int(11) NOT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ")";
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("id"), null, "ref", UPDATE, TEMPORARY_TABLE, showCreate);
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` (`id` int(11) NOT NULL, "
                        + "`_singlestore_kafka_order` BIGINT NULL)",
                writer.generateCreateTemporaryTableQuery());
    }

    @Test
    public void keepsNamedShardKeyConstraint() {
        String showCreate = "CREATE TABLE `partial` (\n"
                + "  `id` int(11) NOT NULL,\n"
                + "  CONSTRAINT `sk` SHARD KEY (`id`),\n"
                + "  CONSTRAINT `pk` PRIMARY KEY (`id`)\n"
                + ")";
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("id"), null, "partial", UPDATE, TEMPORARY_TABLE, showCreate);
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` (`id` int(11) NOT NULL, "
                        + "`_singlestore_kafka_order` BIGINT NULL, CONSTRAINT `sk` SHARD KEY (`id`))",
                writer.generateCreateTemporaryTableQuery());
    }

    @Test
    public void keepsUnnamedShardKeyConstraintAndComplexColumnExpressions() {
        String showCreate = "CREATE TABLE `partial` (\n"
                + "  `id` int(11) NOT NULL,\n"
                + "  `label` varchar(50) DEFAULT 'a,b)''c',\n"
                + "  `computed` AS (concat(`label`, \",\", `id`)) PERSISTED varchar(100),\n"
                + "  CONSTRAINT SHARD KEY (`id`),\n"
                + "  KEY `idx` (`label`) USING HASH\n"
                + ")";
        InsertDbWriter writer = new InsertDbWriter(
                Arrays.asList("id", "label"), null, "partial", UPDATE, TEMPORARY_TABLE, showCreate);
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` (`id` int(11) NOT NULL, "
                        + "`label` varchar(50) DEFAULT 'a,b)''c', "
                        + "`computed` AS (concat(`label`, \",\", `id`)) PERSISTED varchar(100), "
                        + "`_singlestore_kafka_order` BIGINT NULL, CONSTRAINT SHARD KEY (`id`))",
                writer.generateCreateTemporaryTableQuery());
    }

    @Test
    public void generateQueryWithFilter() {
        InsertDbWriter writer = writer(Arrays.asList("id", "data"), "id > 1", "partial");
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) SELECT `_singlestore_kafka_column_0`, `_singlestore_kafka_column_1` "
                        + "FROM (SELECT `id` AS `_singlestore_kafka_column_0`, `data` AS `_singlestore_kafka_column_1`, "
                        + "`_singlestore_kafka_order` FROM `_incoming` WHERE id > 1) AS `_singlestore_kafka_src` "
                        + "ORDER BY `_singlestore_kafka_order` ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateInsertQuery());
    }

    @Test
    public void avoidsOrderColumnCollision() {
        List<String> columns = Arrays.asList("_singlestore_kafka_order", "data");
        InsertDbWriter writer = writer(columns, null, "partial", showCreate(columns));
        assertEquals("__singlestore_kafka_order", writer.getOrderColumn());
    }

    @Test
    public void avoidsOrderColumnCollisionIgnoringCase() {
        List<String> columns = Arrays.asList("_SINGLESTORE_KAFKA_ORDER", "data");
        InsertDbWriter writer = writer(columns, null, "partial", showCreate(columns));
        assertEquals("__singlestore_kafka_order", writer.getOrderColumn());
        assertEquals(
                "INSERT INTO `partial` (`_SINGLESTORE_KAFKA_ORDER`, `data`) "
                        + "SELECT `_singlestore_kafka_column_0`, `_singlestore_kafka_column_1` "
                        + "FROM (SELECT `_SINGLESTORE_KAFKA_ORDER` AS `_singlestore_kafka_column_0`, "
                        + "`data` AS `_singlestore_kafka_column_1`, `__singlestore_kafka_order` FROM `_incoming`) "
                        + "AS `_singlestore_kafka_src` ORDER BY `__singlestore_kafka_order` "
                        + "ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateInsertQuery());
    }

    @Test
    public void avoidsRepeatedOrderColumnCollisionIgnoringCase() {
        List<String> columns = Arrays.asList("_singlestore_kafka_order", "__SINGLESTORE_KAFKA_ORDER");
        InsertDbWriter writer = writer(columns, null, "partial", showCreate(columns));
        assertEquals("___singlestore_kafka_order", writer.getOrderColumn());
    }

    @Test
    public void avoidsStagingAliasCollisionWithDestinationColumn() {
        String showCreate = "CREATE TABLE `partial` ("
                + "`id` int, `_SINGLESTORE_KAFKA_COLUMN_0` int, PRIMARY KEY (`id`))";
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("id"), null, "partial", "`id` = `id` + 1", TEMPORARY_TABLE, showCreate);
        assertEquals(
                "INSERT INTO `partial` (`id`) SELECT `__singlestore_kafka_column_0` "
                        + "FROM (SELECT `id` AS `__singlestore_kafka_column_0`, `_singlestore_kafka_order` FROM `_incoming`) "
                        + "AS `_singlestore_kafka_src` ORDER BY `_singlestore_kafka_order` "
                        + "ON DUPLICATE KEY UPDATE `id` = `id` + 1",
                writer.generateInsertQuery());
    }

    @Test
    public void temporaryTableNameIsStableAndHashed() {
        String ordersTable = "_ssk_1c168adb00d208e42f93314529f1fa9c0427eb63233ceda95a5db52b7012a719";
        assertEquals(ordersTable, InsertDbWriter.temporaryTableName("orders"));
        assertEquals(69, ordersTable.length());
        assertEquals(ordersTable, InsertDbWriter.temporaryTableName("Orders"));
        assertNotEquals(ordersTable, InsertDbWriter.temporaryTableName("orders2"));
        assertNotEquals(ordersTable, InsertDbWriter.temporaryTableName("_ssk_orders"));

        assertNotEquals(
                InsertDbWriter.temporaryTableName(repeat('a', 300) + "1"),
                InsertDbWriter.temporaryTableName(repeat('a', 300) + "2"));
        assertEquals(69, InsertDbWriter.temporaryTableName(repeat('a', 300)).length());
    }

    @Test
    public void stripsAutoIncrementFromStagingTable() {
        String showCreate = "CREATE TABLE `auto` (\n"
                + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `biz` int(11) NOT NULL,\n"
                + "  `data` text,\n"
                + "  PRIMARY KEY (`biz`),\n"
                + "  KEY `id` (`id`),\n"
                + "  SHARD KEY `__SHARDKEY` (`biz`)\n"
                + ") AUTO_INCREMENT=42";
        InsertDbWriter writer = new InsertDbWriter(
                Arrays.asList("biz", "data"), null, "auto", UPDATE, TEMPORARY_TABLE, showCreate);
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` (`id` bigint(20) NULL, "
                        + "`biz` int(11) NOT NULL, `data` text, "
                        + "`_singlestore_kafka_order` BIGINT NULL, SHARD KEY `__SHARDKEY` (`biz`))",
                writer.generateCreateTemporaryTableQuery());
    }

    @Test
    public void rejectsMissingAssignmentList() {
        assertThrows(NullPointerException.class,
                () -> new InsertDbWriter(Collections.singletonList("id"), null, "t", null, TEMPORARY_TABLE, null));
    }

    private static InsertDbWriter writer(List<String> columns, String filter, String table) {
        return writer(columns, filter, table, PARTIAL_SHOW_CREATE);
    }

    private static InsertDbWriter writer(List<String> columns, String filter, String table, String showCreate) {
        return new InsertDbWriter(columns, filter, table, UPDATE, TEMPORARY_TABLE, showCreate);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private static String showCreate(List<String> columns) {
        StringBuilder body = new StringBuilder();
        for (String column : columns) {
            if (body.length() > 0) {
                body.append(", ");
            }
            body.append(SingleStoreDialect.quoteIdentifier(column)).append(" text");
        }
        return "CREATE TABLE `partial` (" + body + ", PRIMARY KEY ("
                + SingleStoreDialect.quoteIdentifier(columns.get(0)) + "))";
    }
}
