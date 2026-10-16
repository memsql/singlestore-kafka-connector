package com.singlestore.kafka.sink;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InsertDbWriterTest {

    private static final String UPDATE = "`data` = VALUES(`data`)";
    private static final String TEMPORARY_TABLE = "_incoming";

    @Test
    public void generateQueries() {
        InsertDbWriter writer = writer(Arrays.asList("id", "data"), null, "partial");

        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `_incoming` AS SELECT NULL :> BIGINT "
                        + "AS `_singlestore_kafka_order`, `id` AS `_singlestore_kafka_column_0`, "
                        + "`data` AS `_singlestore_kafka_column_1` FROM `partial` WHERE 1=0",
                writer.generateCreateTemporaryTableQuery());
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) SELECT `_singlestore_kafka_column_0` AS `id`, "
                        + "`_singlestore_kafka_column_1` AS `data` FROM `_incoming` "
                        + "ORDER BY `_singlestore_kafka_order` ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateInsertQuery());
    }

    @Test
    public void generateQueryEscapesIdentifiers() {
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("c`d"), null, "a`b", "`age` = `age` + 1", "t`mp");
        assertEquals(
                "CREATE ROWSTORE TEMPORARY TABLE `t``mp` AS SELECT NULL :> BIGINT "
                        + "AS `_singlestore_kafka_order`, `c``d` AS `_singlestore_kafka_column_0` "
                        + "FROM `a``b` WHERE 1=0",
                writer.generateCreateTemporaryTableQuery());
        assertEquals(
                "INSERT INTO `a``b` (`c``d`) SELECT `_singlestore_kafka_column_0` AS `c``d` FROM `t``mp` "
                        + "ORDER BY `_singlestore_kafka_order` ON DUPLICATE KEY UPDATE `age` = `age` + 1",
                writer.generateInsertQuery());
    }

    @Test
    public void generateQueryWithFilter() {
        InsertDbWriter writer = writer(Arrays.asList("id", "data"), "id > 1", "partial");
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) SELECT `_singlestore_kafka_column_0` AS `id`, "
                        + "`_singlestore_kafka_column_1` AS `data` FROM `_incoming` "
                        + "WHERE id > 1 ORDER BY `_singlestore_kafka_order` "
                        + "ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateInsertQuery());
    }

    @Test
    public void avoidsOrderColumnCollision() {
        List<String> columns = Arrays.asList("_singlestore_kafka_order", "data");
        InsertDbWriter writer = writer(columns, null, "partial");
        assertEquals("__singlestore_kafka_order", writer.getOrderColumn());
    }

    @Test
    public void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new InsertDbWriter(Collections.singletonList("id"), null, "t", " ", TEMPORARY_TABLE));
    }

    private static InsertDbWriter writer(List<String> columns, String filter, String table) {
        return new InsertDbWriter(columns, filter, table, UPDATE, TEMPORARY_TABLE);
    }
}
