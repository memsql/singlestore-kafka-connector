package com.singlestore.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InsertDbWriterTest {

    private static final String UPDATE = "`data` = VALUES(`data`)";

    @Test
    public void generateQuery() {
        InsertDbWriter writer = writer(Arrays.asList("id", "data"), null, "partial");

        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateQuery(1));
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateQuery(2));
    }

    @Test
    public void generateQueryEscapesIdentifiers() {
        InsertDbWriter writer = new InsertDbWriter(
                Collections.singletonList("c`d"), null, null, "a`b", "`age` = `age` + 1", 10);
        assertEquals(
                "INSERT INTO `a``b` (`c``d`) VALUES (?) ON DUPLICATE KEY UPDATE `age` = `age` + 1",
                writer.generateQuery(1));
    }

    @Test
    public void generateQueryWithFilter() {
        InsertDbWriter writer = new InsertDbWriter(
                Arrays.asList("id", "data"), null, "id > 1", "partial", UPDATE, 10);
        assertEquals(
                "INSERT INTO `partial` (`id`, `data`) SELECT `id`, `data` FROM "
                        + "(SELECT ? AS `id`, ? AS `data` UNION ALL SELECT ? AS `id`, ? AS `data`) "
                        + "AS `_singlestore_kafka_incoming` WHERE id > 1 "
                        + "ON DUPLICATE KEY UPDATE `data` = VALUES(`data`)",
                writer.generateQuery(2));
    }

    @Test
    public void partitionPreservesOrder() {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(createRecord(Schema.INT32_SCHEMA, i));
        }

        List<List<SinkRecord>> batches = InsertDbWriter.partition(records, 2);
        assertEquals(3, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
        assertEquals(1, batches.get(2).size());
        assertEquals(0, batches.get(0).get(0).value());
        assertEquals(1, batches.get(0).get(1).value());
        assertEquals(4, batches.get(2).get(0).value());

        assertEquals(1, InsertDbWriter.partition(records, 10).size());
        assertEquals(5, InsertDbWriter.partition(records, 1).size());
        assertTrue(InsertDbWriter.partition(Collections.emptyList(), 2).isEmpty());
    }

    @Test
    public void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new InsertDbWriter(Collections.singletonList("id"), null, null, "t", " ", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new InsertDbWriter(Collections.singletonList("id"), null, null, "t", UPDATE, 0));
        InsertDbWriter writer = writer(Collections.singletonList("id"), null, "t");
        assertThrows(IllegalArgumentException.class, () -> writer.generateQuery(0));
        assertThrows(IllegalArgumentException.class, () -> InsertDbWriter.partition(Collections.emptyList(), 0));
    }

    private static InsertDbWriter writer(List<String> columns, String filter, String table) {
        return new InsertDbWriter(columns, null, filter, table, UPDATE, 10);
    }
}
