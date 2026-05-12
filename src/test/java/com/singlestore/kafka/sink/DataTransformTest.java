package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.DataTransform;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.singlestore.kafka.utils.SinkRecordCreator.createRecord;
import static org.junit.Assert.assertEquals;

public class DataTransformTest {
    List<SinkRecord> records;

    @Before
    public void setUp() {
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("age", Schema.INT32_SCHEMA).field("name", Schema.STRING_SCHEMA).field("job", Schema.STRING_SCHEMA).build();
        SinkRecord record1 = createRecord(schema, new Struct(schema).put("id", 1).put("age", 25).put("name", "John").put("job", "teacher"), "topic");
        SinkRecord record2 = createRecord(schema, new Struct(schema).put("id", 2).put("age", 30).put("name", "Mary").put("job", "teacher"), "topic");

        records = Arrays.asList(record1, record2);
    }

    @Test
    public void EmptyCollection() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Arrays.asList("age", "name", "nonexisting"), Collections.emptyList())
                .selectFields(new ArrayList<>());
        assertEquals(updatedRecords.size(), 0);
    }

    @Test
    public void BaseTest() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Arrays.asList("age", "name", "nonexisting"), Collections.emptyList())
                .selectFields(records);
        Schema schema = SchemaBuilder.struct().field("age", Schema.INT32_SCHEMA).field("name", Schema.STRING_SCHEMA).build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema).put("age", 25).put("name", "John"), "topic"),  createRecord(schema, new Struct(schema).put("age", 30).put("name", "Mary"), "topic"));
    }

    @Test
    public void Schemaless() {
        Map<Object, Object> mp = new HashMap<>();
        mp.put("id", 1);
        mp.put("age", 25);
        mp.put("name", "John");
        mp.put("job", "teacher");

        SinkRecord record = createRecord(null, mp);

        Collection<SinkRecord> updatedRecords = new DataTransform(Arrays.asList("age", "name", "nonexisting"), Collections.emptyList())
            .selectFields(Collections.singletonList(record));

        assertEquals(updatedRecords.size(), 1);
        Iterator<SinkRecord> iterator = updatedRecords.iterator();
        SinkRecord updatedRecord = iterator.next();
        Map<Object, Object> expectedMp = new HashMap<>();
        expectedMp.put("age", 25);
        expectedMp.put("name", "John");

        assertEquals(createRecord(null, expectedMp), updatedRecord);
    }

    @Test
    public void NonExistingFields() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.singletonList("nonexisting"), Collections.emptyList())
                .selectFields(records);
        Schema schema = SchemaBuilder.struct().build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema), "topic"), createRecord(schema, new Struct(schema), "topic"));
    }

    @Test
    public void DuplicateEntries() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.singletonList("job"), Collections.emptyList())
                .selectFields(records);
        Schema schema = SchemaBuilder.struct().field("job", Schema.STRING_SCHEMA).build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema).put("job", "teacher"), "topic"), createRecord(schema, new Struct(schema).put("job", "teacher"), "topic"));
    }

    @Test
    public void BlacklistOnly() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.emptyList(), Arrays.asList("job", "nonexisting"))
                .selectFields(records);
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        checkExpectedResult(updatedRecords,
                createRecord(schema, new Struct(schema).put("id", 1).put("age", 25).put("name", "John"), "topic"),
                createRecord(schema, new Struct(schema).put("id", 2).put("age", 30).put("name", "Mary"), "topic"));
    }

    @Test
    public void WhitelistAndBlacklist() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Arrays.asList("age", "name", "job"), Collections.singletonList("name"))
                .selectFields(records);
        Schema schema = SchemaBuilder.struct().field("age", Schema.INT32_SCHEMA).field("job", Schema.STRING_SCHEMA).build();
        checkExpectedResult(updatedRecords,
                createRecord(schema, new Struct(schema).put("age", 25).put("job", "teacher"), "topic"),
                createRecord(schema, new Struct(schema).put("age", 30).put("job", "teacher"), "topic"));
    }

    @Test
    public void SchemalessBlacklist() {
        Map<Object, Object> mp = new HashMap<>();
        mp.put("id", 1);
        mp.put("age", 25);
        mp.put("name", "John");
        mp.put("job", "teacher");

        SinkRecord record = createRecord(null, mp);

        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.emptyList(), Collections.singletonList("job"))
                .selectFields(Collections.singletonList(record));

        assertEquals(updatedRecords.size(), 1);
        SinkRecord updatedRecord = updatedRecords.iterator().next();
        Map<Object, Object> expectedMp = new HashMap<>();
        expectedMp.put("id", 1);
        expectedMp.put("age", 25);
        expectedMp.put("name", "John");

        assertEquals(createRecord(null, expectedMp), updatedRecord);
    }

    private void checkExpectedResult(Collection<SinkRecord> updatedRecords, SinkRecord expectedRecord1, SinkRecord expectedRecord2) {
        assertEquals(updatedRecords.size(), 2);
        Iterator<SinkRecord> iterator = updatedRecords.iterator();
        assertEquals(iterator.next(), expectedRecord1);
        assertEquals(iterator.next(), expectedRecord2);
    }
}
