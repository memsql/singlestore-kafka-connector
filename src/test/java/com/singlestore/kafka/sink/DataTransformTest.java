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
    public void BaseTest() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Arrays.asList("age", "name", "nonexisting")).selectWhitelistedFields(records);
        Schema schema = SchemaBuilder.struct().field("age", Schema.INT32_SCHEMA).field("name", Schema.STRING_SCHEMA).build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema).put("age", 25).put("name", "John"), "topic"),  createRecord(schema, new Struct(schema).put("age", 30).put("name", "Mary"), "topic"));
    }

    @Test
    public void NonExistingFields() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.singletonList("nonexisting")).selectWhitelistedFields(records);
        Schema schema = SchemaBuilder.struct().build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema), "topic"), createRecord(schema, new Struct(schema), "topic"));
    }

    @Test
    public void DuplicateEntries() {
        Collection<SinkRecord> updatedRecords = new DataTransform(Collections.singletonList("job")).selectWhitelistedFields(records);
        Schema schema = SchemaBuilder.struct().field("job", Schema.STRING_SCHEMA).build();
        checkExpectedResult(updatedRecords, createRecord(schema, new Struct(schema).put("job", "teacher"), "topic"), createRecord(schema, new Struct(schema).put("job", "teacher"), "topic"));
    }

    private void checkExpectedResult(Collection<SinkRecord> updatedRecords, SinkRecord expectedRecord1, SinkRecord expectedRecord2) {
        assertEquals(updatedRecords.size(), 2);
        Iterator<SinkRecord> iterator = updatedRecords.iterator();
        assertEquals(iterator.next(), expectedRecord1);
        assertEquals(iterator.next(), expectedRecord2);
    }
}
