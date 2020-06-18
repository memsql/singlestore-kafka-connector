package com.memsql.kafka.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

public class SinkRecordCreator {

    public static List<SinkRecord> createRecords(int count) {
        Schema nestedStructSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("admin", SchemaBuilder.bool().defaultValue(false).build())
                .field("country", nestedStructSchema)
                .field("awards", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("children", SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                ))
                .build();

        List<SinkRecord> records = new ArrayList<>();
        Map<String, Integer> mp = new HashMap<>();
        mp.put("Moses Liskov", 1);
        Struct struct = new Struct(schema)
                .put("name", "Barbara Liskov")
                .put("age", 75)
                .put("country", new Struct(nestedStructSchema).put("name", "USA"))
                .put("awards", new ArrayList<>(Arrays.asList("IEEE John von Neumann Medal", "A. M. Turing Award")))
                .put("children", mp);

        for (int i = 0; i < count; i++) {
            records.add(new SinkRecord("topic",0,null,null, schema, struct, i));
        }
        return records;
    }
}
