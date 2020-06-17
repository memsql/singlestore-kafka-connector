package com.memsql.kafka.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class SinkRecordCreator {

    public static List<SinkRecord> createRecords(int count) {
        Schema schema = SchemaBuilder.struct().name("NAME")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("admin", SchemaBuilder.bool().defaultValue(false).build())
                .build();
        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Struct struct = new Struct(schema)
                    .put("name", "Barbara Liskov")
                    .put("age", 75);
            records.add(new SinkRecord("topic",0,null,null, schema, struct, i));
        }
        return records;
    }
}
