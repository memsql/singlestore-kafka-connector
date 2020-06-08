package com.memsql.kafka;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class MemSQLSinkTask extends SinkTask {

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        collection.stream().forEach(sinkRecord -> {
            System.out.println("Record schema: " + sinkRecord.toString());
            sinkRecord.valueSchema().fields().forEach(field -> {
                System.out.println("Value Field schema: " + field.schema().toString());
                System.out.println("Value Field class: " + field.schema().defaultValue().getClass().getName());
                System.out.println("Value Field name: " + field.name());
                System.out.println("Value Field string: " + field.toString());
            });
            sinkRecord.keySchema().fields().forEach(field -> {
                System.out.println("Key Field schema: " + field.schema().toString());
                System.out.println("Key Field class: " + field.schema().defaultValue().getClass().getName());
                System.out.println("Key Field name: " + field.name());
                System.out.println("Key Field string: " + field.toString());
            });
        });
    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return null;
    }
}
