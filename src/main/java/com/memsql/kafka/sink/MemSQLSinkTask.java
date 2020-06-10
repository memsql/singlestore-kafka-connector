package com.memsql.kafka.sink;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class MemSQLSinkTask extends SinkTask {

    MemSQLSinkConfig config;
    MemSQLDbWriter writer;
    int retriesLeft;

    @Override
    public void start(Map<String, String> props) {
        this.config = new MemSQLSinkConfig(props);
        this.writer = new MemSQLDbWriter(config);
        this.retriesLeft = config.maxRetries;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach(sinkRecord -> {
            System.out.println("Sink record value: " + sinkRecord.value());
            System.out.println("Sink record value string: " + sinkRecord.value().toString());
            System.out.println("Sink record value class: " + sinkRecord.value().getClass().toString());
            Struct value = (Struct)sinkRecord.value();

            System.out.println("Record schema: " + sinkRecord.toString());
            if (sinkRecord.valueSchema() != null) {
                sinkRecord.valueSchema().fields().forEach(field -> {
                    if (field.schema() != null) {
                        System.out.println("Value Field schema: " + field.schema().toString());
                        if (field.schema().defaultValue() != null) {
                            System.out.println("Value Field class: " + field.schema().defaultValue().getClass().getName());
                        }
                    }
                    System.out.println("Value Field name: " + field.name());
                    System.out.println("Value Field string: " + field.toString());
                });
            }
            if (sinkRecord.keySchema() != null) {
                sinkRecord.keySchema().fields().forEach(field -> {
                    if (field.schema() != null) {
                        System.out.println("Key Field schema: " + field.schema().toString());
                        if (field.schema().defaultValue() != null) {
                            System.out.println("Key Field class: " + field.schema().defaultValue().getClass().getName());
                        }
                    }
                    System.out.println("Key Field name: " + field.name());
                    System.out.println("Key Field string: " + field.toString());
                });
            }
        });

        writer.write(records);
    }

    @Override
    public void stop() {

    }

    @Override
    public String version() {
        return null;
    }
}
