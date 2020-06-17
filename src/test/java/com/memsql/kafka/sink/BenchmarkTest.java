package com.memsql.kafka.sink;

import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkTest {

    @Ignore
    @Test
    public void benchmarkTest() throws Exception {
        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testDb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};

        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);

        List<SinkRecord> records = SinkRecordCreator.createRecords(3000000);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "avro");
        long startTimeAvro = System.nanoTime();
        writer.write(records);
        System.out.println("Avro time: " + (System.nanoTime() - startTimeAvro));

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "csv");
        long startTimeCsv = System.nanoTime();
        writer.write(records);
        System.out.println("CSV time:  " + (System.nanoTime() - startTimeCsv));
    }
}
