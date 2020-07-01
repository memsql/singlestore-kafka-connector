package com.memsql.kafka.benchmark;

import com.memsql.kafka.sink.MemSQLDbWriter;
import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.*;

public class BenchmarkTest {

    @Ignore
    @Test
    public void benchmarkTest() throws Exception {
        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};

        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);

        List<SinkRecord> records = SinkRecordCreator.createRecords(1000000);

        long startTime = System.nanoTime();
        writer.write(records);
        System.out.println("Time:  " + (System.nanoTime() - startTime));
    }

    @Ignore
    @Test
    public void dataCompressionBenchmarkTest() throws Exception {

        int iterations = 100;
        int recordCount = 5000;

        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);
        List<SinkRecord> records = SinkRecordCreator.createRecords(recordCount);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "skip");
        write(iterations, writer, records, "Skip time: ");

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "gzip");
        write(iterations, writer, records, "Gzip time: ");

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "lz4");
        write(iterations, writer, records, "LZ4 time:  ");
    }

    private void write(int n, MemSQLDbWriter writer, List<SinkRecord> records, String message) throws SQLException {
        long startTime = System.nanoTime();
        for (int i = 0; i < n; i++) {
            writer.write(records);
        }
        System.out.println(message + (System.nanoTime() - startTime));
    }
}
